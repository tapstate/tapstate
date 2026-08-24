#!/usr/bin/env bash
# Verify durable context/session behavior with a loopback server and a disposable local user home.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: verify-cli-session-cache.sh --root DIR --mongo-uri URI [options]

Required:
  --root DIR             Disposable Tapstate worktree containing the candidate CLI and server.
  --mongo-uri URI        Mongo URI template containing the literal {database} placeholder.

Options:
  --out-dir DIR          New directory for a redacted report and diagnostics.
  --port NUMBER          Loopback HTTP port; default is a free ephemeral port.
  --native               Build and verify the production native CLI instead of the JVM jar.
  --cleanup              Drop only the generated tapstate_cli_session_verify_* database.
  --help                 Show this help.

The verifier runs `mvn clean package -DskipTests` in the supplied worktree. Use a disposable
worktree, never a shared checkout. It starts the server only on 127.0.0.1 and uses a fresh
temporary user home. The report excludes passwords, JWTs, Mongo URIs, and session-token values.
EOF
}

fail() {
  printf 'verification failed: %s\n' "$*" >&2
  exit 1
}

root=''
mongo_template=''
out_dir=''
port=''
drop_database=0
use_native=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --root)
      [[ $# -ge 2 ]] || fail '--root needs a directory'
      root=$2
      shift 2
      ;;
    --mongo-uri)
      [[ $# -ge 2 ]] || fail '--mongo-uri needs a URI template'
      mongo_template=$2
      shift 2
      ;;
    --out-dir)
      [[ $# -ge 2 ]] || fail '--out-dir needs a directory'
      out_dir=$2
      shift 2
      ;;
    --port)
      [[ $# -ge 2 ]] || fail '--port needs a number'
      port=$2
      shift 2
      ;;
    --native)
      use_native=1
      shift
      ;;
    --cleanup)
      drop_database=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      fail "unknown argument: $1"
      ;;
  esac
done

[[ -n "$root" ]] || fail '--root is required'
[[ -d "$root/.git" || -f "$root/.git" ]] || fail 'root is not a Git worktree'
[[ -f "$root/pom.xml" ]] || fail 'root has no Maven project'
[[ "$mongo_template" == *'{database}'* ]] || fail '--mongo-uri must contain the literal {database} placeholder'
[[ -z "$port" || "$port" =~ ^[0-9]+$ ]] || fail '--port must be numeric'

if [[ -z "$out_dir" ]]; then
  out_dir=$(mktemp -d "${TMPDIR:-/tmp}/tapstate-cli-session-cache.XXXXXX")
else
  [[ ! -e "$out_dir" ]] || fail "output directory already exists: $out_dir"
  mkdir -p "$out_dir"
fi

if [[ -z "$port" ]]; then
  port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')
fi

timestamp=$(date -u +%Y%m%dT%H%M%SZ)
nonce=$(python3 -c 'import secrets; print(secrets.token_hex(4))')
database="tapstate_cli_session_verify_${timestamp}_${nonce}"
mongo_uri="${mongo_template//\{database\}/$database}"
base_url="http://127.0.0.1:${port}"
home_dir=$(mktemp -d "$out_dir/home.XXXXXX")
workspace="$out_dir/workspace"
auth_dir="$home_dir/.tapstate/auth"
server_pid=''
username="session_verify_${nonce}"
password=$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')
jwt_secret=$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')

mode() {
  stat -f '%Lp' "$1" 2>/dev/null || stat -c '%a' "$1"
}

drop_with_java_driver() {
  local version sync core bson classpath source classes
  version=$(sed -n 's#.*<mongodb.driver.version>\([^<]*\)</mongodb.driver.version>.*#\1#p' "$root/bom/pom.xml" | head -1)
  [[ -n "$version" ]] || return 1
  sync="${M2_REPO:-$HOME/.m2/repository}/org/mongodb/mongodb-driver-sync/$version/mongodb-driver-sync-$version.jar"
  core="${M2_REPO:-$HOME/.m2/repository}/org/mongodb/mongodb-driver-core/$version/mongodb-driver-core-$version.jar"
  bson="${M2_REPO:-$HOME/.m2/repository}/org/mongodb/bson/$version/bson-$version.jar"
  [[ -f "$sync" && -f "$core" && -f "$bson" ]] || return 1
  classpath="$sync:$core:$bson"
  source="$out_dir/DropVerificationDatabase.java"
  classes="$out_dir/drop-classes"
  mkdir -p "$classes"
  cat >"$source" <<'JAVA'
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public final class DropVerificationDatabase {
  public static void main(String[] args) {
    String uri = System.getenv("TAPSTATE_VERIFY_MONGO_URI");
    String database = System.getenv("TAPSTATE_VERIFY_DATABASE");
    if (uri == null || database == null || !database.startsWith("tapstate_cli_session_verify_")) {
      throw new IllegalArgumentException("refusing to drop a non-verification database");
    }
    try (MongoClient client = MongoClients.create(uri)) {
      client.getDatabase(database).drop();
      for (String remaining : client.listDatabaseNames()) {
        if (database.equals(remaining)) {
          throw new IllegalStateException("verification database remains after cleanup");
        }
      }
    }
  }
}
JAVA
  javac -cp "$classpath" -d "$classes" "$source"
  TAPSTATE_VERIFY_MONGO_URI="$mongo_uri" TAPSTATE_VERIFY_DATABASE="$database" \
    java -cp "$classpath:$classes" DropVerificationDatabase
}

cleanup() {
  local status=$?
  if [[ -n "$server_pid" ]]; then
    kill -TERM "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  if [[ "$drop_database" -eq 1 ]]; then
    drop_with_java_driver >"$out_dir/mongo-cleanup.log" 2>&1 || {
      printf 'verification cleanup failed; generated database remains: %s\n' "$database" >&2
      status=1
    }
  fi
  if [[ "$home_dir" == "$out_dir"/home.* ]]; then
    rm -rf "$home_dir"
  fi
  exit "$status"
}
trap cleanup EXIT INT TERM

run_pty() {
  local input=$1
  local output=$2
  shift 2
  set +e
  TAPSTATE_PTY_INPUT="$input" python3 - "$@" >"$output" <<'PY'
import os, pty, select, signal, sys, time

data = os.environ.pop("TAPSTATE_PTY_INPUT").encode()
argv = sys.argv[1:]
pid, fd = pty.fork()
if pid == 0:
    os.environ.pop("TAPSTATE_PTY_INPUT", None)
    if os.environ.get("TERM", "") in ("", "dumb"):
        os.environ["TERM"] = "linux"
    os.execvp(argv[0], argv)

output = bytearray()
sent = False
deadline = time.time() + 30
status = None
while time.time() < deadline:
    readable, _, _ = select.select([fd], [], [], 0.25)
    if readable:
        try:
            chunk = os.read(fd, 4096)
        except OSError:
            chunk = b""
        if not chunk:
            break
        output.extend(chunk)
        if not sent:
            os.write(fd, data)
            sent = True
    done, status = os.waitpid(pid, os.WNOHANG)
    if done:
        break
else:
    os.kill(pid, signal.SIGKILL)
    _, status = os.waitpid(pid, 0)
    output.extend(b"\nPTY timeout\n")

try:
    while True:
        chunk = os.read(fd, 4096)
        if not chunk:
            break
        output.extend(chunk)
except OSError:
    pass
os.close(fd)
sys.stdout.buffer.write(output)
if status is None:
    _, status = os.waitpid(pid, 0)
if os.WIFEXITED(status):
    sys.exit(os.WEXITSTATUS(status))
sys.exit(128 + os.WTERMSIG(status))
PY
  local rc=$?
  set -e
  return "$rc"
}

printf 'building candidate worktree\n'
if [[ "$use_native" -eq 1 ]]; then
  mvn --file "$root/pom.xml" -q -Pnative -pl app,cli -am clean package -DskipTests >"$out_dir/build.log" 2>&1 \
    || fail 'candidate native build failed; see build.log'
else
  mvn --file "$root/pom.xml" -q -pl app,cli -am clean package -DskipTests >"$out_dir/build.log" 2>&1 \
    || fail 'candidate build failed; see build.log'
fi

app_jar=$(find "$root/app/target" -maxdepth 1 -type f -name 'app-*-boot.jar' -print -quit)
[[ -n "$app_jar" ]] || fail 'candidate server deliverable is missing after the build'
if [[ "$use_native" -eq 1 ]]; then
  cli_binary="$root/cli/target/tapstate"
  [[ -x "$cli_binary" ]] || fail 'candidate native CLI is missing after the build'
  cli_runtime='native'
else
  cli_jar=$(find "$root/cli/target" -maxdepth 1 -type f -name 'cli-*.jar' -print -quit)
  [[ -n "$cli_jar" ]] || fail 'candidate JVM CLI jar is missing after the build'
  cli_runtime='jvm'
fi
mkdir -p "$workspace"
chmod 700 "$home_dir"

java -jar "$app_jar" \
  --server.address=127.0.0.1 \
  --server.port="$port" \
  "--tapstate.store.mongo.uri=$mongo_uri" \
  "--tapstate.control.auth.jwt-secret=$jwt_secret" \
  >"$out_dir/server.log" 2>&1 &
server_pid=$!

for _ in $(seq 1 60); do
  if [[ "$(curl --silent --output /dev/null --write-out '%{http_code}' "$base_url/healthz" || true)" == 200 ]]; then
    break
  fi
  sleep 1
done
[[ "$(curl --silent --output /dev/null --write-out '%{http_code}' "$base_url/healthz")" == 200 ]] \
  || fail 'candidate server never became healthy; see server.log'

bootstrap_status=$(curl --silent --show-error --output /dev/null --write-out '%{http_code}' \
  --header 'Content-Type: application/json' --data-binary @- "$base_url/auth/bootstrap" <<EOF
{"username":"$username","password":"$password"}
EOF
)
[[ "$bootstrap_status" == 204 ]] || fail "loopback bootstrap returned HTTP $bootstrap_status"

cd "$out_dir"
context_input=$'1\ndev\n'"$base_url"$'\n\n\n'
if [[ "$use_native" -eq 1 ]]; then
  run_pty "$context_input" "$out_dir/context.pty" \
    env "TAPSTATE_WORKDIR=$workspace" "$cli_binary" "-Duser.home=$home_dir" context
else
  run_pty "$context_input" "$out_dir/context.pty" \
    env "TAPSTATE_WORKDIR=$workspace" java "-Duser.home=$home_dir" -jar "$cli_jar" context
fi
grep -q 'created context dev' "$out_dir/context.pty" || fail 'context creation was not observed'
grep -q 'bound dev' "$out_dir/context.pty" || fail 'exact workspace binding was not observed'

if [[ "$use_native" -eq 1 ]]; then
  run_pty "$password"$'\n' "$out_dir/login.pty" \
    env "TAPSTATE_WORKDIR=$workspace" "$cli_binary" "-Duser.home=$home_dir" auth login "$username"
else
  run_pty "$password"$'\n' "$out_dir/login.pty" \
    env "TAPSTATE_WORKDIR=$workspace" java "-Duser.home=$home_dir" -jar "$cli_jar" auth login "$username"
fi
grep -q 'signed in as' "$out_dir/login.pty" || fail 'masked terminal login was not observed'

[[ -d "$auth_dir" ]] || fail 'auth directory was not created'
auth_file=$(find "$auth_dir" -maxdepth 1 -type f -name '*.json' -print -quit)
[[ -n "$auth_file" ]] || fail 'auth record was not created'
auth_dir_mode=$(mode "$auth_dir")
auth_file_mode=$(mode "$auth_file")
config_mode=$(mode "$home_dir/.tapstate/config.yaml")
[[ "$auth_dir_mode" == 700 ]] || fail "auth directory mode was $auth_dir_mode, expected 700"
[[ "$auth_file_mode" == 600 ]] || fail "auth file mode was $auth_file_mode, expected 600"
[[ "$config_mode" == 600 ]] || fail "config mode was $config_mode, expected 600"

python3 - "$auth_file" "$out_dir/auth-summary.json" <<'PY'
import json, re, sys

auth_path, summary_path = sys.argv[1:]
record = json.load(open(auth_path, encoding="utf-8"))
expected = {
    "version", "authRef", "contextId", "issuer", "principal", "scopes",
    "sessionToken", "createdAt", "idleExpiresAt", "absoluteExpiresAt",
}
if set(record) != expected:
    raise SystemExit("unexpected auth record keys")
if not re.fullmatch(r"tss_[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", record["sessionToken"]):
    raise SystemExit("session token is not opaque tss material")
if any(key.lower() in {"password", "token", "access_token", "jwt"} for key in record if key != "sessionToken"):
    raise SystemExit("auth record contains a forbidden credential field")
json.dump({
    "authKeys": sorted(record),
    "issuer": record["issuer"],
    "principal": record["principal"],
    "scopes": record["scopes"],
    "opaqueSessionFormat": True,
}, open(summary_path, "w", encoding="utf-8"), indent=2, sort_keys=True)
PY

run_cli() {
  if [[ "$use_native" -eq 1 ]]; then
    TAPSTATE_WORKDIR="$workspace" "$cli_binary" "-Duser.home=$home_dir" "$@"
  else
    TAPSTATE_WORKDIR="$workspace" java "-Duser.home=$home_dir" -jar "$cli_jar" "$@"
  fi
}

run_cli auth status >"$out_dir/status.stdout" 2>"$out_dir/status.stderr"
grep -q 'signed in as' "$out_dir/status.stdout" || fail 'fresh-process auth status did not resume the session'
[[ ! -s "$out_dir/status.stderr" ]] || fail 'fresh-process auth status wrote unexpected diagnostics'
run_cli ls >"$out_dir/resume.stdout" 2>"$out_dir/resume.stderr"
grep -q '^source  views$' "$out_dir/resume.stdout" || fail 'fresh-process authenticated read did not list the managed store'
[[ ! -s "$out_dir/resume.stderr" ]] || fail 'fresh-process authenticated read wrote unexpected diagnostics'

python3 - "$base_url" "$auth_file" <<'PY'
import http.client, json, sys, urllib.parse

base_url, auth_path = sys.argv[1:]
parsed = urllib.parse.urlparse(base_url)
record = json.load(open(auth_path, encoding="utf-8"))
connection = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=10)
connection.request("POST", "/auth/logout", headers={"Authorization": "TapstateSession " + record["sessionToken"]})
response = connection.getresponse()
response.read()
if response.status != 204:
    raise SystemExit(f"server-side revoke returned {response.status}")
PY

set +e
run_cli ls >"$out_dir/revoked.stdout" 2>"$out_dir/revoked.stderr"
revoked_rc=$?
set -e
[[ "$revoked_rc" -ne 0 ]] || fail 'revoked session unexpectedly resumed'
grep -q 'cli.auth-session-rejected' "$out_dir/revoked.stderr" || fail 'revoked resume did not return the stable rejection diagnostic'

kill -TERM "$server_pid"
wait "$server_pid" || true
server_pid=''
run_cli auth logout --local-only \
  >"$out_dir/local-only.stdout" 2>"$out_dir/local-only.stderr"
[[ ! -s "$out_dir/local-only.stdout" ]] || fail 'local-only logout polluted stdout'
grep -q 'local session removed; the remote session remains valid until expiry' "$out_dir/local-only.stderr" \
  || fail 'offline local-only logout did not report the remote-session warning'
[[ ! -e "$auth_file" ]] || fail 'local-only logout did not remove the local auth record'

empty_home="$out_dir/empty-home"
mkdir -p "$empty_home"
set +e
if [[ "$use_native" -eq 1 ]]; then
  TAPSTATE_WORKDIR="$workspace" "$cli_binary" "-Duser.home=$empty_home" ls \
    >"$out_dir/no-context.stdout" 2>"$out_dir/no-context.stderr"
else
  TAPSTATE_WORKDIR="$workspace" java "-Duser.home=$empty_home" -jar "$cli_jar" ls \
    >"$out_dir/no-context.stdout" 2>"$out_dir/no-context.stderr"
fi
no_context_rc=$?
set -e
[[ "$no_context_rc" -ne 0 ]] || fail 'non-TTY online call without a context unexpectedly succeeded'
[[ ! -s "$out_dir/no-context.stdout" ]] || fail 'non-TTY missing-context call polluted stdout'
grep -q 'cli.context-required' "$out_dir/no-context.stderr" || fail 'non-TTY missing-context diagnostic was absent'
! grep -Eq 'Password:|Context action:|choice \[1-' "$out_dir/no-context.stderr" \
  || fail 'non-TTY missing-context call opened an interactive prompt'

TAPSTATE_EXPECTED_PASSWORD="$password" python3 - "$out_dir" <<'PY'
import os, pathlib, sys

base = pathlib.Path(sys.argv[1])
needle = os.environ["TAPSTATE_EXPECTED_PASSWORD"].encode()
for path in base.rglob("*"):
    if path.is_file() and needle in path.read_bytes():
        raise SystemExit(f"password leaked into {path.name}")
PY

server_commit=$(git -C "$root" rev-parse HEAD)
python3 - "$out_dir/auth-summary.json" "$out_dir/report.json" "$server_commit" "$timestamp" \
  "$auth_dir_mode" "$auth_file_mode" "$config_mode" "$cli_runtime" <<'PY'
import json, sys

summary_path, report_path, commit, observed_at, auth_dir_mode, auth_file_mode, config_mode, cli_runtime = sys.argv[1:]
summary = json.load(open(summary_path, encoding="utf-8"))
report = {
    "result": "passed",
    "observedAtUtc": observed_at,
    "candidateCommit": commit,
    "cliRuntime": cli_runtime,
    "platform": "POSIX",
    "cacheModes": {"authDirectory": auth_dir_mode, "authRecord": auth_file_mode, "config": config_mode},
    "authRecord": summary,
    "cases": [
        "pty-context-create-and-exact-workspace-bind",
        "pty-masked-password-login",
        "owner-only-cache-modes-and-opaque-record",
        "fresh-process-session-resume-and-online-read",
        "server-side-session-revoke-rejects-resume",
        "offline-local-only-logout-removes-only-local-cache",
        "non-tty-no-context-is-prompt-free-and-stdout-clean",
        "password-absent-from-retained-test-artifacts",
    ],
    "redaction": "No password, JWT, Mongo URI, or session-token value is recorded.",
}
with open(report_path, "w", encoding="utf-8") as handle:
    json.dump(report, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY

printf 'verification passed\nreport: %s\n' "$out_dir/report.json"
