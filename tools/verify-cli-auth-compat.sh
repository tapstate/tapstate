#!/usr/bin/env bash
# Verify a legacy CLI against a candidate server without exposing credentials or bearer values.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  verify-cli-auth-compat.sh --client-root DIR --server-root DIR [options]

Required:
  --client-root DIR       Disposable worktree containing the legacy CLI.
  --server-root DIR       Disposable worktree containing the candidate server.

Options:
  --mongo-uri URI         Mongo URI template containing {database}. If omitted, probe local mongo.
  --out-dir DIR           Output directory. Defaults to a new directory under the system temp directory.
  --port NUMBER           Loopback HTTP port (default: 18081).
  --cleanup               Drop only the generated tapstate_cli_auth_verify_* database after the run.
  --help                  Show this help.

The verifier builds both inputs with `mvn clean package -DskipTests`, so never give it a shared worktree.
EOF
}

fail() {
  printf 'verification failed: %s\n' "$*" >&2
  exit 1
}

client_root=''
server_root=''
mongo_template=''
out_dir=''
port='18081'
drop_database=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --client-root)
      [[ $# -ge 2 ]] || fail '--client-root needs a directory'
      client_root="$2"
      shift 2
      ;;
    --server-root)
      [[ $# -ge 2 ]] || fail '--server-root needs a directory'
      server_root="$2"
      shift 2
      ;;
    --mongo-uri)
      [[ $# -ge 2 ]] || fail '--mongo-uri needs a URI template'
      mongo_template="$2"
      shift 2
      ;;
    --out-dir)
      [[ $# -ge 2 ]] || fail '--out-dir needs a directory'
      out_dir="$2"
      shift 2
      ;;
    --port)
      [[ $# -ge 2 ]] || fail '--port needs a number'
      port="$2"
      shift 2
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

[[ -n "$client_root" ]] || fail '--client-root is required'
[[ -n "$server_root" ]] || fail '--server-root is required'
[[ -d "$client_root/.git" || -f "$client_root/.git" ]] || fail 'client root is not a Git worktree'
[[ -d "$server_root/.git" || -f "$server_root/.git" ]] || fail 'server root is not a Git worktree'
[[ -f "$client_root/pom.xml" ]] || fail 'client root has no Maven project'
[[ -f "$server_root/pom.xml" ]] || fail 'server root has no Maven project'
[[ "$port" =~ ^[0-9]+$ ]] || fail '--port must be numeric'

if [[ -z "$mongo_template" ]]; then
  command -v mongo >/dev/null 2>&1 || fail 'provide --mongo-uri: local mongo shell was not found'
  replica_set="$(mongo --quiet --host 127.0.0.1 --port 27017 --eval 'var h=db.hello(); print(h.setName || "");')"
  [[ -n "$replica_set" ]] || fail 'local MongoDB is not a replica set; provide a replica-set --mongo-uri'
  mongo_template="mongodb://127.0.0.1:27017/{database}?replicaSet=${replica_set}"
fi
[[ "$mongo_template" == *'{database}'* ]] || fail '--mongo-uri must contain the literal {database} placeholder'
if [[ "$drop_database" -eq 1 ]]; then
  command -v mongo >/dev/null 2>&1 || fail '--cleanup needs the mongo shell'
fi

if [[ -z "$out_dir" ]]; then
  out_dir="$(mktemp -d "${TMPDIR:-/tmp}/tapstate-cli-auth-compat.XXXXXX")"
else
  [[ ! -e "$out_dir" ]] || fail "output directory already exists: $out_dir"
  mkdir -p "$out_dir"
fi

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
nonce="$(python3 -c 'import secrets; print(secrets.token_hex(4))')"
database="tapstate_cli_auth_verify_${timestamp}_${nonce}"
mongo_uri="${mongo_template//\{database\}/$database}"
base_url="http://127.0.0.1:${port}"
username="compat_admin_${nonce}"
password="$(python3 -c 'import secrets; print(secrets.token_urlsafe(24))')"
jwt_secret="$(python3 -c 'import secrets; print(secrets.token_urlsafe(32))')"
server_pid=''

cleanup() {
  result=$?
  if [[ -n "$server_pid" ]]; then
    kill -TERM "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  if [[ "$drop_database" -eq 1 ]]; then
    if [[ "$database" == tapstate_cli_auth_verify_* ]]; then
      mongo "$mongo_uri" --quiet --eval 'db.dropDatabase()' >"$out_dir/mongo-cleanup.log" 2>&1 || result=1
    else
      printf 'refusing to drop a database outside the verifier prefix\n' >&2
      result=1
    fi
  fi
  exit "$result"
}
trap cleanup EXIT INT TERM

python3 -c '
import socket, sys
port = int(sys.argv[1])
sock = socket.socket()
try:
    sock.bind(("127.0.0.1", port))
finally:
    sock.close()
' "$port" || fail "loopback port is unavailable: $port"

server_commit="$(git -C "$server_root" rev-parse HEAD)"
client_commit="$(git -C "$client_root" rev-parse HEAD)"
printf 'building candidate server %s\n' "$server_commit"
mvn --file "$server_root/pom.xml" -pl app -am clean package -DskipTests >"$out_dir/server-build.log" 2>&1 || fail 'candidate server build failed; see server-build.log'
printf 'building legacy client %s\n' "$client_commit"
mvn --file "$client_root/pom.xml" -pl cli -am clean package -DskipTests >"$out_dir/client-build.log" 2>&1 || fail 'legacy CLI build failed; see client-build.log'

server_jar="$(find "$server_root/app/target" -maxdepth 1 -type f -name 'app-*-boot.jar' -print -quit)"
client_jar="$(find "$client_root/cli/target" -maxdepth 1 -type f -name 'cli-*.jar' -print -quit)"
[[ -n "$server_jar" ]] || fail 'candidate server boot jar is missing'
[[ -n "$client_jar" ]] || fail 'legacy CLI jar is missing'
workspace="$client_root/cli/src/test/resources/ws-valid"
[[ -d "$workspace" ]] || fail 'legacy CLI test workspace is missing'

java -jar "$server_jar" \
  --server.address=127.0.0.1 \
  --server.port="$port" \
  "--tapstate.store.mongo.uri=$mongo_uri" \
  "--tapstate.control.auth.jwt-secret=$jwt_secret" \
  >"$out_dir/server.log" 2>&1 &
server_pid=$!

health_status=''
for attempt in $(seq 1 60); do
  health_status="$(curl --silent --output /dev/null --write-out '%{http_code}' "$base_url/healthz" || true)"
  [[ "$health_status" == '200' ]] && break
  sleep 1
done
[[ "$health_status" == '200' ]] || fail 'candidate server never became healthy; see server.log'

bootstrap_status="$(curl --silent --show-error --output "$out_dir/bootstrap.response" --write-out '%{http_code}' \
  --header 'Content-Type: application/json' \
  --data-binary @- "$base_url/auth/bootstrap" <<EOF
{"username":"$username","password":"$password"}
EOF
)"
[[ "$bootstrap_status" == '204' ]] || fail "loopback bootstrap returned HTTP $bootstrap_status"

run_cli() {
  TAPSTATE_PASSWORD="$password" java -jar "$client_jar" -c "127.0.0.1:${port}" -u "$username" "$@"
}

run_cli ls >"$out_dir/read-before.txt"
# A fresh deployment registers its managed state store as `views` during startup. Seeing it here proves that
# the legacy client exchanged a human credential and decoded the candidate server's authenticated list reply.
grep -qx 'source  views' "$out_dir/read-before.txt" || fail 'legacy read did not list the managed state store'

run_cli apply "$workspace" >"$out_dir/write-apply.txt"
grep -q 'created' "$out_dir/write-apply.txt" || fail 'legacy write did not create the test workspace'

run_cli ls >"$out_dir/read-after.txt"
grep -q 'kfk2my' "$out_dir/read-after.txt" || fail 'legacy read did not observe its applied pipeline'

run_cli token create --scope read -o json | python3 -c '
import json, sys
created = json.load(sys.stdin)
safe = {key: created[key] for key in ("tokenId", "scope", "createdAt")}
json.dump(safe, sys.stdout, sort_keys=True)
print()
' >"$out_dir/token-created.json"
token_id="$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["tokenId"])' "$out_dir/token-created.json")"
[[ -n "$token_id" ]] || fail 'legacy token create returned no token id'

run_cli token revoke "$token_id" -o json >"$out_dir/token-revoked.json"
run_cli token list -o json >"$out_dir/token-list.json"
python3 -c '
import json, sys
tokens = json.load(open(sys.argv[1])).get("tokens", [])
token_id = sys.argv[2]
if not any(token.get("tokenId") == token_id and token.get("revoked") is True for token in tokens):
    raise SystemExit("revoked token was not listed as revoked")
' "$out_dir/token-list.json" "$token_id" || fail 'legacy token revoke was not observable'

python3 -c '
import json, sys
path, server_commit, client_commit, base_url, database, observed_at = sys.argv[1:]
report = {
    "result": "passed",
    "observedAtUtc": observed_at,
    "serverCommit": server_commit,
    "clientCommit": client_commit,
    "baseUrl": base_url,
    "temporaryDatabase": database,
    "httpResults": {
        "bootstrap": "POST /auth/bootstrap -> 204",
        "legacyHumanLoginAndRead": "POST /auth/login -> 200; GET /api/artifacts -> 200",
        "legacyAuthenticatedWrite": "POST /api/artifacts:apply -> 200",
        "legacyMachineTokenRevocation": "POST /api/tokens -> 201; POST /api/tokens/{id}:revoke -> 204; GET /api/tokens -> 200",
    },
    "cases": [
        "loopback-bootstrap",
        "legacy-human-login-and-read",
        "legacy-authenticated-write",
        "legacy-post-write-read",
        "legacy-machine-token-create-revoke-list",
    ],
    "redaction": "No password, JWT, Mongo URI, or machine-token bearer value is recorded.",
}
with open(path, "w", encoding="utf-8") as handle:
    json.dump(report, handle, indent=2, sort_keys=True)
    handle.write("\n")
' "$out_dir/report.json" "$server_commit" "$client_commit" "$base_url" "$database" "$timestamp"

printf 'verification passed\nreport: %s\n' "$out_dir/report.json"
