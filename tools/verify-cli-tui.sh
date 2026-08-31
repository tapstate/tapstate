#!/usr/bin/env bash
# Black-box smoke checks for the full-screen CLI TUI.
#
# The suite drives a real pseudo-terminal, because a pipe cannot exercise raw mode, alternate-screen
# entry, escape-key decoding, or prompt input. It keeps the home directory and workspace disposable,
# and only runs offline commands, so it does not need a server or credentials.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: verify-cli-tui.sh [--build] [--jar PATH]

Options:
  --build       Build the CLI and its dependencies before running the smoke checks.
  --jar PATH    Use this CLI jar instead of the newest cli/target/cli-*.jar.
  --help        Show this help.

The checks use a temporary user.home and workspace, then remove them on exit.
EOF
}

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
build=0
jar=''

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build)
      build=1
      shift
      ;;
    --jar)
      [[ $# -ge 2 ]] || { echo '--jar needs a path' >&2; exit 2; }
      jar=$2
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ "$build" -eq 1 ]]; then
  echo 'building CLI candidate'
  mvn --file "$repo_root/pom.xml" -q -pl cli -am package -DskipTests
fi

if [[ -z "$jar" ]]; then
  jar=$(ls -t "$repo_root"/cli/target/cli-*.jar 2>/dev/null | head -1 || true)
fi
[[ -f "$jar" ]] || {
  echo 'no CLI jar found; pass --build or --jar PATH' >&2
  exit 2
}

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/tapstate-cli-tui.XXXXXX")
if [[ "${TAPSTATE_KEEP_TMP:-0}" == 1 ]]; then
  trap 'echo "TUI smoke artifacts: $work_dir" >&2' EXIT INT TERM
else
  trap 'rm -rf "$work_dir"' EXIT INT TERM
fi
home_dir="$work_dir/home"
workspace="$work_dir/workspace"
mkdir -p "$home_dir" "$workspace"
# Java's Path normalization removes duplicate separators but deliberately keeps the logical /var path.
workspace_path=$(printf '%s' "$workspace" | sed -E 's#/{2,}#/#g')

run_pty() {
  local input=$1
  local output=$2
  shift 2
  set +e
  TAPSTATE_PTY_INPUT="$input" TAPSTATE_PTY_FOLLOWUP="${TAPSTATE_PTY_FOLLOWUP:-}" \
    TAPSTATE_PTY_FOLLOWUP_DELAY="${TAPSTATE_PTY_FOLLOWUP_DELAY:-1.0}" python3 - "$@" >"$output" <<'PY'
import os
import pty
import select
import signal
import sys
import time

raw_data = os.environ.pop("TAPSTATE_PTY_INPUT").encode()
chunks = raw_data.split(b"\x1e")
followup = os.environ.pop("TAPSTATE_PTY_FOLLOWUP", "").encode()
followup_delay = float(os.environ.pop("TAPSTATE_PTY_FOLLOWUP_DELAY", "1.0"))
chunk_delay = float(os.environ.pop("TAPSTATE_PTY_CHUNK_DELAY", "0.35"))
argv = sys.argv[1:]
pid, fd = pty.fork()
if pid == 0:
    # The host shell may export TERM=dumb even though pty.fork gave us a capable pseudo-terminal.
    # Force a real terminfo entry so JLine can enter raw mode and decode the dashboard keys.
    os.environ["TERM"] = os.environ.get("TAPSTATE_PTY_TERM", "xterm-256color")
    os.execvp(argv[0], argv)

output = bytearray()
os.set_blocking(fd, False)
deadline = time.time() + 30
status = None
start = time.time()
initial_sent = False
sent_at = None
chunk_index = 0
followup_sent = not followup
while time.time() < deadline:
    readable, _, _ = select.select([fd], [], [], 0.25)
    if readable:
        try:
            chunk = os.read(fd, 8192)
        except OSError:
            chunk = b""
        if not chunk:
            break
        output.extend(chunk)
    now = time.time()
    if not initial_sent and (readable or now - start >= 2):
        os.write(fd, chunks[chunk_index])
        chunk_index += 1
        initial_sent = True
        sent_at = now
    if initial_sent and chunk_index < len(chunks) and now - sent_at >= chunk_delay:
        os.write(fd, chunks[chunk_index])
        chunk_index += 1
        sent_at = now
    if initial_sent and chunk_index == len(chunks) and not followup_sent and now - sent_at >= followup_delay:
        os.write(fd, followup)
        followup_sent = True
    done, status = os.waitpid(pid, os.WNOHANG)
    if done:
        break
else:
    os.kill(pid, signal.SIGKILL)
    _, status = os.waitpid(pid, 0)
    output.extend(b"\nPTY timeout\n")

try:
    while True:
        chunk = os.read(fd, 8192)
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

assert_output() {
  local output=$1
  shift
  python3 - "$output" "$@" <<'PY'
import pathlib
import re
import sys

path = pathlib.Path(sys.argv[1])
value = path.read_bytes().decode("utf-8", "replace")
# JLine Display redraws with CSI cursor movement and erase sequences. Keep the accumulated text,
# because the smoke assertions intentionally prove that a status was rendered at least once.
value = re.sub(r"\x1b\][^\x07]*(?:\x07|\x1b\\)", "", value)
value = re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", value)
value = value.replace("\r", "")
missing = [needle for needle in sys.argv[2:] if needle not in value]
if missing:
    print("missing TUI output:", ", ".join(repr(item) for item in missing), file=sys.stderr)
    print(value[-4000:], file=sys.stderr)
    sys.exit(1)
PY
}

context_output="$work_dir/context.pty"
context_name="tui-smoke"
context_input=$'context\n\x1e\n\x1e'"$context_name"$'\n\x1ehttp://127.0.0.1:8081\n\x1en\n\x1e\n\x1e'
set +e
TAPSTATE_PTY_CHUNK_DELAY=1.0 TAPSTATE_PTY_FOLLOWUP=q TAPSTATE_PTY_FOLLOWUP_DELAY=5 \
  run_pty "$context_input" "$context_output" env HOME="$home_dir" TAPSTATE_WORKDIR="$workspace" \
  java -Duser.home="$home_dir" -jar "$jar" tui
context_rc=$?
set -e
[[ "$context_rc" -eq 0 ]] || {
  echo "context TUI smoke exited with $context_rc" >&2
  tail -c 4000 "$context_output" >&2 || true
  exit 1
}
assert_output "$context_output" "bound $context_name"
test -f "$home_dir/.tapstate/config.yaml"
grep -q "$context_name" "$home_dir/.tapstate/config.yaml"

palette_output="$work_dir/palette.pty"
palette_input=$'\x10\x1e\e[B\x1e\n\x1e\n'
set +e
TAPSTATE_PTY_CHUNK_DELAY=0.4 TAPSTATE_PTY_FOLLOWUP=q TAPSTATE_PTY_FOLLOWUP_DELAY=1 \
  run_pty "$palette_input" "$palette_output" env HOME="$home_dir" TAPSTATE_WORKDIR="$workspace" \
  java -Duser.home="$home_dir" -jar "$jar" tui
palette_rc=$?
set -e
[[ "$palette_rc" -eq 0 ]] || {
  echo "palette TUI smoke exited with $palette_rc" >&2
  tail -c 4000 "$palette_output" >&2 || true
  exit 1
}
assert_output "$palette_output" "selected: pwd" "$workspace_path"

echo 'TUI smoke: alternate-screen, context prompts, palette selection, and command execution passed'
