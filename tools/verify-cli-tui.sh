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
mkdir -p "$workspace/source"
cat >"$workspace/source/src_orders.tap.yml" <<'EOF'
version: tapstate/v1
kind: source
id: src_orders
connector: mysql
config: {}
mode: cdc
tables: [orders]
EOF
# Context bindings use the filesystem's canonical path, including macOS's /private prefix.
workspace_path=$(cd "$workspace" && pwd -P)
# macOS may display the same directory through /var while persistence uses its /private/var spelling.
workspace_display_path="${workspace_path#/private}"

run_pty() {
  local input=$1
  local output=$2
  shift 2
  set +e
  TAPSTATE_PTY_INPUT="$input" TAPSTATE_PTY_FOLLOWUP="${TAPSTATE_PTY_FOLLOWUP:-}" \
    TAPSTATE_PTY_FOLLOWUP_DELAY="${TAPSTATE_PTY_FOLLOWUP_DELAY:-1.0}" python3 - "$@" >"$output" <<'PY'
import os
import fcntl
import pty
import select
import signal
import struct
import sys
import termios
import time

raw_data = os.environ.pop("TAPSTATE_PTY_INPUT").encode()
chunks = raw_data.split(b"\x1e")
followup = os.environ.pop("TAPSTATE_PTY_FOLLOWUP", "").encode()
followup_delay = float(os.environ.pop("TAPSTATE_PTY_FOLLOWUP_DELAY", "1.0"))
signal_name = os.environ.pop("TAPSTATE_PTY_SIGNAL", "")
signal_delay = float(os.environ.pop("TAPSTATE_PTY_SIGNAL_DELAY", "1.0"))
chunk_delay = float(os.environ.pop("TAPSTATE_PTY_CHUNK_DELAY", "0.35"))
resize_spec = os.environ.pop("TAPSTATE_PTY_RESIZE", "")
resize_delay = float(os.environ.pop("TAPSTATE_PTY_RESIZE_DELAY", "0.75"))
initial_spec = os.environ.pop("TAPSTATE_PTY_INITIAL_SIZE", "100x24")
try:
    initial_cols, initial_rows = (int(value) for value in initial_spec.lower().split("x", 1))
except ValueError:
    raise SystemExit("TAPSTATE_PTY_INITIAL_SIZE must be COLSxROWS")
if resize_spec:
    try:
        resize_cols, resize_rows = (int(value) for value in resize_spec.lower().split("x", 1))
    except ValueError:
        raise SystemExit("TAPSTATE_PTY_RESIZE must be COLSxROWS")
argv = sys.argv[1:]
pid, fd = pty.fork()
if pid == 0:
    # The host shell may export TERM=dumb even though pty.fork gave us a capable pseudo-terminal.
    # Force a real terminfo entry so JLine can enter raw mode and decode the dashboard keys.
    os.environ["TERM"] = os.environ.get("TAPSTATE_PTY_TERM", "xterm-256color")
    os.execvp(argv[0], argv)
fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", initial_rows, initial_cols, 0, 0))

output = bytearray()
os.set_blocking(fd, False)
deadline = time.time() + 30
status = None
start = time.time()
initial_sent = False
sent_at = None
chunk_index = 0
followup_sent = not followup
signal_sent = not signal_name
resize_sent = not resize_spec
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
    if initial_sent and not resize_sent and now - sent_at >= resize_delay:
        fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", resize_rows, resize_cols, 0, 0))
        os.kill(pid, signal.SIGWINCH)
        resize_sent = True
    if initial_sent and chunk_index == len(chunks) and not followup_sent and now - sent_at >= followup_delay:
        os.write(fd, followup)
        followup_sent = True
    if initial_sent and not signal_sent and now - sent_at >= signal_delay:
        os.kill(pid, getattr(signal, "SIG" + signal_name))
        signal_sent = True
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

assert_terminal_cleanup() {
  local output=$1
  python3 - "$output" <<'PY'
import pathlib
import re
import sys

value = pathlib.Path(sys.argv[1]).read_bytes()
cursor_visible = any(
    b"25" in match.group(1).split(b";")
    for match in re.finditer(rb"\x1b\[\?([0-9;]+)h", value)
)
missing = []
if not cursor_visible:
    missing.append(b"CSI ? ... 25 h")
if b"\x1b[?1049l" not in value:
    missing.append(b"\x1b[?1049l")
if missing:
    print("missing TUI terminal cleanup:", ", ".join(repr(item) for item in missing), file=sys.stderr)
    sys.exit(1)
PY
}

assert_context_binding() {
  local config=$1
  local workspace=$2
  local context=$3
  grep -Fq "  \"$context\":" "$config" || {
    echo "missing persisted context: $context" >&2
    exit 1
  }
  grep -Fq "  \"$workspace\": \"$context\"" "$config" || {
    echo "missing persisted workspace binding: $workspace -> $context" >&2
    exit 1
  }
}

context_output="$work_dir/context.pty"
context_name="tui-smoke"
context_input=$'context\n\x1e\n\x1e'"$context_name"$'\n\x1ehttp://127.0.0.1:8081\n\x1en\n\x1e\n\x1e'
set +e
TAPSTATE_PTY_CHUNK_DELAY=1.0 TAPSTATE_PTY_FOLLOWUP=$'\004' TAPSTATE_PTY_FOLLOWUP_DELAY=5 \
  run_pty "$context_input" "$context_output" env HOME="$home_dir" TAPSTATE_WORKDIR="$workspace_path" \
  java -Duser.home="$home_dir" -jar "$jar" tui
context_rc=$?
set -e
[[ "$context_rc" -eq 0 ]] || {
  echo "context TUI smoke exited with $context_rc" >&2
  tail -c 4000 "$context_output" >&2 || true
  exit 1
}
assert_terminal_cleanup "$context_output"
test -f "$home_dir/.tapstate/config.yaml"
assert_context_binding "$home_dir/.tapstate/config.yaml" "$workspace_path" "$context_name"

palette_output="$work_dir/palette.pty"
# The palette starts with the fixed TUI commands. Select and execute the stable fourth entry (:help)
# so this smoke does not depend on registry ordering or trigger network I/O.
palette_input=$'\x10\x1e\e[B\x1e\e[B\x1e\e[B\x1e\n\x1e\n\x1e\004'
set +e
TAPSTATE_PTY_CHUNK_DELAY=0.4 TAPSTATE_PTY_FOLLOWUP=$'\004' TAPSTATE_PTY_FOLLOWUP_DELAY=3 \
  run_pty "$palette_input" "$palette_output" env HOME="$home_dir" TAPSTATE_WORKDIR="$workspace_path" \
  java -Duser.home="$home_dir" -jar "$jar" tui
palette_rc=$?
set -e
[[ "$palette_rc" -eq 0 ]] || {
  echo "palette TUI smoke exited with $palette_rc" >&2
  tail -c 4000 "$palette_output" >&2 || true
  exit 1
}
# JLine redraws over transient selection frames. The retained activity and successful help result
# prove selection was followed by execution without depending on overwritten screen cells.
assert_output "$palette_output" "> :help" "Result · success" "run one command against a server and exit"
assert_terminal_cleanup "$palette_output"

term_output="$work_dir/term.pty"
set +e
TAPSTATE_PTY_SIGNAL=TERM TAPSTATE_PTY_SIGNAL_DELAY=1 \
  run_pty $'' "$term_output" env HOME="$home_dir" TAPSTATE_WORKDIR="$workspace_path" \
  java -Duser.home="$home_dir" -jar "$jar" tui
term_rc=$?
set -e
[[ "$term_rc" -eq 0 ]] || {
  echo "SIGTERM TUI smoke exited with $term_rc (expected graceful exit 0)" >&2
  tail -c 4000 "$term_output" >&2 || true
  exit 1
}
assert_terminal_cleanup "$term_output"

cancel_output="$work_dir/cancel.pty"
cancel_input=$'pw\x1e\003'
set +e
TAPSTATE_PTY_CHUNK_DELAY=0.4 TAPSTATE_PTY_FOLLOWUP=$'\004' TAPSTATE_PTY_FOLLOWUP_DELAY=3 \
  run_pty "$cancel_input" "$cancel_output" env HOME="$home_dir" TAPSTATE_WORKDIR="$workspace" \
  java -Duser.home="$home_dir" -jar "$jar" tui
cancel_rc=$?
set -e
[[ "$cancel_rc" -eq 0 ]] || {
  echo "Ctrl-C TUI smoke exited with $cancel_rc" >&2
  tail -c 4000 "$cancel_output" >&2 || true
  exit 1
}
assert_output "$cancel_output" "command cleared" "[COMMAND] >"
assert_terminal_cleanup "$cancel_output"

navigation_output="$work_dir/navigation.pty"
navigation_input=$'\e[B\x1e\n\x1e\e\x1e\004'
set +e
TAPSTATE_PTY_CHUNK_DELAY=0.5 TAPSTATE_PTY_FOLLOWUP=$'\004' TAPSTATE_PTY_FOLLOWUP_DELAY=2 \
  run_pty "$navigation_input" "$navigation_output" env HOME="$home_dir" TAPSTATE_WORKDIR="$workspace_path" \
  java -Duser.home="$home_dir" -jar "$jar" tui
navigation_rc=$?
set -e
[[ "$navigation_rc" -eq 0 ]] || {
  echo "navigation TUI smoke exited with $navigation_rc" >&2
  tail -c 4000 "$navigation_output" >&2 || true
  exit 1
}
# Selection notices are transient under JLine redraw. The retained result pane proves that Down
# selected the fixture and Enter opened its details before Esc returned to the resource list.
assert_output "$navigation_output" "Result · success" "src_orders" "mysql · cdc"
assert_terminal_cleanup "$navigation_output"

resize_output="$work_dir/resize.pty"
resize_input=$'\x1e'
set +e
TAPSTATE_PTY_INITIAL_SIZE=120x24 TAPSTATE_PTY_RESIZE=50x12 TAPSTATE_PTY_RESIZE_DELAY=0.75 \
  TAPSTATE_PTY_FOLLOWUP=$'\004' TAPSTATE_PTY_FOLLOWUP_DELAY=1.5 \
  run_pty "$resize_input" "$resize_output" env HOME="$home_dir" TAPSTATE_WORKDIR="$workspace_path" \
  java -Duser.home="$home_dir" -jar "$jar" tui
resize_rc=$?
set -e
[[ "$resize_rc" -eq 0 ]] || {
  echo "resize TUI smoke exited with $resize_rc" >&2
  tail -c 4000 "$resize_output" >&2 || true
  exit 1
}
# The wide header is emitted before SIGWINCH; the compact identity and status rows prove the
# resized frame was rendered even when short-height rules omit the optional hint line.
assert_output "$resize_output" "tapstate  tui-smoke" "offline · tui-smoke" "status:"
assert_terminal_cleanup "$resize_output"

echo 'TUI smoke: alternate-screen, context prompts, palette, Ctrl-C, navigation, SIGTERM, resize, command execution, and cleanup passed'
