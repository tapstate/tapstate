#!/usr/bin/env bash
#
# Native smoke suite for the Tapstate CLI (poc1 D5).
#
# Exercises the GraalVM native-image binary as a black box: it must do the same offline work the
# JVM build does, with every bundled resource (connector catalog / grammar schema / message catalog)
# reachable inside the image and startup under the acceptance budget. JVM unit tests cannot catch a
# missing resource or a reflection gap — only the produced binary can — so this script is the
# executable spec for native packaging. A final check drives one loopback online round-trip
# (connect / login / register) so the authenticated HTTP path is proven reachable in the image too.
#
# Usage:
#   cli/native-smoke.sh [--build] [path-to-binary]
#     --build           build the native image first (mvn -Pnative ...), discovering a JDK-21
#                       native-image toolchain (Liberica NIK for release, Oracle GraalVM for local dev)
#     path-to-binary    the tapstate binary to test (default: cli/target/tapstate)
#
# Exit 0 iff every check passes.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY=""
DO_BUILD=0
STARTUP_BUDGET_MS=200

for arg in "$@"; do
  case "$arg" in
    --build) DO_BUILD=1 ;;
    *) BINARY="$arg" ;;
  esac
done
[[ -z "$BINARY" ]] && BINARY="$REPO_ROOT/cli/target/tapstate"

red()   { printf '\033[31m%s\033[0m\n' "$1"; }
green() { printf '\033[32m%s\033[0m\n' "$1"; }
bold()  { printf '\033[1m%s\033[0m\n' "$1"; }
# strip CSI escape sequences: a pty makes the binary emit colour, so matches must run on clean text
strip_ansi() { sed $'s/\033\\[[0-9;]*[a-zA-Z]//g' | tr -d '\r'; }

# Drive the native binary under a real pty (JLine needs a terminal): feed $1 to its stdin, run it with
# the remaining args, capture all output into PTY_OUT and set PTY_RC=0 only on a clean child exit. A
# child that wedges past the deadline is SIGKILLed and always reaped, so the suite never orphans a
# 36MB process or hangs. PTY_RC, not just output greps, is what callers gate on.
pty_session() {
  local input="$1"; shift
  set +e
  PTY_OUT=$(TAPSTATE_BIN="$BINARY" TAPSTATE_PTY_INPUT="$input" TAPSTATE_PTY_TUI="${TAPSTATE_PTY_TUI:-}" python3 - "$@" <<'PY'
import fcntl, os, pty, struct, sys, select, termios, time, signal

binary = os.environ["TAPSTATE_BIN"]
data = os.environ["TAPSTATE_PTY_INPUT"].encode()
wait_for_workbench = os.environ.get("TAPSTATE_PTY_TUI") == "1"
argv = [binary] + sys.argv[1:]

# A TUI command must finish before Ctrl-D is delivered, otherwise the dashboard quite correctly
# treats the exit request as a request to stop the active operation. Keep completion-only input fast,
# but give submitted commands a short window to render their result first.
deferred_exit = None
if wait_for_workbench and b"\x04" in data:
    command_input, suffix = data.split(b"\x04", 1)
    data = command_input
    deferred_exit = (b"\x1b\x04" if data else b"\x04") + suffix

pid, fd = pty.fork()
if pid == 0:                              # child: the binary on a controlling terminal
    try:
        if os.environ.get("TERM", "") in ("", "dumb"):
            os.environ["TERM"] = "xterm-256color"
        os.execv(binary, argv)
    except Exception:
        os._exit(127)
else:
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 100, 0, 0))
    out = bytearray()
    input_sent = False
    exit_sent = deferred_exit is None
    input_sent_at = None
    write_failed = None
    deadline = time.time() + 15
    timed_out = True                     # cleared when the child closes the pty (clean EOF)

    def send_deferred_exit():
        # Keep Escape and Ctrl-D separate: the TUI needs one redraw to clear a draft before it
        # interprets Ctrl-D as the empty-command quit gesture.
        if deferred_exit.startswith(b"\x1b"):
            os.write(fd, deferred_exit[:1])
            time.sleep(0.2)
            os.write(fd, deferred_exit[1:])
        else:
            os.write(fd, deferred_exit)

    while time.time() < deadline:
        r, _, _ = select.select([fd], [], [], 0.5)
        if r:
            try:
                chunk = os.read(fd, 4096)
            except OSError:
                timed_out = False
                break
            if not chunk:
                timed_out = False
                break
            out += chunk
            # The workbench must enter terminal raw mode before input is written. Its title is
            # stable while the rest of the frame contains terminal control sequences. Explicit
            # commands can accept input as soon as they emit their first prompt bytes.
            ready = b"TAPSTATE" in out if wait_for_workbench else len(argv) > 1
            if ready and not input_sent:
                # The TamboUI backend finishes its terminal capability negotiation just before
                # the first frame. Do not let the PTY race that negotiation with the first key.
                time.sleep(0.75 if len(argv) == 1 else 0.05)
                try:
                    os.write(fd, data)
                except OSError as error:
                    write_failed = str(error)
                    timed_out = False
                    break
                input_sent = True
                input_sent_at = time.time()
                if deferred_exit is not None and not data:
                    try:
                        send_deferred_exit()
                    except OSError as error:
                        write_failed = str(error)
                        timed_out = False
                        break
                    exit_sent = True
        if wait_for_workbench and input_sent and not exit_sent and input_sent_at is not None:
            # TamboUI redraws completion immediately; command execution gets a few seconds to publish
            # its result before the test asks the workbench to leave.
            wait_for_command = b"\n" in data or b"\r" in data
            if (wait_for_command and time.time() - input_sent_at >= 8.0) \
                    or (not wait_for_command and time.time() - input_sent_at >= 0.8):
                try:
                    send_deferred_exit()
                except OSError as error:
                    write_failed = str(error)
                    timed_out = False
                    break
                exit_sent = True
    if timed_out:                        # never leave the native binary running
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    status = 0
    try:
        _, status = os.waitpid(pid, 0)   # blocking reap (no zombie)
    except ChildProcessError:
        pass
    try:
        os.close(fd)
    except OSError:
        pass
    sys.stdout.write(out.decode("utf-8", "replace"))
    sys.stdout.flush()
    if write_failed is not None:
        sys.stderr.write(f"could not write CLI input: {write_failed}\n")
    clean = (write_failed is None) and (not timed_out) and os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0
    sys.exit(0 if clean else 1)
PY
)
  PTY_RC=$?
  set -e
}

PASS=0
FAIL=0
ok()   { green "  PASS  $1"; PASS=$((PASS+1)); }
bad()  { red   "  FAIL  $1"; FAIL=$((FAIL+1)); }

# --- discover a JDK-21 native-image toolchain — Liberica NIK or GraalVM (only needed for --build) ---
discover_graalvm() {
  if [[ -n "${GRAALVM_HOME:-}" && -x "$GRAALVM_HOME/bin/native-image" ]]; then
    echo "$GRAALVM_HOME"; return 0
  fi
  # ask the macOS java_home registry for a JDK 21 that carries native-image (NIK or GraalVM)
  if [[ -x /usr/libexec/java_home ]]; then
    local home
    home="$(/usr/libexec/java_home -v 21 2>/dev/null || true)"
    if [[ -n "$home" && -x "$home/bin/native-image" ]]; then echo "$home"; return 0; fi
  fi
  # fall back to the jdkHome of the toolchain block that is a native-image vendor (Liberica NIK or
  # GraalVM) AND version 21. Parse per <toolchain> block (not by line proximity) so reordering / extra
  # elements do not break discovery, and require version 21 so a 17 toolchain is not picked by mistake.
  # The final -x native-image check below is the real gate; the vendor match only picks the candidate.
  if [[ -f "$HOME/.m2/toolchains.xml" ]]; then
    local home
    home="$(awk '
      /<toolchain>/                              { isni=0; v21=0; jh="" }
      tolower($0) ~ /graalvm|liberica|bellsoft/  { isni=1 }
      /<version>21</                             { v21=1 }
      /<jdkHome>/     { line=$0; sub(/.*<jdkHome>/, "", line); sub(/<\/jdkHome>.*/, "", line); jh=line }
      /<\/toolchain>/ { if (isni && v21 && jh != "") { print jh; exit } }
    ' "$HOME/.m2/toolchains.xml")"
    if [[ -n "$home" && -x "$home/bin/native-image" ]]; then echo "$home"; return 0; fi
  fi
  return 1
}

if [[ "$DO_BUILD" == 1 ]]; then
  bold "Building native image (mvn -Pnative)…"
  if ! GVM="$(discover_graalvm)"; then
    red "No JDK-21 native-image toolchain found. Set GRAALVM_HOME, or install Liberica NIK (release) or Oracle GraalVM (local dev) for JDK 21."
    exit 1
  fi
  echo "  native-image toolchain: $GVM"
  ( cd "$REPO_ROOT" && JAVA_HOME="$GVM" mvn -q -Pnative -pl cli -am -DskipTests package )
fi

if [[ ! -x "$BINARY" ]]; then
  red "native binary not found or not executable: $BINARY"
  red "build it first:  cli/native-smoke.sh --build"
  exit 1
fi
bold "Native smoke — binary: $BINARY"

CORPUS="$REPO_ROOT/core/core-dsl/src/test/resources/corpus"

# --- timing helper: median of N runs of a command, in milliseconds -----------------------------
median_ms() {
  local n=5 i t samples=()
  for ((i=0; i<n; i++)); do
    local start end
    start=$(python3 -c 'import time; print(int(time.time()*1000))')
    "$@" >/dev/null 2>&1 || true
    end=$(python3 -c 'import time; print(int(time.time()*1000))')
    samples+=( $((end-start)) )
  done
  printf '%s\n' "${samples[@]}" | sort -n | sed -n '3p'
}

# --- 1. startup budget --------------------------------------------------------------------------
bold "[1] startup time (<${STARTUP_BUDGET_MS}ms)"
VERSION_MS=$(median_ms "$BINARY" --version)
if (( VERSION_MS < STARTUP_BUDGET_MS )); then
  ok "tapstate --version  median ${VERSION_MS}ms"
else
  bad "tapstate --version  median ${VERSION_MS}ms  (budget ${STARTUP_BUDGET_MS}ms)"
fi
# a resource-touching command (validate loads catalog + messages) must also stay in budget
VALID_DIR="$CORPUS/valid/s01-mirror-rename-ddl"
VALIDATE_MS=$(median_ms "$BINARY" validate "$VALID_DIR")
if (( VALIDATE_MS < STARTUP_BUDGET_MS )); then
  ok "tapstate validate <dir>  median ${VALIDATE_MS}ms (catalog + schema + messages loaded)"
else
  bad "tapstate validate <dir>  median ${VALIDATE_MS}ms  (budget ${STARTUP_BUDGET_MS}ms)"
fi

# --- 2. validate over the full corpus -----------------------------------------------------------
bold "[2] validate — full corpus (valid → exit 0, invalid → exit 1)"
valid_fail=0
for dir in "$CORPUS"/valid/*/; do
  if "$BINARY" validate "$dir" >/dev/null 2>&1; then :; else
    bad "valid corpus rejected: $(basename "$dir")"; valid_fail=$((valid_fail+1))
  fi
done
(( valid_fail == 0 )) && ok "all $(ls -d "$CORPUS"/valid/*/ | wc -l | tr -d ' ') valid scenarios accepted (exit 0)"

invalid_fail=0
for dir in "$CORPUS"/invalid/*/; do
  base=$(basename "$dir")
  # capture output, not just the exit code: an uncaught native fault (a missing bundled resource, a
  # reflection gap) also exits 1, so exit-1-alone cannot tell a coded rejection from a crash — exactly
  # the regression class this suite exists to catch. Require the coded `invalid:` line and no stack frame.
  set +e; out=$("$BINARY" validate "$dir" 2>&1); code=$?; set -e
  if (( code != 1 )); then
    bad "invalid corpus not rejected with exit 1: $base (got exit $code)"; invalid_fail=$((invalid_fail+1)); continue
  fi
  if echo "$out" | grep -qE '\.java:[0-9]+\)'; then
    bad "invalid corpus exit 1 but emitted a Java stack trace — uncaught native fault, not a coded rejection: $base"
    invalid_fail=$((invalid_fail+1)); continue
  fi
  if ! echo "$out" | grep -q 'invalid:'; then
    bad "invalid corpus exit 1 but no coded diagnostic rendered — possible native crash: $base"
    invalid_fail=$((invalid_fail+1)); continue
  fi
done
(( invalid_fail == 0 )) && ok "all $(ls -d "$CORPUS"/invalid/*/ | wc -l | tr -d ' ') invalid scenarios rejected (exit 1, coded diagnostic, no stack trace)"

# --- 3. new — non-interactive scaffolding (catalog-driven) --------------------------------------
# --dry-run previews the canonical artifact on stdout (no file written); it proves the connector
# catalog resource is reachable in the image (mysql resolved) and the canonical writer runs. The
# -o json result-envelope path is exercised separately by [4] explain (shared JsonOut writer).
bold "[3] new — non-interactive (catalog read + canonical render)"
NEW_OUT=$("$BINARY" new --kind source --connector mysql --id smoke_src -m cdc --dry-run 2>&1) || true
if echo "$NEW_OUT" | grep -q 'id: smoke_src' \
   && echo "$NEW_OUT" | grep -q 'connector: mysql' \
   && echo "$NEW_OUT" | grep -q 'mode: cdc'; then
  ok "new --kind source --connector mysql -m cdc --dry-run rendered the canonical artifact"
else
  bad "new non-interactive did not render the expected artifact; output: $NEW_OUT"
fi

# --- 4. explain — schema navigation (schema resource) ------------------------------------------
bold "[4] explain — field documentation (schema resource)"
EXPLAIN_OUT=$("$BINARY" explain source -o json 2>&1) || true
if echo "$EXPLAIN_OUT" | grep -q '"description"'; then
  ok "explain source -o json returned a documented node"
else
  bad "explain did not return a documented node; output: $EXPLAIN_OUT"
fi

# --- 5. TUI command execution under a real pty -------------------------------------------------
bold "[5] TUI — command execution under a pty (JLine + TamboUI)"
# The default launch is the only interactive entry point. The command and Ctrl-D are sent after the
# workbench has drawn its first frame; the command result must land in the workbench before cleanup.
printf -v tui_in 'validate %s\n\004' "$VALID_DIR"
TAPSTATE_PTY_TUI=1 pty_session "$tui_in"
TUI_COMMAND_CLEAN=$(printf '%s' "$PTY_OUT" | strip_ansi)
if (( PTY_RC == 0 )) \
   && grep -q "TAPSTATE" <<< "$TUI_COMMAND_CLEAN" \
   && grep -qE '(^|[^[:alpha:]])valid:' <<< "$TUI_COMMAND_CLEAN"; then
  ok "TUI ran validate and restored its alternate screen and cursor (rc 0)"
else
  bad "TUI command session failed (rc=$PTY_RC) or missing expected markers; output:"; echo "$PTY_OUT"
fi

# --- 6. new wizard under a real pty (JLinePrompter interactive flow) ----------------------------
# Drive the interactive `new` source wizard (the JLinePrompter path, distinct from [3]'s flag path):
# connector, read mode, blank tables, an id, then blank lines so every connector config field is
# skipped (collect() asks each once; a blank reply omits it — no re-prompt, so surplus blanks are
# harmless and discarded when the wizard finishes). --dry-run previews the artifact, writing no file.
bold "[6] new — interactive wizard under a pty (JLinePrompter)"
# printf -v preserves the trailing newlines (command substitution would strip them); the 50 blank
# lines submit a skip for each connector config field so the wizard runs to completion.
printf -v wizard_in 'mysql\ncdc\n\nsmoke_iface\n'
printf -v wizard_pad '\n%.0s' {1..50}
pty_session "${wizard_in}${wizard_pad}" new --dry-run
WIZARD_CLEAN=$(printf '%s' "$PTY_OUT" | strip_ansi)
if (( PTY_RC == 0 )) \
   && printf '%s' "$WIZARD_CLEAN" | grep -q 'connector: mysql' \
   && printf '%s' "$WIZARD_CLEAN" | grep -q 'id: smoke_iface' \
   && printf '%s' "$WIZARD_CLEAN" | grep -q 'mode: cdc'; then
  ok "interactive new wizard rendered the canonical artifact + clean exit (rc 0) over a pty"
else
  bad "new wizard pty session failed (rc=$PTY_RC) or missing expected artifact; output:"; echo "$PTY_OUT"
fi

# --- 7. structured YAML output (the YAML writer + its resources reachable in the image) ----------
# [4] exercises -o json; the YAML writer is a separate code path and the acceptance bar promises
# both json|yaml. Run explain -o yaml (schema-backed node envelope) and validate -o yaml (diagnostics
# envelope) through the native binary; require real block-mapping output and no uncaught native fault
# (a missing resource / reflection gap would surface as a stack frame, not a clean mapping).
bold "[7] -o yaml — structured YAML output (YAML writer reachable in the image)"
YAML_EXPLAIN=$("$BINARY" explain source -o yaml 2>&1) || true
YAML_VALIDATE=$("$BINARY" validate "$VALID_DIR" -o yaml 2>&1) || true
if echo "$YAML_EXPLAIN" | grep -qE '^path: source$' \
   && echo "$YAML_EXPLAIN" | grep -qE '^description:' \
   && echo "$YAML_VALIDATE" | grep -qE '^status: valid$' \
   && ! printf '%s\n%s' "$YAML_EXPLAIN" "$YAML_VALIDATE" | grep -qE '\.java:[0-9]+\)'; then
  ok "explain + validate -o yaml rendered block mappings (no native fault)"
else
  bad "-o yaml did not render the expected block mapping; explain: $YAML_EXPLAIN | validate: $YAML_VALIDATE"
fi

# --- 8. Tab completion under a pty (the TUI command bar completer) ------------------------------
# Feed `va` + TAB so the TUI command bar resolves it to `validate`, then cancel the draft and exit.
# The JVM suite covers candidate selection; this black-box check proves the native command bar accepts
# the key sequence without falling into the removed line-oriented session.
bold "[8] Tab completion — TUI command bar under a pty"
TAPSTATE_PTY_TUI=1 pty_session $'va\t\033\004'
COMP_CLEAN=$(printf '%s' "$PTY_OUT" | strip_ansi)
if (( PTY_RC == 0 )) \
   && grep -q "TAPSTATE" <<< "$COMP_CLEAN" \
   && grep -q "validate" <<< "$COMP_CLEAN" \
   && ! grep -q 'Unmatched argument' <<< "$COMP_CLEAN"; then
  ok "TUI Tab completed 'va'→'validate' and exited cleanly over a pty"
else
  bad "TUI Tab completion failed (rc=$PTY_RC) or did not complete 'va'→'validate'; output:"; echo "$PTY_OUT"
fi

# Typing must also open the inline suggestion surface without waiting for Tab. Escape closes the
# candidates, while the JVM reducer test covers Enter selecting the highlighted command.
TAPSTATE_PTY_TUI=1 pty_session $'va\033\004'
AUTO_CLEAN=$(printf '%s' "$PTY_OUT" | strip_ansi)
if (( PTY_RC == 0 )) \
   && grep -q "validate" <<< "$AUTO_CLEAN" \
   && grep -q "Enter sel" <<< "$AUTO_CLEAN" \
   && ! grep -q 'Unmatched argument' <<< "$AUTO_CLEAN"; then
  ok "TUI typing rendered inline suggestions with an Enter-select hint"
else
  bad "TUI inline suggestion rendering failed (rc=$PTY_RC); output:"; echo "$PTY_OUT"
fi

# macOS/JLine commonly reports the Return key as CR. Select an incomplete `:ctx` candidate with CR,
# enter its choice prompt, move with Down, choose Quit with CR, and then leave the TUI. This catches
# the event-loop branch that would otherwise submit `:ct` and the prompt path that could leak arrows.
TAPSTATE_PTY_TUI=1 pty_session $':ct\r\r\033[B\r\004'
CR_CTX_CLEAN=$(printf '%s' "$PTY_OUT" | strip_ansi)
if (( PTY_RC == 0 )) \
   && grep -q "Contex" <<< "$CR_CTX_CLEAN" \
   && grep -q "action" <<< "$CR_CTX_CLEAN" \
   && grep -q "Quit" <<< "$CR_CTX_CLEAN" \
   && ! grep -q 'Unmatched argument' <<< "$CR_CTX_CLEAN"; then
  ok "TUI CR completion and :ctx arrow selection stayed inside the prompt"
else
  bad "TUI CR completion or :ctx arrow selection failed (rc=$PTY_RC); output:"; echo "$PTY_OUT"
fi

# --- 9. online TUI launch under a pty (HttpClient + issuer discovery reachable in the image) --------
# Sections 1-8 never open a socket. Stand up a throwaway loopback
# stub (healthz + issuer discovery), launch the TUI with a machine token, and confirm the dashboard sees
# the online session. The one-shot register path remains covered by [10]; this case covers the TUI launch
# and authenticated session setup without asking the TUI to write without a selected context.
bold "[9] online TUI launch — connect + machine token under a pty"
STUB_DIR="$(mktemp -d)"
trap 'if [[ -f "$STUB_DIR/pid" ]]; then kill "$(cat "$STUB_DIR/pid")" 2>/dev/null || true; fi; rm -rf "$STUB_DIR"' EXIT
printf 'PK\003\004smoke-jar' > "$STUB_DIR/smoke.jar"   # any bytes; the stub does not inspect the jar
cat > "$STUB_DIR/stub.py" <<'PY'
import http.server, json, os, socketserver, sys
EVENTS = sys.argv[3]
class H(http.server.BaseHTTPRequestHandler):
    def _event(self):
        authorization = self.headers.get("Authorization")
        authorization_kind = ("none" if authorization is None else
                              "bearer" if authorization.startswith("Bearer ") else "other")
        with open(EVENTS, "a") as f:
            f.write(f"{self.command} {self.path} auth={authorization_kind}\n")
    def _send(self, code, obj=None):
        self.send_response(code)
        if obj is not None:
            body = json.dumps(obj).encode()
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.end_headers()
    def do_GET(self):
        self._event()
        if self.path == "/healthz":
            self._send(200)
        elif self.path == "/.well-known/tapstate":
            self._send(200, {
                "issuer": "urn:tapstate:cluster:native-smoke",
                "clusterId": "native-smoke",
                "apiVersion": "tapstate/v1",
                "authModes": ["password", "machine_token"],
            })
        else:
            self._send(404)
    def do_POST(self):
        self._event()
        self.rfile.read(int(self.headers.get("Content-Length", "0")))
        if self.path == "/auth/login":
            self._send(200, {"token": "smoke-token"})
        elif self.path == "/api/connectors:register":
            self._send(200, {"connectorId": "smoke", "contentHash": "h1", "newlyRegistered": True})
        else:
            self._send(404)
    def log_message(self, *a):
        pass
srv = socketserver.TCPServer(("127.0.0.1", 0), H)
with open(sys.argv[1], "w") as f:                       # publish the ephemeral port before serving
    f.write(str(srv.server_address[1]))
if os.fork() > 0:                                       # parent returns so bash proceeds; child serves
    os._exit(0)
os.setsid()
with open(sys.argv[2], "w") as f:                       # publish the child pid so bash can reap it
    f.write(str(os.getpid()))
srv.serve_forever()
PY
python3 "$STUB_DIR/stub.py" "$STUB_DIR/port" "$STUB_DIR/pid" "$STUB_DIR/events"
STUB_PORT="$(cat "$STUB_DIR/port" 2>/dev/null || true)"
printf -v online_in '\004'
TAPSTATE_PTY_TUI=1 pty_session "$online_in" \
  -c "127.0.0.1:$STUB_PORT" --token smoke-token
ONLINE_CLEAN=$(printf '%s' "$PTY_OUT" | strip_ansi)
if (( PTY_RC == 0 )) \
   && [[ -n "$STUB_PORT" ]] \
   && printf '%s' "$ONLINE_CLEAN" | grep -q "online" \
   && ! printf '%s' "$ONLINE_CLEAN" | grep -qE '\.java:[0-9]+\)'; then
  ok "TUI launch connected with a machine token and rendered the online session (no AOT fault)"
else
  bad "TUI online launch failed (rc=$PTY_RC, port=${STUB_PORT:-none}); output:"; echo "$PTY_OUT"
fi

bold "[10] one-line launch — -c / -u reach the server without a session"
# The scripting form: connect, sign in and run one command from the arguments alone. Checked on the
# native binary because this path parses its own options before the command table is built, so an AOT
# fault here would not show up in any of the interactive cases above.
ONELINE_OUT=$(TAPSTATE_PASSWORD=smoke-pw "$BINARY" -c "127.0.0.1:$STUB_PORT" -u admin \
                register "$STUB_DIR/smoke.jar" 2>"$STUB_DIR/oneline.err") && ONELINE_RC=0 || ONELINE_RC=$?
ONELINE_CLEAN=$(printf '%s' "$ONELINE_OUT" | strip_ansi)
if (( ONELINE_RC == 0 )) \
   && printf '%s' "$ONELINE_CLEAN" | grep -qE 'registered[[:space:]]+smoke' \
   && ! printf '%s' "$ONELINE_CLEAN" | grep -q "connected to" \
   && ! printf '%s' "$ONELINE_CLEAN" | grep -q "logged in as" \
   && ! printf '%s' "$ONELINE_CLEAN" | grep -qE '\.java:[0-9]+\)'; then
  ok "one-line launch registered end to end, exit 0, and kept its stdout to the command's own output"
else
  bad "one-line launch failed (rc=$ONELINE_RC); stdout:"; echo "$ONELINE_OUT"
  echo "stderr:"; cat "$STUB_DIR/oneline.err" 2>/dev/null || true
fi

# a command that fails must fail the process: the whole point of running one from a script
TAPSTATE_PASSWORD=smoke-pw "$BINARY" -c "127.0.0.1:$STUB_PORT" -u admin start >/dev/null 2>&1 && BADRC=0 || BADRC=$?
if (( BADRC != 0 )); then
  ok "a one-line command that failed exited non-zero (rc=$BADRC)"
else
  bad "a one-line command that failed still exited 0"
fi

# --- 11. kafka.modes — the overlay-declared set survived into the image --------------------------
# kafka has no derivable mode signal: its modes come from the repo overlay and are baked into
# catalog/kafka.json when the snapshot is assembled. The image keeps that resource only because
# -H:IncludeResources matches catalog/.*\.json, and losing it does NOT fail loudly — an entry with
# no modes counts as "no trustworthy offline signal", so every mode is accepted and the CLI just
# quietly stops rejecting. [2] reddens on that too, since the invalid corpus carries a kafka + cdc
# scenario, but only incidentally: nothing there says that scenario stands in for the overlay, so
# re-pointing it at another connector would take the evidence away without a word. Pin the set by
# name here instead. The JVM side of the same claim is pinned by the same value in the checked-in
# catalog and the corpus gate over it, so this deliberately does not A/B a second process: two
# independent pins cannot drift together, one live comparison of two drifting shapes can.
# `validate` is the probe rather than `new` because `new` refuses kafka one guard earlier (it is not
# in this release's installed set) and would never reach the mode check at all.
bold "[11] kafka.modes — the overlay-declared set survived into the image"
KAFKA_DIR="$REPO_ROOT/cli/target/native-smoke-kafka"
rm -rf "$KAFKA_DIR" && mkdir -p "$KAFKA_DIR"
printf 'version: tapstate/v1\nkind: source\nid: smoke_kafka\nconnector: kafka\nmode: cdc\n' > "$KAFKA_DIR/kafka.tap.yml"
# the rejection names the connector's modes; empty means the binary did not reject at all
KAFKA_MODES="$({ "$BINARY" validate "$KAFKA_DIR" 2>&1 || true; } | strip_ansi | sed -n 's/.*supported modes: \(.*\)\.$/\1/p' | head -1)"
if [[ "$KAFKA_MODES" == "stream" ]]; then
  ok "kafka + cdc rejected, naming exactly the overlay-declared modes: [stream]"
elif [[ -z "$KAFKA_MODES" ]]; then
  bad "the image accepted kafka + cdc — kafka.modes is empty in the binary, so the catalog resource or its overlay-declared modes did not survive the build"
else
  bad "kafka.modes in the image is [$KAFKA_MODES], not the overlay-declared [stream]"
fi
rm -rf "$KAFKA_DIR"
# --- 12. one-line machine token ---------------------------------------------------------------
bold "[12] one-line machine token — discovery precedes Bearer and password login stays unused"
MACHINE_TOKEN="native-smoke-machine-token"
MACHINE_EVENT_COUNT=$(wc -l < "$STUB_DIR/events")
MACHINE_OUT=$("$BINARY" --token "$MACHINE_TOKEN" -c "127.0.0.1:$STUB_PORT" \
                register "$STUB_DIR/smoke.jar" 2>"$STUB_DIR/machine.err") && MACHINE_RC=0 || MACHINE_RC=$?
MACHINE_CLEAN=$(printf '%s' "$MACHINE_OUT" | strip_ansi)
MACHINE_EVENTS=$(tail -n +$((MACHINE_EVENT_COUNT + 1)) "$STUB_DIR/events" 2>/dev/null || true)
DISCOVERY_LINE=$(printf '%s\n' "$MACHINE_EVENTS" | grep -n '^GET /.well-known/tapstate auth=none$' | head -1 | cut -d: -f1 || true)
API_LINE=$(printf '%s\n' "$MACHINE_EVENTS" | grep -n '^POST /api/connectors:register auth=bearer$' | head -1 | cut -d: -f1 || true)
if (( MACHINE_RC == 0 )) \
   && printf '%s' "$MACHINE_CLEAN" | grep -qE 'registered[[:space:]]+smoke' \
   && ! printf '%s' "$MACHINE_CLEAN" | grep -q "$MACHINE_TOKEN" \
   && ! printf '%s\n' "$MACHINE_EVENTS" | grep -q '^POST /auth/login ' \
   && [[ -n "$DISCOVERY_LINE" && -n "$API_LINE" ]] \
   && (( DISCOVERY_LINE < API_LINE )); then
  ok "one-line machine-token launch discovered the issuer before Bearer use and skipped password login"
else
  bad "one-line --token did not preserve its discovery and process-only credential contract (rc=$MACHINE_RC)"
fi

# --- 13. TUI lifecycle -------------------------------------------------------------------------
# A native image has a separate reachability surface for JLine terminal resources and shutdown
# cleanup. Exercise the default TUI entry point on a real pseudo-terminal with an explicit workspace,
# then require the alternate screen to be restored after Ctrl-D exits.
bold "[13] TUI dashboard — native workspace entry and cleanup"
TUI_WORKSPACE="$STUB_DIR/tui-workspace"
mkdir -p "$TUI_WORKSPACE/source"
cat > "$TUI_WORKSPACE/source/src_orders.tap.yml" <<'EOF'
version: tapstate/v1
kind: source
id: src_orders
connector: mysql
config: {}
mode: cdc
tables: [orders]
EOF
TAPSTATE_PTY_TUI=1 pty_session $'\004' -w "$TUI_WORKSPACE"
TUI_CLEAN=$(printf '%s' "$PTY_OUT" | strip_ansi)
if (( PTY_RC == 0 )) \
   && grep -Fq $'\033[?1049h' <<< "$PTY_OUT" \
   && grep -Fq $'\033[?1049l' <<< "$PTY_OUT" \
   && grep -Fq "${TUI_WORKSPACE##*/}" <<< "$TUI_CLEAN" \
   && { grep -Fq $'\033[?25h' <<< "$PTY_OUT" \
        || grep -Fq $'\033[?12;25h' <<< "$PTY_OUT"; }; then
  ok "native tui accepted its workspace and restored its alternate screen and cursor"
else
  bad "native tui workspace PTY failed (rc=$PTY_RC); output:"; echo "$PTY_OUT"
fi

# The first frame is the visual contract for the modern workbench: the composer is a quiet rounded
# input surface, its usage hint is below it, and transient scroll state never appears in that surface.
if (( PTY_RC == 0 )) \
   && grep -Fq '╭' <<< "$TUI_CLEAN" \
   && grep -Fq '╰' <<< "$TUI_CLEAN" \
   && grep -Fq 'Enter run · Type for' <<< "$TUI_CLEAN" \
   && ! grep -Fq '[COMMAND]' <<< "$TUI_CLEAN" \
   && ! grep -qE 'scroll [0-9]+' <<< "$TUI_CLEAN"; then
  ok "native tui rendered the rounded composer and kept hints/scroll state out of the input"
else
  bad "native tui visual contract failed; output:"; echo "$TUI_CLEAN"
fi

# Picocli help is deliberately long enough to exercise the workspace viewport. The scrollbar belongs
# to that result area, not to the command composer, and must remain a glyph rather than a status label.
TAPSTATE_PTY_TUI=1 pty_session $'help\n\004' -w "$TUI_WORKSPACE"
TUI_SCROLL_CLEAN=$(printf '%s' "$PTY_OUT" | strip_ansi)
if (( PTY_RC == 0 )) \
   && grep -Fq '┃' <<< "$TUI_SCROLL_CLEAN" \
   && ! grep -qE 'scroll [0-9]+' <<< "$TUI_SCROLL_CLEAN"; then
  ok "native tui rendered a workspace scrollbar for long command output"
else
  bad "native tui scrollbar contract failed; output:"; echo "$TUI_SCROLL_CLEAN"
fi

# --- summary ------------------------------------------------------------------------------------
echo
bold "native smoke: ${PASS} passed, ${FAIL} failed"
(( FAIL == 0 ))
