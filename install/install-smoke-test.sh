#!/usr/bin/env bash
# The installer smoke's own verdict when its install-event sink never starts. The sink is a fixture, a
# local stand-in for the event endpoint that the install-event cases read to see what an install sent.
# When it never binds a port those cases have nothing to read, and the only true report is that the
# sink failed: one failure, naming it. Reading the absent port file anyway hands the installer an
# endpoint with no port, every case that reads the sink then fails as though the installer had sent the
# wrong thing, and a fixture that never started is reported as several defects in the installer.
#
# A sink that never binds either exits, as one that failed to start does, or stays alive, as one stalled
# before it listens does. The smoke's wait for its port catches the first with its liveness check and
# never reaches its own end, so only the second shows what the smoke reports when the wait runs out.
# Both are driven below.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SCRATCH="$(mktemp -d)"
# If the smoke fails to stop a sink that stays alive, stop it here, so a failing run leaves no process
# behind.
finish() {
  if [ -s "$SCRATCH/sink-pid" ]; then kill "$(cat "$SCRATCH/sink-pid")" 2>/dev/null || true; fi
  rm -rf "$SCRATCH"
}
trap finish EXIT

# The sink is the one program the smoke hands python3 on stdin. Only that run is refused; every other
# python3 run stays real, or the cases around the sink fail too and this proves nothing about it.
REAL_PYTHON3="$(command -v python3 || true)"
[ -n "$REAL_PYTHON3" ] || { printf 'FAIL  python3 is needed: the smoke runs its sink with it\n' >&2; exit 1; }
mkdir -p "$SCRATCH/bin"
cat > "$SCRATCH/bin/python3" <<EOF
#!/bin/sh
# Exit before binding anything, as a sink that failed to start does, and leave a mark that it did.
for arg in "\$@"; do
  if [ "\$arg" = - ]; then : > "$SCRATCH/sink-refused"; exit 1; fi
done
exec "$REAL_PYTHON3" "\$@"
EOF
chmod +x "$SCRATCH/bin/python3"

status=0
PATH="$SCRATCH/bin:$PATH" bash "$HERE/install-smoke.sh" > "$SCRATCH/out" 2>&1 || status=$?

if [ ! -e "$SCRATCH/sink-refused" ]; then
  cat "$SCRATCH/out" >&2
  printf 'FAIL  the smoke never started its sink from a python3 program on stdin, so nothing here was tested\n' >&2
  exit 1
fi
fails="$(grep -c '^  FAIL' "$SCRATCH/out" || true)"
if [ "$status" -ne 0 ] && [ "$fails" = 1 ] && grep -qi '^  FAIL.*sink' "$SCRATCH/out"; then
  printf 'PASS  a sink that exits without binding a port is reported as one failure, and that failure names the sink\n'
else
  cat "$SCRATCH/out" >&2
  printf 'FAIL  a sink that exits without binding a port was reported as %s failure(s) (smoke exit %s), not as one naming the sink\n' \
    "$fails" "$status" >&2
  exit 1
fi

# The same sink stalled instead: it never binds and never exits, so the liveness check inside the wait
# passes on every round and only the failure the wait returns when it runs out can report it.
mkdir -p "$SCRATCH/stall"
cat > "$SCRATCH/stall/python3" <<EOF
#!/bin/sh
# Stay alive without binding anything, under the pid the smoke holds for its sink, and record that pid.
for arg in "\$@"; do
  if [ "\$arg" = - ]; then echo "\$\$" > "$SCRATCH/sink-pid"; exec sleep 300; fi
done
exec "$REAL_PYTHON3" "\$@"
EOF
chmod +x "$SCRATCH/stall/python3"

status=0
PATH="$SCRATCH/stall:$PATH" bash "$HERE/install-smoke.sh" > "$SCRATCH/out" 2>&1 || status=$?

if [ ! -s "$SCRATCH/sink-pid" ]; then
  cat "$SCRATCH/out" >&2
  printf 'FAIL  the smoke never started its sink from a python3 program on stdin, so nothing here was tested\n' >&2
  exit 1
fi
fails="$(grep -c '^  FAIL' "$SCRATCH/out" || true)"
if [ "$status" -ne 0 ] && [ "$fails" = 1 ] && grep -qi '^  FAIL.*sink' "$SCRATCH/out"; then
  printf 'PASS  a sink that stays alive without binding a port is reported as one failure, and that failure names the sink\n'
else
  cat "$SCRATCH/out" >&2
  printf 'FAIL  a sink that stays alive without binding a port was reported as %s failure(s) (smoke exit %s), not as one naming the sink\n' \
    "$fails" "$status" >&2
  exit 1
fi

# A sink the smoke gave up on must not outlive the smoke. Once it is known to be gone its pid is
# forgotten, so the exit trap cannot signal whatever process is given that pid later.
if kill -0 "$(cat "$SCRATCH/sink-pid")" 2>/dev/null; then
  printf 'FAIL  the smoke exited and left its sink that never bound a port still running\n' >&2
  exit 1
fi
rm -f "$SCRATCH/sink-pid"
printf 'PASS  the smoke stops a sink that never bound a port before it exits\n'

# The sink must start without resolving a name for the address it bound. On some macOS runners that
# lookup stalls for about 35 seconds, past any wait for the port, and on Linux it returns at once, so
# only a lookup that fails outright makes a server that performs it fail here as well.
mkdir -p "$SCRATCH/site"
cat > "$SCRATCH/site/sitecustomize.py" <<EOF
import socket
open("$SCRATCH/lookup-armed", "w").close()
def getfqdn(name=""):
    raise OSError("name lookup refused by install-smoke-test.sh")
socket.getfqdn = getfqdn
EOF
status=0
PYTHONPATH="$SCRATCH/site${PYTHONPATH:+:$PYTHONPATH}" bash "$HERE/install-smoke.sh" > "$SCRATCH/out" 2>&1 || status=$?
if [ ! -e "$SCRATCH/lookup-armed" ]; then
  cat "$SCRATCH/out" >&2
  printf 'FAIL  python3 never loaded the refusing name lookup, so nothing here was tested\n' >&2
  exit 1
fi
fails="$(grep -c '^  FAIL' "$SCRATCH/out" || true)"
if [ "$status" -eq 0 ] && [ "$fails" = 0 ]; then
  printf 'PASS  the sink starts without a name lookup, so a stalled lookup cannot keep it from binding\n'
else
  cat "$SCRATCH/out" >&2
  printf 'FAIL  with name lookups refused the smoke reported %s failure(s) (smoke exit %s)\n' "$fails" "$status" >&2
  exit 1
fi
