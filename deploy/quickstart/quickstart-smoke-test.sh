#!/usr/bin/env bash
# The quickstart smoke's own verdict when its install-event sink never binds a port. The sink is a
# fixture, a local stand-in for the event endpoint that the install-event cases read to see what a
# quickstart run sent. When it never binds a port those cases have nothing to read, and the only true
# report is that the sink failed: one failure, naming it. Reading the absent port file anyway hands the
# quickstart an endpoint with no port, every case that reads the sink then fails as though the
# quickstart had sent the wrong thing, and a fixture that never started is reported as several defects.
#
# The sink here stays alive and never binds, as one stalled before it listens does. A sink that exits at
# once is caught by the liveness check inside the wait for its port and never reaches the end of that
# wait, so only one that stays alive shows what the smoke reports when the wait runs out.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SCRATCH="$(mktemp -d)"
# If the smoke fails to stop the sink, stop it here, so a failing run leaves no process behind.
finish() {
  if [ -s "$SCRATCH/sink-pid" ]; then kill "$(cat "$SCRATCH/sink-pid")" 2>/dev/null || true; fi
  rm -rf "$SCRATCH"
}
trap finish EXIT

# The sink is the one program the smoke hands python3 on stdin. Only that run stalls; every other
# python3 run stays real, or the cases around the sink fail too and this proves nothing about it.
REAL_PYTHON3="$(command -v python3 || true)"
[ -n "$REAL_PYTHON3" ] || { printf 'FAIL  python3 is needed: the smoke runs its sink with it\n' >&2; exit 1; }
mkdir -p "$SCRATCH/bin"
cat > "$SCRATCH/bin/python3" <<EOF
#!/bin/sh
# Stay alive without binding anything, under the pid the smoke holds for its sink, and record that pid.
for arg in "\$@"; do
  if [ "\$arg" = - ]; then echo "\$\$" > "$SCRATCH/sink-pid"; exec sleep 300; fi
done
exec "$REAL_PYTHON3" "\$@"
EOF
chmod +x "$SCRATCH/bin/python3"

status=0
PATH="$SCRATCH/bin:$PATH" bash "$HERE/quickstart-smoke.sh" > "$SCRATCH/out" 2>&1 || status=$?

if [ ! -s "$SCRATCH/sink-pid" ]; then
  cat "$SCRATCH/out" >&2
  printf 'FAIL  the smoke never started its sink from a python3 program on stdin, so nothing here was tested\n' >&2
  exit 1
fi
fails="$(grep -c '^  FAIL' "$SCRATCH/out" || true)"
if [ "$status" -ne 0 ] && [ "$fails" = 1 ] && grep -qi '^  FAIL.*sink' "$SCRATCH/out"; then
  printf 'PASS  a sink that never bound a port is reported as one failure, and that failure names the sink\n'
else
  cat "$SCRATCH/out" >&2
  printf 'FAIL  a sink that never bound a port was reported as %s failure(s) (smoke exit %s), not as one naming the sink\n' \
    "$fails" "$status" >&2
  exit 1
fi

# A sink the smoke gave up on must not outlive the smoke.
if kill -0 "$(cat "$SCRATCH/sink-pid")" 2>/dev/null; then
  printf 'FAIL  the smoke exited and left its sink that never bound a port still running\n' >&2
  exit 1
fi
printf 'PASS  the smoke stops a sink that never bound a port before it exits\n'
