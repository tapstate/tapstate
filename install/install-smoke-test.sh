#!/usr/bin/env bash
# The installer smoke's own verdict when its install-event sink never starts. The sink is a fixture, a
# local stand-in for the event endpoint that the install-event cases read to see what an install sent.
# When it never binds a port those cases have nothing to read, and the only true report is that the
# sink failed: one failure, naming it. Reading the absent port file anyway hands the installer an
# endpoint with no port, every case that reads the sink then fails as though the installer had sent the
# wrong thing, and a fixture that never started is reported as several defects in the installer.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SCRATCH="$(mktemp -d)"
trap 'rm -rf "$SCRATCH"' EXIT

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
  printf 'PASS  a sink that never bound a port is reported as one failure, and that failure names the sink\n'
else
  cat "$SCRATCH/out" >&2
  printf 'FAIL  a sink that never bound a port was reported as %s failure(s) (smoke exit %s), not as one naming the sink\n' \
    "$fails" "$status" >&2
  exit 1
fi
