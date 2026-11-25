#!/usr/bin/env bash
# Supervise only the command owned by this worker; never cancel another Actions run.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
exec python3 "$here/_ci-pr-work.py" "$@"
