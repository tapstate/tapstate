#!/usr/bin/env bash
# Read original workflow and event source facts without evaluating archived code.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
exec python3 "$here/_ci-trigger-basis.py" "$@"
