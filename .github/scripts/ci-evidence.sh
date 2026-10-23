#!/usr/bin/env bash
# Compare independent source cases with raw test, coverage and gate observations.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
exec python3 "$here/_ci-evidence.py" "$@"
