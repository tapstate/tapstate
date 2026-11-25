#!/usr/bin/env bash
# Record actual build inputs or archive original CI evidence without changing the test workflow.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
exec python3 "$here/_ci-receipt.py" "$@"
