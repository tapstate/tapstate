#!/usr/bin/env bash
# Read-only PR group input reconciliation and current-head inspection.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
exec python3 "$here/_ci-pr-groups.py" "$@"
