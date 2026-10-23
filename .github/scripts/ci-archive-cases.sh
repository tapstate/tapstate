#!/usr/bin/env bash
# Parse bound checkout data and replay original case members without running them.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
exec python3 "$here/_ci-archive-cases.py" "$@"
