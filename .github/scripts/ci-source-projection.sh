#!/usr/bin/env bash
# Read back archived source against an owned reviewed variant recipe.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
exec python3 "$here/_ci-source-projection.py" "$@"
