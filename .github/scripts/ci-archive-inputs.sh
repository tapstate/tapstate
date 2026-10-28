#!/usr/bin/env bash
# Read original CI input/result members against archived checkout and source recipe.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
exec python3 "$here/_ci-archive-inputs.py" "$@"
