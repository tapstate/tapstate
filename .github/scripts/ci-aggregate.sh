#!/usr/bin/env bash
# Package and admit the exact source-selected cohort before restoring coverage inputs.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
exec python3 "$here/_ci-shards.py" "$@"
