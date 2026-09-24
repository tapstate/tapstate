#!/usr/bin/env bash
# Derive source-selected test shards or run one after a separate reactor install.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
exec python3 "$here/_ci-shards.py" "$@"
