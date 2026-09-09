#!/usr/bin/env bash
# Plan and admit real-connector witnesses using the shared shard engine.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
exec python3 "$here/_connector-witnesses.py" "$@"
