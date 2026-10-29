#!/usr/bin/env bash
# Ordinary CI variant commits and bounded owned-source recovery.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
exec python3 "$here/_ci-variants.py" "$@"
