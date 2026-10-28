#!/usr/bin/env bash
# Retain and replay native evidence without granting complete PR or performance proof.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
exec python3 "$here/_ci-instance-evidence.py" "$@"
