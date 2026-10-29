#!/usr/bin/env bash
# Verify original source facts while keeping workflow applicability unverified.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
PYTHONDONTWRITEBYTECODE=1 exec python3 "$here/_ci-trigger-basis-smoke.py"
