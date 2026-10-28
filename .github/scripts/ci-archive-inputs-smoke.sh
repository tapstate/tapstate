#!/usr/bin/env bash
# Exercise real Git and artifact readback without CI/API/JVM/Docker operations.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
PYTHONDONTWRITEBYTECODE=1 python3 "$here/_ci-archive-inputs-smoke.py"
PYTHONDONTWRITEBYTECODE=1 exec python3 "$here/_ci-retained-readback-smoke.py"
