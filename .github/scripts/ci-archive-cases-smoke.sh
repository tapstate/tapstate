#!/usr/bin/env bash
# Source inventory and raw case-member controls, with no archived code execution.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
PYTHONDONTWRITEBYTECODE=1 exec python3 "$here/_ci-archive-cases-smoke.py"
