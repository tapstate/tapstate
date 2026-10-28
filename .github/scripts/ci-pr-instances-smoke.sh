#!/usr/bin/env bash
# Offline source, identity and archive controls; no workflows, JVMs or Docker.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
exec python3 "$here/_ci-pr-instances-smoke.py"
