#!/usr/bin/env bash
# Archive actual API instance candidates without certifying historical event membership.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
exec python3 "$here/_ci-pr-instances.py" "$@"
