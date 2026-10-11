#!/usr/bin/env bash
# Regression cases for the pinned Web checkout, build profile, and generated-resource contract.
set -euo pipefail

script_directory="$(cd "$(dirname "$0")" && pwd -P)"
exec python3 "$script_directory/web-assets-profile-smoke.py" "$@"
