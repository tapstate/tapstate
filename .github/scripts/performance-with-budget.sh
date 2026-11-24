#!/usr/bin/env bash
set -euo pipefail

stage=$1
shift
remaining=$((STOP_AT - $(date +%s)))
status=124
if (( remaining > 0 )); then
  if timeout --kill-after=10 "$remaining" "$@"; then
    exit 0
  else
    status=$?
  fi
fi

if (( (status == 124 || status == 137) && STOP_AT <= $(date +%s) )); then
  printf 'The batch budget expired during %s.\n' "$stage" > "$RUNNER_TEMP/performance-batch/expired.txt"
fi
exit "$status"
