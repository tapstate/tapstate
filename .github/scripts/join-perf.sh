#!/usr/bin/env bash
set -euo pipefail

run_join() {
  mvn -B "$@" -pl runtime/engine -am test -Dtest=JoinPerformanceGateTest \
    -Dsurefire.failIfNoSpecifiedTests=false -Djacoco.skip=true \
    -Djoinperf.scenarios=F1,F2,I1,I2,I3,I4,R1,R2,R3,R4,R5 \
    -Djoinperf.tiers=hot,mixed,cold 2>&1 | tee run.log
}

if run_join -o; then
  exit 0
else
  status=$?
fi

# A real test failure must not be retried. Only Maven's offline resolver can
# send this job down the dependency preparation path.
if ! grep -Eq '^\[ERROR\].*(Cannot access .* in offline mode|has not been downloaded from it before)' run.log; then
  exit "$status"
fi

echo '::notice::The restored Maven repository is missing Join dependencies. Preparing only missing files.'
# Retry Central downloads in place before considering a second Maven invocation.
if run_join -Daether.transport.http.retryHandler.count.central=5 \
    -Daether.transport.http.retryHandler.interval.central=10000; then
  :
else
  status=$?
  if ! grep -Eq '^\[ERROR\].*Non-resolvable import POM:.*from/to central .*([Ss]tatus code|HTTP Status):[[:space:]]*429([^[:digit:]]|$)' run.log; then
    exit "$status"
  fi
  echo '::warning::Maven Central rate limited dependency preparation. Retrying once in 15 seconds.'
  sleep 15
  # Keep the second invocation on Maven's default download retry budget.
  run_join -U
fi

# Only the offline attempt supplies the report published by the workflow.
run_join -o
