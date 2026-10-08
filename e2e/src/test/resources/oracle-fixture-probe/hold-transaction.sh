#!/bin/bash
set -eu
directory=/tmp/tapstate-capture-probe
# Retain the original direct SQLPlus child and readiness-only HOST return.
"$ORACLE_HOME/bin/sqlplus-probe-real" -s / as sysdba \
  > /tmp/tapstate-transaction.log 2>&1 <<'SQL' &
WHENEVER SQLERROR EXIT FAILURE ROLLBACK
CREATE TABLE SYSTEM.TAPSTATE_LOGGING_HOLD (ID NUMBER);
INSERT INTO SYSTEM.TAPSTATE_LOGGING_HOLD VALUES (1);
HOST bash /tmp/tapstate-probe-stage.sh holder-insert-complete
HOST touch /tmp/tapstate-transaction-ready
HOST bash /tmp/tapstate-probe-stage.sh holder-sleep-invocation-before
BEGIN DBMS_SESSION.SLEEP(130); END;
/
HOST bash /tmp/tapstate-probe-stage.sh holder-sleep-invocation-after
ROLLBACK;
HOST bash /tmp/tapstate-probe-stage.sh holder-rollback-complete
EXIT;
SQL
holder=$!
printf '%s\n' "$holder" > "$directory/holder.pid"
printf 'capture-probe-holder-shape=ORIGINAL_DIRECT_CHILD_NO_REAP_AFTER_READY\n'
for attempt in {1..30}; do
  if [ -f /tmp/tapstate-transaction-ready ]; then
    bash /tmp/tapstate-probe-stage.sh launcher-observed-ready
    bash /tmp/tapstate-probe-waits.sh > "$directory/waits.log" 2>&1 &
    printf '%s\n' "$!" > "$directory/wait-sampler.pid"
    printf 'capture-probe: real transaction active; bounded rollback in 130 seconds\n'
    exit 0
  fi
  if ! kill -0 "$holder" 2>/dev/null; then
    cat /tmp/tapstate-transaction.log
    exit 1
  fi
  sleep 1
done
cat /tmp/tapstate-transaction.log
exit 1
