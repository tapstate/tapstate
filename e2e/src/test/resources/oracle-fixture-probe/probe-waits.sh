#!/bin/bash
set -eu
directory=/tmp/tapstate-capture-probe
main=$(cat "$directory/main.pid")
holder=$(cat "$directory/holder.pid")
[[ "$main" =~ ^[0-9]+$ && "$holder" =~ ^[0-9]+$ ]]
began=$SECONDS
# Three fixed samples bracket the original hold without a continuous poller.
for offset in 0 120 140; do
  remaining=$((began + offset - SECONDS))
  if [ "$remaining" -gt 0 ]; then sleep "$remaining"; fi
  if [ ! -r "/proc/$main/cmdline" ] ||
     ! tr '\0' ' ' < "/proc/$main/cmdline" | grep -q 'sqlplus-probe-real'; then
    bash /tmp/tapstate-probe-stage.sh "wait-sample-$offset-main-not-present"
    exit 0
  fi
  bash /tmp/tapstate-probe-stage.sh "wait-sample-$offset-before"
  set +e
  timeout 2 "$ORACLE_HOME/bin/sqlplus-probe-real" -s / as sysdba <<SQL
WHENEVER SQLERROR EXIT FAILURE ROLLBACK
SET LINESIZE 300 PAGESIZE 100 TRIMSPOOL ON
SELECT s.sid, s.serial#, s.process, s.status, s.state, s.event,
       s.seconds_in_wait, s.sql_id, s.blocking_session, s.final_blocking_session,
       t.xidusn, t.xidslot, t.xidsqn
  FROM v\$session s LEFT JOIN v\$transaction t ON s.taddr = t.addr
 WHERE REGEXP_LIKE(s.process, '^($main|$holder)(:|$)')
 ORDER BY s.sid;
EXIT;
SQL
  status=$?
  set -e
  printf 'capture-probe-wait-sample offset=%s sqlplusExit=%s\n' "$offset" "$status"
  if [ -f "$directory/holder.exit" ]; then
    printf 'capture-probe-observed-holder-exit='; cat "$directory/holder.exit"
  else
    printf 'capture-probe-observed-holder-exit=UNKNOWN\n'
  fi
  bash /tmp/tapstate-probe-stage.sh "wait-sample-$offset-after"
done
