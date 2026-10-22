#!/bin/bash
# Observe the controlled wait before an outer timeout interrupts the SQL client.
for sample in {1..25}; do
  date -u '+oracle-fixture-live sample epoch-ms=%s%3N'
  for process in /proc/[0-9]*; do
    [ -r "$process/comm" ] || continue
    read -r command < "$process/comm" || continue
    case "$command" in
      sqlplus*|bash|timeout)
        printf 'process %s ' "$process"
        cat "$process/stat" "$process/wchan" 2>/dev/null || true
        printf '\n'
        ;;
    esac
  done
  timeout --kill-after=2 5 "$ORACLE_HOME/bin/sqlplus-probe-real" -s / as sysdba <<'SQL'
WHENEVER SQLERROR CONTINUE NONE
SET HEADING OFF FEEDBACK OFF PAGESIZE 0 LINESIZE 1000 TRIMSPOOL ON
SELECT 'session|' || S.SID || '|' || S.SERIAL# || '|' || S.PROCESS || '|' || P.SPID
       || '|' || S.STATE || '|' || S.EVENT || '|' || S.SQL_ID || '|' || S.PREV_SQL_ID
       || '|' || S.BLOCKING_SESSION_STATUS || '|' || S.BLOCKING_SESSION
       || '|' || S.FINAL_BLOCKING_SESSION_STATUS || '|' || S.FINAL_BLOCKING_SESSION
       || '|' || S.P1 || '|' || S.P2 || '|' || S.P3 || '|' || S.SECONDS_IN_WAIT
  FROM V$SESSION S JOIN V$PROCESS P ON P.ADDR = S.PADDR
 WHERE S.PROGRAM LIKE '%sqlplus%';
SELECT 'sql|' || SQL_ID || '|' || SQL_TEXT FROM V$SQLAREA
 WHERE SQL_ID IN (SELECT SQL_ID FROM V$SESSION WHERE PROGRAM LIKE '%sqlplus%');
SELECT 'transaction|' || RAWTOHEX(XID) || '|' || STATUS || '|' || START_TIME
       || '|' || USED_UBLK || '|' || USED_UREC FROM V$TRANSACTION;
SELECT 'lock|' || SID || '|' || TYPE || '|' || ID1 || '|' || ID2
       || '|' || LMODE || '|' || REQUEST || '|' || BLOCK FROM V$LOCK
 WHERE TYPE IN ('TX', 'TM');
SELECT 'logging|' || LOG_MODE || '|' || SUPPLEMENTAL_LOG_DATA_MIN || '|' || FORCE_LOGGING
  FROM V$DATABASE;
EXIT;
SQL
  printf 'observation-exit=%s\n' "$?"
  sleep 10
done
