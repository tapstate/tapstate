package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.testcontainers.oracle.OracleContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

/** One Oracle Free server per JVM, with a separate schema for every specification run. */
final class SharedOracle {
    private static final String PASSWORD = "Tapstate_Test_42";
    private static final String USER = "C##TAPSTATE";
    private static final Duration STARTUP_BUDGET = Duration.ofMinutes(5);
    private static OracleContainer container;

    // DriverManager hands a caller only a driver registered from a class loader that caller can see, and it
    // discovers drivers once, on its first use, through the context class loader of whichever thread uses it
    // first. In a JVM that has already run a server, that thread can be a connector's, whose own loader cannot
    // see this classpath, and the container does not load the driver either: it waits on its log, not on a
    // connection. So the driver is registered from here, where every caller of this fixture can see it.
    static {
        try {
            DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
        } catch (SQLException error) {
            throw new ExceptionInInitializerError(error);
        }
    }

    private SharedOracle() {
    }

    static synchronized Map<String, Object> settings(String database) {
        OracleContainer server = server();
        String schema = schemaName(database);
        Map<String, Object> settings = new LinkedHashMap<>();
        settings.put("host", server.getHost());
        settings.put("port", server.getOraclePort());
        settings.put("user", USER);
        settings.put("password", PASSWORD);
        settings.put("schema", schema);
        settings.put("thinType", "SERVICE_NAME");
        settings.put("database", "FREE");
        settings.put("multiTenant", true);
        settings.put("pdb", "FREEPDB1");
        try (Connection connection = DriverManager.getConnection(
                "jdbc:oracle:thin:@//" + server.getHost() + ":" + server.getOraclePort() + "/FREEPDB1",
                USER, PASSWORD); Statement statement = connection.createStatement()) {
            try (var user = statement.executeQuery(
                    "SELECT 1 FROM ALL_USERS WHERE USERNAME = '" + schema + "'")) {
                if (user.next()) {
                    return settings;
                }
            }
            statement.execute("CREATE USER " + EnterpriseJdbcEndpoints.quoted(schema)
                    + " IDENTIFIED BY \"" + PASSWORD + "\" QUOTA UNLIMITED ON USERS");
            statement.execute("GRANT CREATE SESSION, CREATE TABLE TO " + EnterpriseJdbcEndpoints.quoted(schema));
        } catch (SQLException error) {
            throw new EnvelopeException("cannot provision the Oracle schema " + schema, error);
        }
        return settings;
    }

    /** Supplemental LogMiner reports long schema identifiers as unsupported redo. */
    static String schemaName(String database) {
        String candidate = database.toUpperCase(Locale.ROOT);
        int miningNameLimit = 30;
        if (candidate.length() <= miningNameLimit) {
            return candidate;
        }
        String digest = ProvisionedStores.digest(candidate).toUpperCase(Locale.ROOT);
        return candidate.substring(0, miningNameLimit - digest.length() - 1) + "_" + digest;
    }

    private static OracleContainer server() {
        if (container != null) {
            return container;
        }
        DockerGate.require();
        OracleContainer starting = new OracleContainer("gvenzl/oracle-free:23-slim-faststart")
                .withPassword(PASSWORD)
                .withStartupTimeout(STARTUP_BUDGET);
        long began = System.nanoTime();
        starting.start();
        System.err.printf("oracle-fixture-probe container-ready id=%s epoch-ms=%d%n",
                starting.getContainerId(), System.currentTimeMillis());
        try {
            // Supplemental logging waits for in-flight transactions after the database reopens.
            // Give that wait the same bounded budget as container startup.
            // SQL*Plus does not echo redirected input, so mark statements before executing them.
            var result = starting.execInContainer("bash", "-c", """
                    timeout %d sqlplus -s / as sysdba <<'SQL'
                    WHENEVER SQLERROR EXIT SQL.SQLCODE
                    HOST date -u '+oracle-fixture-probe main-stage=shutdown epoch-ms=%%s%%3N' >> /tmp/tapstate-main-stage-probe.log
                    PROMPT SHUTDOWN IMMEDIATE;
                    SHUTDOWN IMMEDIATE;
                    HOST date -u '+oracle-fixture-probe main-stage=startup-mount epoch-ms=%%s%%3N' >> /tmp/tapstate-main-stage-probe.log
                    PROMPT STARTUP MOUNT;
                    STARTUP MOUNT;
                    HOST date -u '+oracle-fixture-probe main-stage=archivelog epoch-ms=%%s%%3N' >> /tmp/tapstate-main-stage-probe.log
                    PROMPT ALTER DATABASE ARCHIVELOG;
                    ALTER DATABASE ARCHIVELOG;
                    HOST date -u '+oracle-fixture-probe main-stage=database-open epoch-ms=%%s%%3N' >> /tmp/tapstate-main-stage-probe.log
                    PROMPT ALTER DATABASE OPEN;
                    ALTER DATABASE OPEN;
                    HOST date -u '+oracle-fixture-probe main-stage=pdb-open epoch-ms=%%s%%3N' >> /tmp/tapstate-main-stage-probe.log
                    PROMPT ALTER PLUGGABLE DATABASE FREEPDB1 OPEN;
                    BEGIN
                      EXECUTE IMMEDIATE 'ALTER PLUGGABLE DATABASE FREEPDB1 OPEN';
                    EXCEPTION WHEN OTHERS THEN
                      IF SQLCODE != -65019 THEN RAISE; END IF;
                    END;
                    /
                    HOST date -u '+oracle-fixture-probe main-stage=supplemental epoch-ms=%%s%%3N' >> /tmp/tapstate-main-stage-probe.log
                    PROMPT Enabling minimal supplemental logging;
                    ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
                    HOST date -u '+oracle-fixture-probe main-stage=force-logging epoch-ms=%%s%%3N' >> /tmp/tapstate-main-stage-probe.log
                    PROMPT ALTER DATABASE FORCE LOGGING;
                    ALTER DATABASE FORCE LOGGING;
                    HOST date -u '+oracle-fixture-probe main-stage=create-user epoch-ms=%%s%%3N' >> /tmp/tapstate-main-stage-probe.log
                    PROMPT CREATE USER C##TAPSTATE;
                    CREATE USER C##TAPSTATE IDENTIFIED BY "Tapstate_Test_42" CONTAINER=ALL;
                    HOST date -u '+oracle-fixture-probe main-stage=grant epoch-ms=%%s%%3N' >> /tmp/tapstate-main-stage-probe.log
                    PROMPT GRANT DBA, LOGMINING TO C##TAPSTATE CONTAINER=ALL;
                    GRANT DBA, LOGMINING TO C##TAPSTATE CONTAINER=ALL;
                    HOST date -u '+oracle-fixture-probe main-stage=register epoch-ms=%%s%%3N' >> /tmp/tapstate-main-stage-probe.log
                    PROMPT ALTER SYSTEM REGISTER;
                    ALTER SYSTEM REGISTER;
                    EXIT;
                    SQL
                    status=$?
                    printf '\nOracle bounded diagnostic stage journal:\n'
                    for journal in /tmp/tapstate-main-stage-probe.log /tmp/tapstate-holder-stage-probe.log /tmp/tapstate-transaction.log; do
                      [ -f "$journal" ] || continue
                      printf '\nOracle diagnostic journal: %%s\n' "$journal"
                      head -c 65536 "$journal" || true
                    done
                    if [ "$status" -ne 0 ]; then
                      printf '\\nSQL*Plus exit code: %%s\\n' "$status"
                      timeout 10 bash -c '
                        found=0
                        for alert in "$ORACLE_BASE"/diag/rdbms/*/*/trace/alert_*.log; do
                          [ -f "$alert" ] || continue
                          found=1
                          printf "\\nOracle alert log (last 200 lines): %%s\\n" "$alert"
                          tail -n 200 "$alert" || exit $?
                        done
                        [ "$found" -eq 1 ] || exit 1
                      ' 2>&1 || printf 'Oracle alert log collection failed or timed out.\\n'
                    fi
                    exit "$status"
                    """.formatted(STARTUP_BUDGET.toSeconds()));
            System.err.printf("oracle-fixture-probe capture-sql-exit=%d stdout=%n%s%n stderr=%n%s%n",
                    result.getExitCode(), result.getStdout(), result.getStderr());
            if (result.getExitCode() != 0) {
                throw new EnvelopeException("cannot enable Oracle change capture: " + result.getStdout() + result.getStderr());
            }
            container = starting;
            System.out.printf("Oracle source fixture ready in %.1f seconds%n", (System.nanoTime() - began) / 1_000_000_000.0);
            return container;
        } catch (Exception error) {
            boolean interrupted = error instanceof InterruptedException;
            interrupted |= captureInitializationState(starting, error);
            try {
                starting.stop();
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            throw new EnvelopeException("cannot initialize Oracle source fixture", error);
        }
    }

    /** Retains live initialization state before a failed fixture is stopped. */
    private static boolean captureInitializationState(OracleContainer server, Exception failure) {
        var capture = new FutureTask<>(() -> server.execInContainer("bash", "-c", """
                for probe_log in /tmp/tapstate-initialization-stage.log /tmp/tapstate-initialization.log /tmp/tapstate-transaction.log; do
                  if [ -f "$probe_log" ]; then
                    printf '\\nOracle test probe log (last 120 lines): %s\\n' "$probe_log"
                    tail -n 120 "$probe_log"
                  fi
                done
                for probe_pid_file in /tmp/tapstate-main-sqlplus.pid /tmp/tapstate-transaction.pid; do
                  if [ -f "$probe_pid_file" ]; then
                    probe_pid=$(cat "$probe_pid_file")
                    case "$probe_pid" in
                      ''|*[!0-9]*) printf 'Invalid Oracle test probe PID: %s\\n' "$probe_pid_file" ;;
                      *)
                        printf 'Oracle test probe process: %s PID=%s\\n' "$probe_pid_file" "$probe_pid"
                        if kill -0 "$probe_pid" 2>/dev/null; then
                          awk '/^(Name|State|PPid):/ { print }' "/proc/$probe_pid/status"
                        else
                          printf 'Oracle test probe process has exited.\\n'
                        fi
                        ;;
                    esac
                  fi
                done
                for journal in /tmp/tapstate-main-stage-probe.log /tmp/tapstate-holder-stage-probe.log /tmp/tapstate-transaction.log; do
                  [ -f "$journal" ] || continue
                  printf '\nOracle diagnostic journal before cleanup: %s\n' "$journal"
                  head -c 65536 "$journal" || true
                done
                oracle_state_output=$(mktemp) || exit $?
                timeout --kill-after=2 10 sqlplus -s / as sysdba > "$oracle_state_output" 2>&1 <<'SQL'
                WHENEVER SQLERROR CONTINUE NONE
                SET LINESIZE 260
                SET PAGESIZE 100
                SELECT STATUS, DATABASE_STATUS, STARTUP_TIME FROM V$INSTANCE;
                SELECT LOG_MODE, SUPPLEMENTAL_LOG_DATA_MIN, FORCE_LOGGING FROM V$DATABASE;
                SELECT S.SID, S.SERIAL#, S.STATUS, S.STATE, S.EVENT, S.SECONDS_IN_WAIT,
                       S.SQL_ID, S.PREV_SQL_ID, S.SQL_EXEC_START, S.PROCESS AS CLIENT_PROCESS,
                       P.SPID AS SERVER_PROCESS
                  FROM V$SESSION S JOIN V$PROCESS P ON P.ADDR = S.PADDR
                 WHERE S.PROGRAM LIKE '%sqlplus%';
                SELECT SQL_ID, SQL_TEXT FROM V$SQLAREA
                 WHERE SQL_ID IN (SELECT SQL_ID FROM V$SESSION WHERE PROGRAM LIKE '%sqlplus%'
                                  UNION
                                  SELECT PREV_SQL_ID FROM V$SESSION WHERE PROGRAM LIKE '%sqlplus%');
                SELECT XID, STATUS, START_TIME, USED_UBLK, USED_UREC FROM V$TRANSACTION;
                SELECT USERNAME FROM ALL_USERS WHERE USERNAME = 'C##TAPSTATE';
                EXIT;
                SQL
                status=$?
                cat "$oracle_state_output"
                rm -f "$oracle_state_output"
                exit "$status"
                """));
        try {
            Thread.ofVirtual().name("oracle-state-capture").start(capture);
            var state = capture.get(15, TimeUnit.SECONDS);
            failure.addSuppressed(new EnvelopeException("Oracle initialization state (SQL*Plus exit "
                    + state.getExitCode() + "):\n" + state.getStdout() + state.getStderr()));
            System.err.printf("Oracle initialization state before cleanup (SQL*Plus exit %d):%n%s%s",
                    state.getExitCode(), state.getStdout(), state.getStderr());
        } catch (InterruptedException interrupted) {
            failure.addSuppressed(interrupted);
            return true;
        } catch (Exception diagnosticFailure) {
            failure.addSuppressed(diagnosticFailure);
        } finally {
            capture.cancel(true);
        }
        return false;
    }
}
