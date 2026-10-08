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
        try {
            // Supplemental logging waits for in-flight transactions after the database reopens.
            // Give that wait the same bounded budget as container startup.
            var result = starting.execInContainer("bash", "-c", """
                    timeout %d sqlplus -s / as sysdba <<'SQL'
                    WHENEVER SQLERROR EXIT SQL.SQLCODE
                    SHUTDOWN IMMEDIATE;
                    STARTUP MOUNT;
                    ALTER DATABASE ARCHIVELOG;
                    ALTER DATABASE OPEN;
                    BEGIN
                      EXECUTE IMMEDIATE 'ALTER PLUGGABLE DATABASE FREEPDB1 OPEN';
                    EXCEPTION WHEN OTHERS THEN
                      IF SQLCODE != -65019 THEN RAISE; END IF;
                    END;
                    /
                    ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
                    ALTER DATABASE FORCE LOGGING;
                    CREATE USER C##TAPSTATE IDENTIFIED BY "Tapstate_Test_42" CONTAINER=ALL;
                    GRANT DBA, LOGMINING TO C##TAPSTATE CONTAINER=ALL;
                    ALTER SYSTEM REGISTER;
                    EXIT;
                    SQL
                    """.formatted(STARTUP_BUDGET.toSeconds()));
            if (result.getExitCode() != 0) {
                throw new EnvelopeException("cannot enable Oracle change capture: " + result.getStdout() + result.getStderr());
            }
            container = starting;
            System.out.printf("Oracle source fixture ready in %.1f seconds%n", (System.nanoTime() - began) / 1_000_000_000.0);
            return container;
        } catch (Exception error) {
            starting.stop();
            if (error instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new EnvelopeException("cannot initialize Oracle source fixture", error);
        }
    }
}
