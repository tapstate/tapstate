package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.testcontainers.containers.MSSQLServerContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/** One SQL Server with Agent enabled, and a separate CDC-enabled database per run. */
final class SharedSqlServer {
    private static MSSQLServerContainer<?> container;

    private SharedSqlServer() {
    }

    static synchronized Map<String, Object> settings(String database) {
        MSSQLServerContainer<?> server = server();
        Map<String, Object> settings = new LinkedHashMap<>();
        settings.put("host", server.getHost());
        settings.put("port", server.getMappedPort(1433));
        settings.put("user", server.getUsername());
        settings.put("password", server.getPassword());
        settings.put("database", database);
        settings.put("schema", "dbo");
        try (Connection admin = DriverManager.getConnection(server.getJdbcUrl(), server.getUsername(), server.getPassword());
             Statement statement = admin.createStatement()) {
            try (var existing = statement.executeQuery("SELECT 1 FROM sys.databases WHERE name = '" + database + "'")) {
                if (existing.next()) {
                    return settings;
                }
            }
            statement.execute("CREATE DATABASE " + EnterpriseJdbcEndpoints.quoted(database));
            statement.execute("USE " + EnterpriseJdbcEndpoints.quoted(database));
            enableCdc(statement, Duration.ofSeconds(60));
        } catch (SQLException error) {
            throw new EnvelopeException("cannot provision the SQL Server database " + database, error);
        }
        return settings;
    }

    static void enableCdc(Statement statement, Duration bound) throws SQLException {
        long deadline = System.nanoTime() + bound.toNanos();
        while (true) {
            long remaining = deadline - System.nanoTime();
            statement.setQueryTimeout((int) Math.max(1, (remaining + 999_999_999L) / 1_000_000_000L));
            try {
                statement.execute("EXEC sys.sp_cdc_enable_db");
                return;
            } catch (SQLException error) {
                if (!deadlock(error) || System.nanoTime() - deadline >= 0) {
                    throw error;
                }
                try {
                    Await.pauseBeforeNextPoll(Duration.ofMillis(Math.min(100, Math.max(1,
                            Duration.ofNanos(deadline - System.nanoTime()).toMillis()))));
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new EnvelopeException("interrupted enabling SQL Server database CDC", interrupted);
                }
                if (System.nanoTime() - deadline >= 0) {
                    throw error;
                }
            }
        }
    }

    private static boolean deadlock(SQLException error) {
        for (SQLException current = error; current != null; current = current.getNextException()) {
            // Database CDC setup can wrap the original deadlock in a metadata error.
            if (current.getErrorCode() == 1205
                    || (current.getErrorCode() == 22830 && current.getMessage() != null
                    && current.getMessage().contains("The error returned was 1205:"))) {
                return true;
            }
        }
        return false;
    }

    private static MSSQLServerContainer<?> server() {
        if (container == null) {
            DockerGate.require();
            MSSQLServerContainer<?> starting = new MSSQLServerContainer<>(
                    "mcr.microsoft.com/mssql/server:2022-CU14-ubuntu-22.04")
                    .acceptLicense()
                    .withEnv("MSSQL_AGENT_ENABLED", "true")
                    .withEnv("MSSQL_MEMORY_LIMIT_MB", "2048")
                    .withStartupTimeout(Duration.ofMinutes(5))
                    .withCreateContainerCmdModifier(command -> {
                        command.withPlatform("linux/amd64");
                        command.getHostConfig().withMemory(3L * 1024 * 1024 * 1024);
                    });
            long began = System.nanoTime();
            starting.start();
            container = starting;
            System.out.printf("SQL Server source fixture ready in %.1f seconds%n", (System.nanoTime() - began) / 1_000_000_000.0);
        }
        return container;
    }
}
