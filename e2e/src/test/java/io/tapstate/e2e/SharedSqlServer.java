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
            statement.execute("EXEC sys.sp_cdc_enable_db");
        } catch (SQLException error) {
            throw new EnvelopeException("cannot provision the SQL Server database " + database, error);
        }
        return settings;
    }

    private static MSSQLServerContainer<?> server() {
        if (container == null) {
            DockerGate.require();
            MSSQLServerContainer<?> starting = new MSSQLServerContainer<>(
                    "mcr.microsoft.com/mssql/server:2022-CU14-ubuntu-22.04")
                    .acceptLicense()
                    .withEnv("MSSQL_AGENT_ENABLED", "true")
                    .withStartupTimeout(Duration.ofMinutes(5))
                    .withCreateContainerCmdModifier(command -> command.withPlatform("linux/amd64"));
            long began = System.nanoTime();
            starting.start();
            container = starting;
            System.out.printf("SQL Server source fixture ready in %.1f seconds%n", (System.nanoTime() - began) / 1_000_000_000.0);
        }
        return container;
    }
}
