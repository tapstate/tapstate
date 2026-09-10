package io.tapstate.e2e;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;

/** SQL Server seed and readback use the same database and credentials as the source resource. */
final class SqlServerEndpoints extends EnterpriseJdbcEndpoints {
    @Override
    public void seed(EndpointAddress address, String table, List<Map<String, Object>> rows) {
        super.seed(address, table, rows);
        // SQL Agent captures asynchronously. The native reader needs an initial LSN;
        // a connection-ready database can still return null immediately after CDC is enabled.
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        try (Connection connection = connect(address);
             var statement = connection.createStatement()) {
            statement.setQueryTimeout(5);
            do {
                try (var result = statement.executeQuery("SELECT max(start_lsn) FROM cdc.lsn_time_mapping")) {
                    if (result.next() && result.getBytes(1) != null) {
                        return;
                    }
                }
                Thread.sleep(100);
            } while (System.nanoTime() < deadline);
        } catch (SQLException error) {
            throw new EnvelopeException("cannot inspect SQL Server CDC readiness for " + table, error);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new EnvelopeException("interrupted waiting for SQL Server CDC readiness for " + table, interrupted);
        }
        throw new EnvelopeException("SQL Server CDC did not publish an initial LSN within 60 seconds for " + table);
    }

    @Override
    String url(EndpointAddress address) {
        return "jdbc:sqlserver://" + address.text("host") + ":" + address.text("port")
                + ";databaseName={" + address.text("database").replace("}", "}}")
                + "};encrypt=false;trustServerCertificate=true";
    }

    @Override
    String schema(EndpointAddress address) {
        return String.valueOf(address.settings().getOrDefault("schema", "dbo"));
    }

    @Override
    String scalarType(Object value) {
        return value instanceof String ? "VARCHAR(255)" : "BIGINT";
    }

    @Override
    void enableChanges(Connection connection, EndpointAddress address, String table) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "EXEC sys.sp_cdc_enable_table @source_schema=?, @source_name=?, @role_name=NULL, @supports_net_changes=0")) {
            statement.setString(1, schema(address));
            statement.setString(2, table);
            statement.execute();
        }
    }
}
