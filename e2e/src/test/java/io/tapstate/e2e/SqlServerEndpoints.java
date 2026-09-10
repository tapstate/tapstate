package io.tapstate.e2e;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/** SQL Server seed and readback use the same database and credentials as the source resource. */
final class SqlServerEndpoints extends EnterpriseJdbcEndpoints {
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
