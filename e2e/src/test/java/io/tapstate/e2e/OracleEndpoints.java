package io.tapstate.e2e;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/** The independent Oracle reader honors the resource's service, PDB and schema settings. */
final class OracleEndpoints extends EnterpriseJdbcEndpoints {
    @Override
    String url(EndpointAddress address) {
        if ("SID".equals(address.settings().get("thinType")) && !address.settings().containsKey("pdb")) {
            return "jdbc:oracle:thin:@" + address.text("host") + ":" + address.text("port") + ":" + address.text("sid");
        }
        String service = address.settings().containsKey("pdb") ? address.text("pdb") : address.text("database");
        return "jdbc:oracle:thin:@//" + address.text("host") + ":" + address.text("port") + "/" + service;
    }

    @Override
    String schema(EndpointAddress address) {
        return address.text("schema");
    }

    @Override
    String scalarType(Object value) {
        return value instanceof String ? "VARCHAR2(255)" : "NUMBER(19)";
    }

    @Override
    void enableChanges(Connection connection, EndpointAddress address, String table) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("ALTER TABLE " + table(address, table) + " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        }
    }
}
