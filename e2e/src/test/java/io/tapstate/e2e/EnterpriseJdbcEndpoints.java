package io.tapstate.e2e;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Independent JDBC seed and readback operations shared by the two enterprise source fixtures. */
abstract class EnterpriseJdbcEndpoints implements Endpoints {
    abstract String url(EndpointAddress address);
    abstract String schema(EndpointAddress address);
    abstract String scalarType(Object value);
    abstract void enableChanges(Connection connection, EndpointAddress address, String table) throws SQLException;

    static String quoted(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    String table(EndpointAddress address, String table) {
        return quoted(schema(address)) + "." + quoted(table);
    }

    Connection connect(EndpointAddress address) throws SQLException {
        String user = address.settings().containsKey("user") ? address.text("user") : address.text("username");
        return DriverManager.getConnection(url(address), user, address.text("password"));
    }

    private boolean exists(Connection connection, EndpointAddress address, String table) throws SQLException {
        try (ResultSet tables = connection.getMetaData().getTables(
                connection.getCatalog(), schema(address), table, new String[]{"TABLE"})) {
            return tables.next();
        }
    }

    @Override
    public void seed(EndpointAddress address, String table, List<Map<String, Object>> rows) {
        try (Connection connection = connect(address); Statement statement = connection.createStatement()) {
            if (exists(connection, address, table)) {
                statement.execute("DROP TABLE " + table(address, table));
            }
            Map<String, Object> shape = rows.isEmpty() ? SeedRows.generatedShape() : rows.getFirst();
            List<String> columns = new ArrayList<>();
            shape.forEach((name, value) -> columns.add(quoted(name) + " " + scalarType(value)
                    + (name.equals(SeedRows.ID) ? " PRIMARY KEY" : "")));
            statement.execute("CREATE TABLE " + table(address, table) + " (" + String.join(", ", columns) + ")");
            enableChanges(connection, address, table);
            insertRows(connection, address, table, rows);
        } catch (SQLException error) {
            throw new EnvelopeException("cannot seed " + table, error);
        }
    }

    private void insertRows(Connection connection, EndpointAddress address, String table,
                            List<Map<String, Object>> rows) throws SQLException {
        if (rows.isEmpty()) {
            return;
        }
        List<String> columns = List.copyOf(rows.getFirst().keySet());
        String sql = "INSERT INTO " + table(address, table) + " ("
                + String.join(", ", columns.stream().map(EnterpriseJdbcEndpoints::quoted).toList())
                + ") VALUES (" + String.join(", ", Collections.nCopies(columns.size(), "?")) + ")";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (Map<String, Object> row : rows) {
                bind(statement, columns.stream().map(row::get).toList());
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    @Override
    public void insert(EndpointAddress address, String table, List<Map<String, Object>> rows) {
        try (Connection connection = connect(address)) {
            insertRows(connection, address, table, rows);
        } catch (SQLException error) {
            throw new EnvelopeException("cannot insert into " + table, error);
        }
    }

    private static String predicates(Map<String, Object> values) {
        if (values.isEmpty()) {
            throw new EnvelopeException("a row change needs at least one equality setting");
        }
        return String.join(" AND ", values.keySet().stream().map(key -> quoted(key) + " = ?").toList());
    }

    private static void bind(PreparedStatement statement, List<Object> values) throws SQLException {
        for (int index = 0; index < values.size(); index++) {
            statement.setObject(index + 1, values.get(index));
        }
    }

    private void change(EndpointAddress address, String sql, List<Object> values) {
        try (Connection connection = connect(address); PreparedStatement statement = connection.prepareStatement(sql)) {
            bind(statement, values);
            int changed = statement.executeUpdate();
            if (changed != 1) {
                throw new EnvelopeException("a valued change must affect exactly one row; affected " + changed);
            }
        } catch (SQLException error) {
            throw new EnvelopeException("cannot change the addressed row", error);
        }
    }

    @Override
    public void update(EndpointAddress address, String table, Map<String, Object> where, Map<String, Object> set) {
        if (set.isEmpty()) {
            throw new EnvelopeException("an update needs at least one column");
        }
        List<Object> values = new ArrayList<>(set.values());
        values.addAll(where.values());
        change(address, "UPDATE " + table(address, table) + " SET "
                + String.join(", ", set.keySet().stream().map(key -> quoted(key) + " = ?").toList())
                + " WHERE " + predicates(where), values);
    }

    @Override
    public void delete(EndpointAddress address, String table, Map<String, Object> where) {
        change(address, "DELETE FROM " + table(address, table) + " WHERE " + predicates(where),
                new ArrayList<>(where.values()));
    }

    @Override
    public Optional<Map<String, Object>> fetch(EndpointAddress address, String table, Map<String, Object> where) {
        try (Connection connection = connect(address)) {
            if (!exists(connection, address, table)) {
                return Optional.empty();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT * FROM " + table(address, table) + " WHERE " + predicates(where))) {
                bind(statement, new ArrayList<>(where.values()));
                try (ResultSet results = statement.executeQuery()) {
                    if (!results.next()) {
                        return Optional.empty();
                    }
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int column = 1; column <= results.getMetaData().getColumnCount(); column++) {
                        row.put(results.getMetaData().getColumnLabel(column), results.getObject(column));
                    }
                    if (results.next()) {
                        throw new EnvelopeException("more than one row matches " + where);
                    }
                    return Optional.of(row);
                }
            }
        } catch (SQLException error) {
            throw new EnvelopeException("cannot read " + table, error);
        }
    }

    @Override
    public long count(EndpointAddress address, String table) {
        try (Connection connection = connect(address); Statement statement = connection.createStatement()) {
            if (!exists(connection, address, table)) {
                return 0;
            }
            try (ResultSet rows = statement.executeQuery("SELECT COUNT(*) FROM " + table(address, table))) {
                rows.next();
                return rows.getLong(1);
            }
        } catch (SQLException error) {
            throw new EnvelopeException("cannot count " + table, error);
        }
    }

    @Override
    public void cdc(EndpointAddress address, String table, CdcOp op, long rows) {
        List<Long> ids = new ArrayList<>();
        try (Connection connection = connect(address); Statement statement = connection.createStatement();
             ResultSet selected = statement.executeQuery("SELECT " + quoted(SeedRows.ID) + " FROM "
                     + table(address, table) + " ORDER BY " + quoted(SeedRows.ID))) {
            while (selected.next()) {
                ids.add(selected.getLong(1));
            }
        } catch (SQLException error) {
            throw new EnvelopeException("cannot select rows for changes to " + table, error);
        }
        if (op == CdcOp.INSERT) {
            long next = ids.isEmpty() ? 1 : ids.getLast() + 1;
            List<Map<String, Object>> added = new ArrayList<>();
            for (long offset = 0; offset < rows; offset++) {
                long id = next + offset;
                added.add(Map.of(SeedRows.ID, id, SeedRows.SEQ, id));
            }
            insert(address, table, added);
            return;
        }
        for (long id : ids.stream().limit(rows).toList()) {
            if (op == CdcOp.UPDATE) {
                update(address, table, Map.of(SeedRows.ID, id), Map.of(SeedRows.SEQ, -id));
            } else {
                delete(address, table, Map.of(SeedRows.ID, id));
            }
        }
    }

    @Override
    public void close() {
        // Every operation owns and closes its JDBC connection.
    }
}
