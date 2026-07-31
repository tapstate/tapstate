package io.tapstate.e2e;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The {@link Endpoints} driver for a MySQL store, addressed by the host, port, database and credentials
 * the resource carries - a JDBC endpoint has no single setting that is its address, which is why the
 * whole mapping arrives here rather than one string.
 *
 * <p>Reached with a plain JDBC driver and nothing the product uses. Rule R3 keeps connector code to the
 * adapter ring; this module is deliberately not one of them, so what this reads is a second, independent
 * opinion about what the database holds - the only kind of reading a specification's count is worth
 * taking.
 *
 * <p>Row shape is the file and Mongo drivers' shape: an id and a sequence, ids being the whole numbers
 * 1..N, an insert continuing them. One specification therefore reads the same against any store, and an
 * author choosing MySQL does not have to learn a different one.
 */
final class MySqlEndpoints implements Endpoints {

    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String DATABASE = "database";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";

    /**
     * Connection settings that are the driver's business rather than the endpoint's: a local test
     * database speaks plaintext, and connector/J refuses to fetch a server's public key over an
     * unencrypted link unless told it may. Neither changes which endpoint is dialled, which is what the
     * resource decides and what must not drift.
     */
    private static final String DRIVER_SETTINGS = "?sslMode=DISABLED&allowPublicKeyRetrieval=true";

    private final Map<String, Connection> connectionsByUrl = new LinkedHashMap<>();

    @Override
    public void seed(EndpointAddress address, String table, long rows) {
        Connection connection = connection(address);
        // Dropped rather than truncated: a seed replaces whatever the table held, including a shape some
        // earlier run left behind, and a truncate would keep the old columns.
        execute(connection, "DROP TABLE IF EXISTS " + quoted(table));
        execute(connection, "CREATE TABLE " + quoted(table) + " (id BIGINT PRIMARY KEY, seq BIGINT)");
        insertRange(connection, table, 1, rows);
    }

    @Override
    public void cdc(EndpointAddress address, String table, CdcOp op, long rows) {
        Connection connection = connection(address);
        if (!exists(connection, address, table)) {
            throw new EnvelopeException(
                    "the table " + table + " at " + address.text(HOST)
                            + " has not been seeded, so there is nothing to change");
        }
        switch (op) {
            case INSERT -> insertRange(connection, table, highestId(connection, table) + 1, rows);
            // Lowest ids by order rather than by id <= rows: after a delete the ids no longer start at
            // one, and a change meant to touch two rows would touch none.
            case UPDATE -> execute(
                    connection,
                    "UPDATE " + quoted(table) + " SET seq = -id ORDER BY id LIMIT " + rows);
            case DELETE -> execute(
                    connection, "DELETE FROM " + quoted(table) + " ORDER BY id LIMIT " + rows);
        }
    }

    /**
     * Re-emits every current row as a delete followed by an insert of the same values, in one
     * transaction. The binlog is positional: a row written before the product's tail is positioned is
     * never delivered, and nothing observable says when positioning is done. Re-emitting the whole
     * table under existing ids is idempotent at any upserting target - rows that already crossed are
     * overwritten with themselves - so it is safe to do on nothing more than suspicion of a stall.
     */
    @Override
    public void redeliver(EndpointAddress address, String table) {
        Connection connection = connection(address);
        if (!exists(connection, address, table)) {
            return;
        }
        try {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement();
                    ResultSet rows = statement.executeQuery(
                            "SELECT id, seq FROM " + quoted(table) + " ORDER BY id")) {
                try (PreparedStatement reemit = connection.prepareStatement(
                        "DELETE FROM " + quoted(table) + " WHERE id = ?");
                        PreparedStatement insert = connection.prepareStatement(
                                "INSERT INTO " + quoted(table) + " (id, seq) VALUES (?, ?)")) {
                    while (rows.next()) {
                        long id = rows.getLong(1);
                        long seq = rows.getLong(2);
                        reemit.setLong(1, id);
                        reemit.executeUpdate();
                        insert.setLong(1, id);
                        insert.setLong(2, seq);
                        insert.executeUpdate();
                    }
                }
            }
            connection.commit();
        } catch (SQLException e) {
            rollback(connection);
            throw new EnvelopeException("cannot re-emit the rows of " + table, e);
        } finally {
            restoreAutoCommit(connection);
        }
    }

    private static void rollback(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException rollbackIsBestEffort) {
            // The re-emission already failed; the original failure is the one worth reporting.
        }
    }

    private static void restoreAutoCommit(Connection connection) {
        try {
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            throw new EnvelopeException("cannot restore auto-commit after a re-emission", e);
        }
    }

    /**
     * The rows the table holds now, or none when the table is not there. A table the product has not
     * created yet is absent rather than empty, and the honest count of it is zero: a specification that
     * waits for a first write is waiting for exactly this reading to move. Letting the database's "no
     * such table" out instead would fail every such wait on its first poll.
     */
    @Override
    public long count(EndpointAddress address, String table) {
        Connection connection = connection(address);
        if (!exists(connection, address, table)) {
            return 0L;
        }
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT COUNT(*) FROM " + quoted(table))) {
            return results.next() ? results.getLong(1) : 0L;
        } catch (SQLException e) {
            throw new EnvelopeException("cannot count " + table + " at " + address.text(HOST), e);
        }
    }

    @Override
    public void close() {
        for (Connection connection : connectionsByUrl.values()) {
            try {
                connection.close();
            } catch (SQLException closingIsBestEffort) {
                // A connection that cannot be closed is already unusable; failing here would replace a
                // test's real result with the noise of its own cleanup.
            }
        }
        connectionsByUrl.clear();
    }

    /**
     * Asked of the catalog rather than by running the query and reading the failure: "no such table" is
     * one of several reasons a select fails, and telling them apart by error code would make an absent
     * table and an unreachable database look alike to a waiting specification.
     */
    private boolean exists(Connection connection, EndpointAddress address, String table) {
        String sql = "SELECT COUNT(*) FROM information_schema.tables "
                + "WHERE table_schema = ? AND table_name = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, address.text(DATABASE));
            statement.setString(2, table);
            try (ResultSet results = statement.executeQuery()) {
                return results.next() && results.getLong(1) > 0;
            }
        } catch (SQLException e) {
            throw new EnvelopeException("cannot look up " + table + " at " + address.text(HOST), e);
        }
    }

    private void insertRange(Connection connection, String table, long firstId, long rows) {
        if (rows <= 0) {
            return;
        }
        String sql = "INSERT INTO " + quoted(table) + " (id, seq) VALUES (?, ?)";
        try (PreparedStatement insert = connection.prepareStatement(sql)) {
            for (long id = firstId; id < firstId + rows; id++) {
                insert.setLong(1, id);
                insert.setLong(2, id);
                insert.addBatch();
            }
            insert.executeBatch();
        } catch (SQLException e) {
            throw new EnvelopeException("cannot write rows into " + table, e);
        }
    }

    private long highestId(Connection connection, String table) {
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery("SELECT MAX(id) FROM " + quoted(table))) {
            return results.next() ? results.getLong(1) : 0L;
        } catch (SQLException e) {
            throw new EnvelopeException("cannot read the highest id in " + table, e);
        }
    }

    private void execute(Connection connection, String sql) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new EnvelopeException("cannot run " + sql, e);
        }
    }

    private Connection connection(EndpointAddress address) {
        String url = "jdbc:mysql://" + address.text(HOST) + ":" + address.text(PORT) + "/"
                + address.text(DATABASE) + DRIVER_SETTINGS;
        return connectionsByUrl.computeIfAbsent(url, key -> {
            try {
                return DriverManager.getConnection(key, address.text(USERNAME), address.text(PASSWORD));
            } catch (SQLException e) {
                throw new EnvelopeException("cannot reach the endpoint at " + key, e);
            }
        });
    }

    /** Guards a table name that collides with a reserved word; specifications choose these names. */
    private static String quoted(String table) {
        return "`" + table.replace("`", "``") + "`";
    }
}
