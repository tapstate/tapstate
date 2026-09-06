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

/**
 * The {@link Endpoints} driver for a PostgreSQL store, addressed by the host, port, database and
 * credentials the resource carries - a JDBC endpoint has no single setting that is its address, which is
 * why the whole mapping arrives here rather than one string.
 *
 * <p>Reached with a plain JDBC driver and nothing the product uses, so what this reads is a second,
 * independent opinion about what the database holds - the only kind of reading a specification's count is
 * worth taking.
 *
 * <p>Row shape is the file, Mongo and MySQL drivers' shape: an id and a sequence, ids being the whole
 * numbers 1..N, an insert continuing them. One specification therefore reads the same against any store,
 * and an author choosing PostgreSQL does not have to learn a different one.
 *
 * <p>Three things differ from the MySQL driver, and all three are dialect rather than choice: identifiers
 * are quoted with double quotes; a positioned update or delete cannot carry {@code ORDER BY} and
 * {@code LIMIT} directly, so it selects the ids first; and tables are looked up in the {@code public}
 * schema, because a store here is a database of its own and every table it holds lands in that schema.
 */
final class PostgresEndpoints implements Endpoints {

    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String DATABASE = "database";
    /**
     * The account, under either name it can arrive with.
     *
     * <p>This driver is addressed from two places and they do not agree, for good reasons on both
     * sides. Provisioning hands it the harness's own settings, which spell the account the same way for
     * every store so that one specification reads the same against any of them. A resource hands it the
     * connector's settings, and this connector spells the account "user" where MySQL says "username" -
     * a resource written any other way would not connect. Rather than make one of them lie, the driver
     * accepts both and says so.
     */
    private static final String USER = "user";
    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";

    /**
     * The schema a table lands in. Every store is a database of its own, so nothing here ever writes
     * outside the default schema - naming it keeps the catalog lookup from matching a same-named table
     * some other schema happens to hold.
     */
    private static final String SCHEMA = "public";

    private final Map<String, Connection> connectionsByUrl = new LinkedHashMap<>();

    @Override
    public void seed(EndpointAddress address, String table, List<Map<String, Object>> rows) {
        Connection connection = connection(address);
        // Dropped rather than truncated: a seed replaces whatever the table held, including a shape some
        // earlier run left behind, and a truncate would keep the old columns.
        execute(connection, "DROP TABLE IF EXISTS " + quoted(table));
        // A seed of no rows still lays the table down: seeding nothing is how a specification says the
        // table exists and holds nothing, and a wait for the first write is a wait on a table that is
        // already there. Explicit values can never be empty, so an empty seed is the generated shape.
        execute(connection, createTable(table, rows.isEmpty() ? SeedRows.generatedShape() : rows.getFirst()));
        // The whole previous row on an update or a delete, not the key alone, which is all PostgreSQL
        // sends unless it is told otherwise. A change to a row that is embedded somewhere has to be
        // placeable by what the row *was*, not only by what it became - and a delete has nothing else at
        // all. Without this a specification can add a child row and watch it arrive, then delete it and
        // watch nothing happen, with every other reading healthy. Set on the seed rather than left to
        // each case: two bespoke witnesses were already doing it by hand, which is a step a declarative
        // example has no way to take.
        execute(connection, "ALTER TABLE " + quoted(table) + " REPLICA IDENTITY FULL");
        if (!rows.isEmpty()) {
            insertRows(connection, table, rows);
        }
    }

    /**
     * Puts the table back to what PostgreSQL does unless it is told otherwise, which is to send the key
     * columns of the old row and nothing else. The seed above turns that up to the whole row; this is
     * the one case that wants it left alone, and it is spelled as an undoing of the seed rather than as
     * a branch inside it so that the seed keeps saying one thing.
     */
    @Override
    public void withoutBeforeImages(EndpointAddress address, String table) {
        execute(connection(address), "ALTER TABLE " + quoted(table) + " REPLICA IDENTITY DEFAULT");
    }

    /**
     * The table the first row describes: {@code id} is the primary key, integers become BIGINT and
     * strings VARCHAR. The parser has already held every row to one shape and to these two scalar
     * types, so the first row speaks for all of them.
     */
    private static String createTable(String table, Map<String, Object> shape) {
        StringBuilder columns = new StringBuilder();
        shape.forEach((column, value) -> {
            if (!columns.isEmpty()) {
                columns.append(", ");
            }
            columns.append(quoted(column))
                    .append(value instanceof String ? " VARCHAR(255)" : " BIGINT")
                    .append(SeedRows.ID.equals(column) ? " PRIMARY KEY" : "");
        });
        return "CREATE TABLE " + quoted(table) + " (" + columns + ")";
    }

    private void insertRows(Connection connection, String table, List<Map<String, Object>> rows) {
        List<String> columns = List.copyOf(rows.getFirst().keySet());
        String sql = "INSERT INTO " + quoted(table) + " ("
                + String.join(", ", columns.stream().map(PostgresEndpoints::quoted).toList())
                + ") VALUES (" + String.join(", ", Collections.nCopies(columns.size(), "?")) + ")";
        try (PreparedStatement insert = connection.prepareStatement(sql)) {
            for (Map<String, Object> row : rows) {
                for (int i = 0; i < columns.size(); i++) {
                    insert.setObject(i + 1, row.get(columns.get(i)));
                }
                insert.addBatch();
            }
            insert.executeBatch();
        } catch (SQLException e) {
            throw new EnvelopeException("cannot write rows into " + table, e);
        }
    }

    /**
     * Reads the whole matching row back as the columns the table carries. Equality only, and exactly
     * one match allowed: a specification locating a document ambiguously should hear that, not get
     * whichever row sorted first.
     */
    @Override
    public Optional<Map<String, Object>> fetch(EndpointAddress address, String table, Map<String, Object> where) {
        Connection connection = connection(address);
        if (!exists(connection, address, table)) {
            return Optional.empty();
        }
        List<String> settings = List.copyOf(where.keySet());
        String sql = "SELECT * FROM " + quoted(table) + " WHERE "
                + String.join(" AND ", settings.stream().map(s -> quoted(s) + " = ?").toList());
        try (PreparedStatement select = connection.prepareStatement(sql)) {
            for (int i = 0; i < settings.size(); i++) {
                select.setObject(i + 1, where.get(settings.get(i)));
            }
            try (ResultSet results = select.executeQuery()) {
                if (!results.next()) {
                    return Optional.empty();
                }
                Map<String, Object> row = rowOf(results);
                if (results.next()) {
                    throw new EnvelopeException(
                            "more than one row in " + table + " matches " + where
                                    + "; a document read must locate exactly one");
                }
                return Optional.of(row);
            }
        } catch (SQLException e) {
            throw new EnvelopeException("cannot read the row of " + table + " matching " + where, e);
        }
    }

    private static Map<String, Object> rowOf(ResultSet results) throws SQLException {
        Map<String, Object> row = new LinkedHashMap<>();
        var metadata = results.getMetaData();
        for (int column = 1; column <= metadata.getColumnCount(); column++) {
            row.put(metadata.getColumnLabel(column), results.getObject(column));
        }
        return row;
    }

    /**
     * Sets the given columns on the one row the equality settings locate, leaving its other columns
     * alone. Plain equality on both halves, so unlike this driver's row-count changes there is no
     * {@code ORDER BY} to place and nothing about it is dialect - what differs from the MySQL driver is
     * only how an identifier is quoted.
     */
    @Override
    public void update(
            EndpointAddress address, String table, Map<String, Object> where, Map<String, Object> set) {
        List<String> columns = List.copyOf(set.keySet());
        List<String> settings = List.copyOf(where.keySet());
        String sql = "UPDATE " + quoted(table)
                + " SET " + String.join(", ", columns.stream().map(c -> quoted(c) + " = ?").toList())
                + " WHERE " + String.join(" AND ", settings.stream().map(s -> quoted(s) + " = ?").toList());
        List<Object> values = new ArrayList<>();
        columns.forEach(column -> values.add(set.get(column)));
        settings.forEach(setting -> values.add(where.get(setting)));
        change(address, table, sql, values, where, "update");
    }

    /** Removes the one row the settings locate; matching none is an error, as it is for an update. */
    @Override
    public void delete(EndpointAddress address, String table, Map<String, Object> where) {
        List<String> settings = List.copyOf(where.keySet());
        String sql = "DELETE FROM " + quoted(table) + " WHERE "
                + String.join(" AND ", settings.stream().map(s -> quoted(s) + " = ?").toList());
        List<Object> values = settings.stream().map(where::get).toList();
        change(address, table, sql, values, where, "delete");
    }

    /**
     * Adds the given rows to a table that is already seeded, leaving what it held alone.
     *
     * <p>An unseeded table is refused by name rather than left to the database. A case that adds rows
     * to a table nobody laid down is about to wait for them downstream, and "no such table" surfacing
     * from the driver reads as a store that is unreachable rather than as a specification that skipped
     * its seed.
     */
    @Override
    public void insert(EndpointAddress address, String table, List<Map<String, Object>> rows) {
        Connection connection = connection(address);
        if (!exists(connection, address, table)) {
            throw new EnvelopeException(
                    "the table " + table + " at " + address.text(HOST)
                            + " has not been seeded, so there is nothing to add to");
        }
        insertRows(connection, table, rows);
    }

    /**
     * Runs one valued change and holds it to having moved exactly one row.
     *
     * <p>Zero rows is refused rather than passed over: the case that wrote the change is about to wait
     * for its effect downstream, and a silent no-op turns that wait into a timeout that reads like the
     * product dropped a change nobody actually made. More than one row is refused for the same reason a
     * document read refuses it - the specification named a row, and moving several is not what it asked.
     */
    private void change(
            EndpointAddress address,
            String table,
            String sql,
            List<Object> values,
            Map<String, Object> where,
            String what) {
        Connection connection = connection(address);
        if (!exists(connection, address, table)) {
            throw new EnvelopeException(
                    "cannot " + what + " a row of " + table + ": the table does not exist");
        }
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < values.size(); i++) {
                statement.setObject(i + 1, values.get(i));
            }
            int moved = statement.executeUpdate();
            if (moved != 1) {
                throw new EnvelopeException(
                        "a " + what + " of " + table + " matching " + where + " moved " + moved
                                + " rows; a valued change names exactly one");
            }
        } catch (SQLException e) {
            throw new EnvelopeException(
                    "cannot " + what + " the row of " + table + " matching " + where, e);
        }
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
            case UPDATE -> {
                requireSequence(connection, table);
                execute(connection, "UPDATE " + quoted(table) + " SET " + quoted(SeedRows.SEQ)
                        + " = -" + quoted(SeedRows.ID) + " WHERE " + quoted(SeedRows.ID) + " IN ("
                        + lowestIds(table, rows) + ")");
            }
            case DELETE -> execute(connection, "DELETE FROM " + quoted(table) + " WHERE "
                    + quoted(SeedRows.ID) + " IN (" + lowestIds(table, rows) + ")");
        }
    }

    /**
     * The ids of the lowest {@code rows} rows, as a subquery.
     *
     * <p>MySQL spells this as {@code ORDER BY ... LIMIT} on the update or delete itself; PostgreSQL has
     * no such form, so the rows are chosen first and matched by id. The result is the same set of rows
     * and, because the choice is made inside the one statement, it is made against the same snapshot -
     * writing it as two statements would leave a window in which a concurrent change moved the boundary.
     */
    private static String lowestIds(String table, long rows) {
        return "SELECT " + quoted(SeedRows.ID) + " FROM " + quoted(table)
                + " ORDER BY " + quoted(SeedRows.ID) + " LIMIT " + rows;
    }

    /**
     * Re-emits every current row as a delete followed by an insert of the same values, in one
     * transaction. Logical decoding is positional in the same way a binary log is: a row written before
     * the product's stream is positioned is never delivered, and nothing observable says when
     * positioning is done. Re-emitting the whole table under existing ids is idempotent at any upserting
     * target - rows that already crossed are overwritten with themselves - so it is safe to do on
     * nothing more than suspicion of a stall.
     *
     * <p>Taking {@link Endpoints}' no-op default instead would compile and read as though this store
     * simply had nothing to re-emit, while quietly removing the one recovery a stalled cross-engine case
     * has. It is overridden here for the same reason MySQL overrides it, not because the two dialects
     * differ.
     */
    @Override
    public void redeliver(EndpointAddress address, String table) {
        Connection connection = connection(address);
        if (!exists(connection, address, table)) {
            return;
        }
        try {
            connection.setAutoCommit(false);
            List<Map<String, Object>> rows = new ArrayList<>();
            try (Statement statement = connection.createStatement();
                    ResultSet current = statement.executeQuery(
                            "SELECT * FROM " + quoted(table) + " ORDER BY " + quoted(SeedRows.ID))) {
                while (current.next()) {
                    rows.add(rowOf(current));
                }
            }
            if (!rows.isEmpty()) {
                try (PreparedStatement delete = connection.prepareStatement(
                        "DELETE FROM " + quoted(table) + " WHERE " + quoted(SeedRows.ID) + " = ?")) {
                    for (Map<String, Object> row : rows) {
                        delete.setObject(1, row.get(SeedRows.ID));
                        delete.executeUpdate();
                    }
                }
                insertRows(connection, table, rows);
            }
            connection.commit();
        } catch (SQLException e) {
            rollback(connection);
            throw MySqlEndpoints.withAutoCommitRestored(
                    connection, new EnvelopeException("cannot re-emit the rows of " + table, e));
        } catch (RuntimeException e) {
            // The insert helper throws its own diagnosis; the transaction must still come back.
            rollback(connection);
            throw MySqlEndpoints.withAutoCommitRestored(connection, e);
        }
        // Only on the way out without a failure: here a connection that will not commit again is the
        // whole of what went wrong, so it is the diagnosis rather than a footnote to another one.
        restoreAutoCommit(connection);
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
     *
     * <p>Matched on the schema rather than on the database the way MySQL does. The two are not the same
     * thing here: this connection already reaches exactly one database, and within it a table lands in
     * the default schema, so that is what identifies it.
     */
    private boolean exists(Connection connection, EndpointAddress address, String table) {
        String sql = "SELECT COUNT(*) FROM information_schema.tables "
                + "WHERE table_schema = ? AND table_name = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, SCHEMA);
            statement.setString(2, table);
            try (ResultSet results = statement.executeQuery()) {
                return results.next() && results.getLong(1) > 0;
            }
        } catch (SQLException e) {
            throw new EnvelopeException("cannot look up " + table + " at " + address.text(HOST), e);
        }
    }

    /**
     * Appends rows of the generated shape, and refuses any other shape by name.
     *
     * <p>A table seeded with explicit values carries whatever columns those rows carried, and there is
     * no honest value this can put in them - inventing one would make a document assertion pass against
     * data no specification described. Left to the database the refusal arrives as "unknown column",
     * which reads as a fault in the store rather than as a combination the vocabulary does not yet say.
     */
    private void insertRange(Connection connection, String table, long firstId, long rows) {
        if (rows <= 0) {
            return;
        }
        List<String> columns = columnsOf(connection, table);
        if (!columns.equals(SeedRows.generatedColumns())) {
            throw new EnvelopeException(
                    "cannot insert into " + table + ": an inserted row is the generated shape "
                            + SeedRows.generatedColumns() + ", and this table carries " + columns
                            + "; seed it with a row count, or make the change with update or delete");
        }
        String sql = "INSERT INTO " + quoted(table) + " (" + quoted(SeedRows.ID) + ", "
                + quoted(SeedRows.SEQ) + ") VALUES (?, ?)";
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

    /**
     * Refuses an update of a table that has no sequence to rewrite, by name.
     *
     * <p>An update is spelled as a rewrite of {@code seq}, which is the generated shape's second column
     * and need not exist in a table seeded with explicit values. Left to the database the refusal
     * arrives as "unknown column", which reads as a fault in the store rather than as a word this
     * table cannot be asked. A delete needs only the id every row carries, so it is not held to this.
     */
    private void requireSequence(Connection connection, String table) {
        List<String> columns = columnsOf(connection, table);
        if (!columns.contains(SeedRows.SEQ)) {
            throw new EnvelopeException(
                    "cannot update " + table + ": an update rewrites the column " + SeedRows.SEQ
                            + ", and this table carries " + columns
                            + "; seed it with a row count, or make the change with delete");
        }
    }

    /** The columns the table carries now, in the order it declares them. */
    private List<String> columnsOf(Connection connection, String table) {
        try (Statement statement = connection.createStatement();
                ResultSet empty = statement.executeQuery("SELECT * FROM " + quoted(table) + " LIMIT 0")) {
            var metadata = empty.getMetaData();
            List<String> columns = new ArrayList<>();
            for (int column = 1; column <= metadata.getColumnCount(); column++) {
                columns.add(metadata.getColumnLabel(column));
            }
            return columns;
        } catch (SQLException e) {
            throw new EnvelopeException("cannot read the columns of " + table, e);
        }
    }

    private long highestId(Connection connection, String table) {
        try (Statement statement = connection.createStatement();
                ResultSet results = statement.executeQuery(
                        "SELECT MAX(" + quoted(SeedRows.ID) + ") FROM " + quoted(table))) {
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
        String url = "jdbc:postgresql://" + address.text(HOST) + ":" + address.text(PORT) + "/"
                + address.text(DATABASE);
        return connectionsByUrl.computeIfAbsent(url, key -> {
            try {
                return DriverManager.getConnection(key, account(address), address.text(PASSWORD));
            } catch (SQLException e) {
                throw new EnvelopeException("cannot reach the endpoint at " + key, e);
            }
        });
    }

    /** The account this address carries, under whichever of the two names it uses. */
    private static String account(EndpointAddress address) {
        return address.settings().containsKey(USER) ? address.text(USER) : address.text(USERNAME);
    }

    /**
     * Guards an identifier that collides with a reserved word; specifications choose table names.
     *
     * <p>Double quotes rather than backticks, and that is more than a spelling difference: an unquoted
     * identifier here is folded to lower case, so a table a specification named in mixed case would be
     * created under one name and looked up under another.
     */
    private static String quoted(String identifier) {
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }
}
