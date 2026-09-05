package io.tapstate.e2e;

import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.event.Op;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.JoinTree;
import io.tapstate.core.sql.OutputField;
import io.tapstate.core.sql.SourceColumn;
import io.tapstate.core.sql.SourceTable;
import io.tapstate.core.sql.SqlFrontEnd;
import io.tapstate.core.sql.Unsupported;
import io.tapstate.runtime.engine.join.JoinExecutor;
import io.tapstate.runtime.engine.join.JoinSink;
import io.tapstate.runtime.engine.join.SourceChange;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.BiFunction;

/**
 * The reference answer machine: a source database, one join SQL, a sequence of changes, and the set of
 * ways a carrier's output differs from what that database itself answers to the same text.
 *
 * <p><b>Why the answer comes from the database rather than from a second implementation of the join.</b>
 * The risk this exists for is the half of a join nothing else covers - the incremental one, written
 * here rather than taken off the shelf - and every way of getting it wrong publishes rows that look
 * entirely ordinary: a row left behind after its key moved, a fact row never re-sent after its
 * dimension row changed, a null key matched as if it were a value. None of them reports anything, and
 * a hand-written expectation only covers the shapes whoever wrote it thought of. A real database
 * executing the same text shares none of this code and none of its assumptions, so where the two
 * disagree, one of them is wrong for a reason nobody had to anticipate.
 *
 * <p><b>It takes a {@link JoinExecutor}, so it is every carrier's acceptance surface rather than one
 * carrier's self-test.</b> A second carrier is held to this by being passed to it; nothing here names
 * the one this product ships, and nothing here is allowed to.
 *
 * <p><b>What the changelog is folded on.</b> A carrier publishes changes, not a table, so turning them
 * back into rows needs to know what identifies a row - which is the target table's key, and is not
 * derivable from the SQL. It is named by the caller, and every column named has to be projected: a
 * fold on something the rows do not carry silently collapses them into one.
 *
 * <p><b>What it cannot see, named rather than left to be discovered.</b> Values are compared after
 * normalisation, so that the two sides' spellings of one value - a scale of trailing zeros, a
 * {@code TINYINT(1)} against a boolean - do not read as disagreements. Numbers and booleans normalise
 * onto one another, so a carrier that answered {@code true} where the database answered {@code 1} would
 * pass here. Type identity is not this instrument's question; row content is. What it is sharp about is
 * rows: one missing, one surplus, or one holding the wrong values.
 */
final class JoinConformance implements AutoCloseable {

    /** A carrier that never says it is finished is a defect, not a reason to hang a suite. */
    private static final int DRAIN_LIMIT = 100_000;

    private final Connection db;
    private final String sql;
    private final List<String> identityColumns;
    private final JoinExecutor executor;
    private final JoinPlan plan;
    private final Map<String, String> sourceByTable;
    private final Map<String, List<String>> keyColumnsByTable;

    /** The published rows, folded as they are emitted rather than kept as a changelog. */
    private final Map<List<Object>, Map<String, Object>> published = new LinkedHashMap<>();

    private final JoinSink sink = new JoinSink() {
        @Override
        public boolean offer(Envelope change) {
            fold(change);
            return true;
        }
    };

    private JoinConformance(Connection db, String sql, List<String> identityColumns,
            JoinPlan plan, Map<String, String> sourceByTable,
            Map<String, List<String>> keyColumnsByTable, JoinExecutor executor) {
        this.db = db;
        this.sql = sql;
        this.identityColumns = identityColumns;
        this.plan = plan;
        this.sourceByTable = sourceByTable;
        this.keyColumnsByTable = keyColumnsByTable;
        this.executor = executor;
    }

    /**
     * Compiles {@code sql} against the schema {@code db} reports and opens {@code carrier} on the plan.
     *
     * @param identityColumns the output columns that identify one published row
     * @param carrier         built from the driving source's own key columns and the stream name, both
     *                        of which come from the database rather than from the SQL - which is why
     *                        this takes a factory rather than a carrier already made
     */
    static JoinConformance of(Connection db, String sql, List<String> identityColumns,
            BiFunction<List<String>, String, JoinExecutor> carrier) throws SQLException {
        Optional<Unsupported> refused = SqlFrontEnd.unsupported(sql);
        if (refused.isPresent()) {
            // Comparing a shape the product refuses would be measuring something it never runs.
            throw new IllegalArgumentException(
                    "this SQL is not one the product accepts: " + refused.get().shape());
        }
        Map<String, List<String>> keys = new LinkedHashMap<>();
        List<SourceTable> tables = discover(db, keys);
        JoinPlan plan;
        try {
            plan = SqlFrontEnd.derive(sql, tables);
        } catch (RuntimeException e) {
            // The front end reports the SQL, because the SQL is all it was given. When the schema is
            // the thing that is wrong, that message sends a reader to a statement which is correct -
            // so the schema this was actually derived against is named here, where it is known.
            throw new IllegalArgumentException("deriving a plan from this SQL failed against the "
                    + "schema discovered from " + db.getCatalog() + ": " + describe(tables), e);
        }

        Map<String, String> sourceByTable = new LinkedHashMap<>();
        for (JoinTree.Source source : plan.from().sources()) {
            String table = source.table().toLowerCase(java.util.Locale.ROOT);
            if (sourceByTable.put(table, source.name()) != null) {
                // Two aliases over one table: a change to that table belongs to both, and nothing in
                // the change says which. Refused rather than guessed.
                throw new IllegalArgumentException(
                        "this SQL reads " + table + " under more than one name, which a change cannot "
                                + "be attributed to");
            }
        }
        Set<String> projected = new LinkedHashSet<>();
        for (OutputField field : plan.outputFields()) {
            projected.add(field.name());
        }
        if (!projected.containsAll(identityColumns)) {
            throw new IllegalArgumentException("the SQL does not project every identity column: "
                    + identityColumns + " against " + projected);
        }

        String factTable = plan.factSource().table().toLowerCase(java.util.Locale.ROOT);
        List<String> factKeyColumns = keys.get(factTable);
        if (factKeyColumns == null) {
            // Naming what was discovered rather than only what was missing: the failure this replaces
            // reported a column the schema does have, which sends a reader to the SQL.
            throw new IllegalArgumentException("the source database holds no table " + factTable
                    + "; it holds " + keys.keySet());
        }
        if (factKeyColumns == null || factKeyColumns.isEmpty()) {
            throw new IllegalArgumentException(
                    "the driving table " + factTable + " declares no key, so its rows have no identity");
        }
        JoinExecutor executor = carrier.apply(factKeyColumns, "conformance");
        executor.open(plan);
        return new JoinConformance(db, sql, List.copyOf(identityColumns), plan, sourceByTable, keys,
                executor);
    }

    /** The plan the front end derived, for a case that wants to say what it is comparing against. */
    JoinPlan plan() {
        return plan;
    }

    /**
     * Writes one row to {@code table} and hands the carrier the same change.
     *
     * <p>Both images are read back out of the database rather than assembled here, which is what a
     * change data capture reader would deliver and is the only way the carrier sees the columns the
     * statement did not name.
     */
    void upsert(String table, Map<String, Object> row) throws SQLException {
        String name = table.toLowerCase(java.util.Locale.ROOT);
        List<String> key = keyOf(name);
        Map<String, Object> identity = new LinkedHashMap<>();
        for (String column : key) {
            if (!row.containsKey(column)) {
                throw new IllegalArgumentException("a row written to " + name + " has to carry its key "
                        + key + ", and this one does not: " + row.keySet());
            }
            identity.put(column, row.get(column));
        }
        Map<String, Object> before = read(name, identity);
        if (before == null) {
            insert(name, row);
        } else {
            update(name, row, identity);
        }
        Map<String, Object> after = read(name, identity);
        feed(name, before == null ? Envelope.insert(1L, name, after, null)
                : Envelope.update(1L, name, before, after, null));
    }

    /** Removes one row from {@code table} and hands the carrier the same change. */
    void delete(String table, Map<String, Object> identity) throws SQLException {
        String name = table.toLowerCase(java.util.Locale.ROOT);
        Map<String, Object> before = read(name, identity);
        if (before == null) {
            // Deleting what is not there changes neither side, so there is nothing to hand over.
            return;
        }
        StringBuilder statement = new StringBuilder("DELETE FROM `").append(name).append("` WHERE ");
        appendPredicate(statement, identity.keySet());
        try (PreparedStatement delete = db.prepareStatement(statement.toString())) {
            bind(delete, 1, identity.values());
            delete.executeUpdate();
        }
        feed(name, Envelope.delete(1L, name, before, null));
    }

    /**
     * Every way the carrier's rows differ from the database's own answer to the same SQL. Empty is the
     * only passing result; each entry names the identity it is about and both sides of it.
     */
    List<String> differences() throws SQLException {
        Map<List<Object>, Map<String, Object>> reference = new LinkedHashMap<>();
        List<String> found = new ArrayList<>();
        try (Statement query = db.createStatement(); ResultSet rows = query.executeQuery(sql)) {
            ResultSetMetaData columns = rows.getMetaData();
            while (rows.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int column = 1; column <= columns.getColumnCount(); column++) {
                    row.put(columns.getColumnLabel(column), rows.getObject(column));
                }
                List<Object> identity = identityOf(row);
                if (reference.put(identity, row) != null) {
                    // Two source rows under one identity: the fold cannot hold both, so saying so is
                    // the honest answer rather than reporting whichever one landed second.
                    found.add("the source answers two rows under identity " + identity
                            + ", which one published row cannot represent");
                }
            }
        }
        for (Map.Entry<List<Object>, Map<String, Object>> expected : reference.entrySet()) {
            Map<String, Object> actual = published.get(expected.getKey());
            if (actual == null) {
                found.add("missing " + expected.getKey() + ": the source has "
                        + normalised(expected.getValue()) + " and the carrier published nothing");
            } else if (!normalised(expected.getValue()).equals(normalised(actual))) {
                found.add("differs " + expected.getKey() + ": source " + normalised(expected.getValue())
                        + " against carrier " + normalised(actual));
            }
        }
        for (Map.Entry<List<Object>, Map<String, Object>> extra : published.entrySet()) {
            if (!reference.containsKey(extra.getKey())) {
                found.add("surplus " + extra.getKey() + ": the carrier published "
                        + normalised(extra.getValue()) + " and the source has no such row");
            }
        }
        return found;
    }

    @Override
    public void close() {
        executor.close();
    }

    /** Hands one change over and drains until the carrier says it has nothing left. */
    private void feed(String table, Envelope event) {
        String source = sourceByTable.get(table);
        if (source == null) {
            throw new IllegalArgumentException("this join reads no table called " + table);
        }
        List<SourceChange> changes = List.of(new SourceChange(source, event));
        for (int round = 0; !executor.apply(changes, sink); round++) {
            changes = List.of();
            if (round > DRAIN_LIMIT) {
                throw new IllegalStateException(
                        "the carrier has not finished after " + DRAIN_LIMIT + " drains");
            }
        }
    }

    private void fold(Envelope change) {
        if (change.op() == Op.DELETE) {
            published.remove(identityOf(change.before()));
            return;
        }
        published.put(identityOf(change.after()), change.after());
    }

    private List<Object> identityOf(Map<String, Object> row) {
        List<Object> identity = new ArrayList<>(identityColumns.size());
        for (String column : identityColumns) {
            identity.add(normalise(row.get(column)));
        }
        return Collections.unmodifiableList(identity);
    }

    private List<String> keyOf(String table) {
        List<String> key = keyColumnsByTable.get(table);
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException(table + " declares no key");
        }
        return key;
    }

    private Map<String, Object> read(String table, Map<String, Object> identity) throws SQLException {
        StringBuilder statement = new StringBuilder("SELECT * FROM `").append(table).append("` WHERE ");
        appendPredicate(statement, identity.keySet());
        try (PreparedStatement select = db.prepareStatement(statement.toString())) {
            bind(select, 1, identity.values());
            try (ResultSet rows = select.executeQuery()) {
                if (!rows.next()) {
                    return null;
                }
                ResultSetMetaData columns = rows.getMetaData();
                Map<String, Object> row = new LinkedHashMap<>();
                for (int column = 1; column <= columns.getColumnCount(); column++) {
                    row.put(columns.getColumnLabel(column), rows.getObject(column));
                }
                return row;
            }
        }
    }

    private void insert(String table, Map<String, Object> row) throws SQLException {
        StringBuilder statement = new StringBuilder("INSERT INTO `").append(table).append("` (");
        StringBuilder values = new StringBuilder(") VALUES (");
        boolean first = true;
        for (String column : row.keySet()) {
            if (!first) {
                statement.append(", ");
                values.append(", ");
            }
            statement.append('`').append(column).append('`');
            values.append('?');
            first = false;
        }
        try (PreparedStatement insert = db.prepareStatement(statement.append(values).append(')')
                .toString())) {
            bind(insert, 1, row.values());
            insert.executeUpdate();
        }
    }

    private void update(String table, Map<String, Object> row, Map<String, Object> identity)
            throws SQLException {
        StringBuilder statement = new StringBuilder("UPDATE `").append(table).append("` SET ");
        boolean first = true;
        for (String column : row.keySet()) {
            if (!first) {
                statement.append(", ");
            }
            statement.append('`').append(column).append("` = ?");
            first = false;
        }
        statement.append(" WHERE ");
        appendPredicate(statement, identity.keySet());
        try (PreparedStatement update = db.prepareStatement(statement.toString())) {
            int at = bind(update, 1, row.values());
            bind(update, at, identity.values());
            update.executeUpdate();
        }
    }

    private static void appendPredicate(StringBuilder statement, Set<String> columns) {
        boolean first = true;
        for (String column : columns) {
            if (!first) {
                statement.append(" AND ");
            }
            statement.append('`').append(column).append("` = ?");
            first = false;
        }
    }

    private static int bind(PreparedStatement statement, int from, Iterable<Object> values)
            throws SQLException {
        int at = from;
        for (Object value : values) {
            statement.setObject(at++, value);
        }
        return at;
    }

    /** Table by table, the columns discovery found - what a derive failure has to be read against. */
    private static String describe(List<SourceTable> tables) {
        StringBuilder described = new StringBuilder();
        for (SourceTable table : tables) {
            described.append(table.name()).append('(');
            for (int column = 0; column < table.columns().size(); column++) {
                if (column > 0) {
                    described.append(", ");
                }
                described.append(table.columns().get(column).name());
            }
            described.append(") ");
        }
        return described.toString().trim();
    }

    /** The tables of the connection's own catalog, and - into {@code keys} - each one's key columns. */
    private static List<SourceTable> discover(Connection db, Map<String, List<String>> keys)
            throws SQLException {
        DatabaseMetaData metadata = db.getMetaData();
        String catalog = db.getCatalog();
        Map<String, List<SourceColumn>> columnsByTable = new LinkedHashMap<>();
        try (ResultSet columns = metadata.getColumns(catalog, null, "%", "%")) {
            while (columns.next()) {
                String table = columns.getString("TABLE_NAME").toLowerCase(java.util.Locale.ROOT);
                columnsByTable.computeIfAbsent(table, any -> new ArrayList<>())
                        .add(new SourceColumn(columns.getString("COLUMN_NAME"),
                                typeOf(columns.getInt("DATA_TYPE")),
                                columns.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls));
            }
        }
        List<SourceTable> tables = new ArrayList<>();
        for (Map.Entry<String, List<SourceColumn>> table : columnsByTable.entrySet()) {
            tables.add(new SourceTable(table.getKey(), List.copyOf(table.getValue())));
            keys.put(table.getKey(), keyColumns(metadata, catalog, table.getKey()));
        }
        return tables;
    }

    /** One table's key columns, in the order the key declares them rather than alphabetically. */
    private static List<String> keyColumns(DatabaseMetaData metadata, String catalog, String table)
            throws SQLException {
        Map<Short, String> ordered = new TreeMap<>();
        try (ResultSet key = metadata.getPrimaryKeys(catalog, null, table)) {
            while (key.next()) {
                ordered.put(key.getShort("KEY_SEQ"), key.getString("COLUMN_NAME"));
            }
        }
        return List.copyOf(ordered.values());
    }

    private static TapstateType typeOf(int jdbcType) {
        return switch (jdbcType) {
            case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR,
                    Types.LONGNVARCHAR, Types.CLOB -> TapstateType.STRING;
            case Types.DECIMAL, Types.NUMERIC -> TapstateType.DECIMAL;
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT -> TapstateType.INT64;
            case Types.REAL, Types.FLOAT, Types.DOUBLE -> TapstateType.DOUBLE;
            case Types.BIT, Types.BOOLEAN -> TapstateType.BOOLEAN;
            case Types.DATE -> TapstateType.DATE;
            case Types.TIME -> TapstateType.TIME;
            case Types.TIMESTAMP -> TapstateType.DATETIME;
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> TapstateType.BINARY;
            default -> TapstateType.UNKNOWN;
        };
    }

    private static Map<String, Object> normalised(Map<String, Object> row) {
        Map<String, Object> normalised = new LinkedHashMap<>();
        for (Map.Entry<String, Object> column : row.entrySet()) {
            normalised.put(column.getKey(), normalise(column.getValue()));
        }
        return normalised;
    }

    /**
     * One value in a spelling both sides can be compared in. A number carries a marker so that it does
     * not compare equal to the text of itself; the two sides' numbers reach here from the same driver,
     * so what is bridged is the carrier's own coercion rather than any database's spelling.
     */
    private static Object normalise(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[] bytes) {
            return "#b" + Base64.getEncoder().encodeToString(bytes);
        }
        if (value instanceof Boolean flag) {
            return "#n" + (flag ? "1" : "0");
        }
        if (value instanceof BigDecimal decimal) {
            return "#n" + decimal.stripTrailingZeros().toPlainString();
        }
        if (value instanceof Number number) {
            return "#n" + new BigDecimal(number.toString()).stripTrailingZeros().toPlainString();
        }
        return Objects.toString(value);
    }
}
