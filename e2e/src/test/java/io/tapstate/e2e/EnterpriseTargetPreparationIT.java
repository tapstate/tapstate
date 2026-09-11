package io.tapstate.e2e;

import io.tapstate.adapters.pdk.ConnectorIntrospector;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.adapters.pdk.PdkSinkPort;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.*;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.testsupport.DockerGate;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Real enterprise target readback for preparation policies, writer reopening, and upsert keys.
 * The declarative vocabulary cannot assert a rejected write, inspect physical indexes, or reopen
 * the sink with an in-memory receipt while deliberately withholding a source checkpoint.
 * This exercises writer reopening, not process-crash durability. Those controls make
 * this a Java integration test; the ordinary cross-database pipeline remains a published example.
 */
@RequiresDocker
class EnterpriseTargetPreparationIT {
    private static final PipelineNode NODE = new PipelineNode("preparation", "sink");
    private static final Map<String, ConnectorRef> connectors = new HashMap<>();

    @BeforeAll
    static void requireRealTarget() throws Exception {
        DockerGate.require();
        RealConnectorGate.require("oracle", "sqlserver");
        for (String id : List.of("oracle", "sqlserver")) {
            Path jar;
            try (var paths = Files.list(Path.of(System.getProperty("tapstate.e2e.connectors-dir")))) {
                jar = paths.filter(path -> path.getFileName().toString().startsWith(id)
                        && path.toString().endsWith(".jar")).findFirst().orElseThrow();
            }
            var inspected = new ConnectorIntrospector().introspect(List.of(jar));
            connectors.put(id, new ConnectorRef(List.of(jar), inspected.className(), inspected.pdkApiVersion(),
                    null, inspected.spec()));
        }
    }

    enum Database {
        ORACLE("oracle", new OracleEndpoints()), SQLSERVER("sqlserver", new SqlServerEndpoints());
        final String connector;
        final EnterpriseJdbcEndpoints endpoints;
        Database(String connector, EnterpriseJdbcEndpoints endpoints) {
            this.connector = connector;
            this.endpoints = endpoints;
        }
        Map<String, Object> settings(String suffix) {
            return this == ORACLE ? SharedOracle.settings("target_" + suffix)
                    : SharedSqlServer.settings("target_" + suffix);
        }
        Connection connect(Map<String, Object> settings) throws Exception {
            return endpoints.connect(new EndpointAddress("target", settings));
        }
        String table(Map<String, Object> settings) {
            return endpoints.table(new EndpointAddress("target", settings), "orders");
        }
    }

    @ParameterizedTest
    @EnumSource(Database.class)
    void existingRowsHonorClearAppendAndFail(Database db) throws Exception {
        for (OnFullLoad policy : OnFullLoad.values()) {
            Map<String, Object> settings = db.settings("policy_" + policy.name().toLowerCase());
            try (Connection connection = db.connect(settings)) {
                seed(db, settings, connection, true, true);
                State state = new State();
                if (policy == OnFullLoad.FAIL) {
                    assertThatThrownBy(() -> write(db, settings, state, policy, true, target(true), 1, 10))
                            .hasStackTraceContaining("orders").hasStackTraceContaining("not empty")
                            .hasStackTraceContaining(TapstateException.class.getName());
                    assertThat(rows(db, settings, connection)).containsExactlyEntriesOf(Map.of(99L, 99L));
                } else {
                    write(db, settings, state, policy, true, target(true), 1, 10);
                    assertThat(rows(db, settings, connection)).containsEntry(1L, 10L);
                    assertThat(rows(db, settings, connection).containsKey(99L)).isEqualTo(policy == OnFullLoad.APPEND);
                }
            }
        }
    }

    @ParameterizedTest
    @EnumSource(Database.class)
    void createsMissingTablesWithRecommendedNumericTypesAndOnePrimaryKey(Database db) throws Exception {
        for (OnFullLoad policy : OnFullLoad.values()) {
            Map<String, Object> settings = db.settings("created_" + policy.name().toLowerCase());
            try (Connection connection = db.connect(settings)) {
                State state = new State();
                write(db, settings, state, policy, true, target(true), 1, 10);
                write(db, settings, state, policy, true, target(true), 2, 20);
                assertThat(rows(db, settings, connection)).containsExactlyInAnyOrderEntriesOf(Map.of(1L, 10L, 2L, 20L));
                try (var statement = connection.createStatement(); var result = statement.executeQuery(
                        "SELECT * FROM " + db.table(settings))) {
                    var metadata = result.getMetaData();
                    assertThat(metadata.getColumnTypeName(1).toUpperCase(java.util.Locale.ROOT))
                            .isIn("NUMBER", "NUMERIC", "DECIMAL", "BIGINT");
                    assertThat(metadata.getPrecision(1)).isGreaterThanOrEqualTo(19);
                    assertThat(metadata.getScale(1)).isZero();
                }
                assertUniqueKey(db, settings, connection);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(Database.class)
    void failAcceptsAnExistingEmptyTable(Database db) throws Exception {
        Map<String, Object> settings = db.settings("empty");
        try (Connection connection = db.connect(settings)) {
            seed(db, settings, connection, true, false);
            write(db, settings, new State(), OnFullLoad.FAIL, true, target(true), 1, 10);
            assertThat(rows(db, settings, connection)).containsExactlyEntriesOf(Map.of(1L, 10L));
        }
    }

    @ParameterizedTest
    @EnumSource(Database.class)
    void recoveryCdcOnlyAndAppendRerunsDoNotClearDeliveredRows(Database db) throws Exception {
        Map<String, Object> settings = db.settings("resume");
        try (Connection connection = db.connect(settings)) {
            seed(db, settings, connection, true, true);
            State state = new State();
            write(db, settings, state, OnFullLoad.CLEAR, true, target(true), 1, 10);
            assertThat(rows(db, settings, connection)).containsExactlyEntriesOf(Map.of(1L, 10L));
            write(db, settings, state, OnFullLoad.CLEAR, false, target(true), 2, 20);
            write(db, settings, state, OnFullLoad.CLEAR, true, target(true), 3, 30);
            state.dropNamespace(SinkPreparationNamespace.of(NODE));
            write(db, settings, state, OnFullLoad.APPEND, true, target(true), 4, 40);
            write(db, settings, new State(), OnFullLoad.CLEAR, false, target(true), 5, 50);
            assertThat(rows(db, settings, connection)).containsExactlyInAnyOrderEntriesOf(
                    Map.of(1L, 10L, 2L, 20L, 3L, 30L, 4L, 40L, 5L, 50L));
        }
    }

    @ParameterizedTest
    @EnumSource(Database.class)
    void addsAUniqueKeyAndRepeatedUpsertsConverge(Database db) throws Exception {
        Map<String, Object> settings = db.settings("index");
        try (Connection connection = db.connect(settings)) {
            seed(db, settings, connection, false, true);
            State state = new State();
            write(db, settings, state, OnFullLoad.APPEND, true, target(true), 1, 10);
            write(db, settings, state, OnFullLoad.APPEND, true, target(true), 1, 20);
            assertThat(rows(db, settings, connection)).containsExactlyInAnyOrderEntriesOf(Map.of(99L, 99L, 1L, 20L));
            assertUniqueKey(db, settings, connection);
        }
    }

    @ParameterizedTest
    @EnumSource(Database.class)
    void recommendedTypesRoundTripBasicTextNullAndTimestamp(Database db) throws Exception {
        Map<String, Object> settings = db.settings("types");
        TargetTable typed = new TargetTable("orders", List.of(
                new TargetField("id", "source_integer", true, TapstateType.INT64),
                new TargetField("label", "source_text", false, TapstateType.STRING),
                new TargetField("optional_text", "source_text", false, TapstateType.STRING),
                new TargetField("occurred", "source_timestamp", false, TapstateType.DATETIME)),
                List.of(new TargetIndex(List.of("id"), true)));
        var instant = java.sql.Timestamp.valueOf("2026-09-11 03:04:05");
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", 1L);
        row.put("label", "target text");
        row.put("optional_text", null);
        row.put("occurred", instant);
        try (SinkWriter writer = new PdkSinkPort(connectors::get, new State()).open(new SinkConfig(
                db.connector, settings, WriteMode.UPSERT, DdlPolicy.FAIL, typed, NODE, OnFullLoad.FAIL, true))) {
            assertThat(writer.write(List.of(Envelope.insert(1L, "orders", row, null)))
                    .toCompletableFuture().get(30, TimeUnit.SECONDS).written()).isEqualTo(1);
        }
        try (Connection connection = db.connect(settings); var statement = connection.createStatement();
             var result = statement.executeQuery("SELECT \"label\", \"optional_text\", \"occurred\" FROM "
                     + db.table(settings))) {
            assertThat(result.next()).isTrue();
            assertThat(result.getString(1)).isEqualTo("target text");
            assertThat(result.getObject(2)).isNull();
            assertThat(result.getTimestamp(3)).isEqualTo(instant);
            assertThat(result.getMetaData().isNullable(2)).isEqualTo(java.sql.ResultSetMetaData.columnNullable);
            System.out.printf("%s target types: text=%s timestamp=%s%n", db,
                    result.getMetaData().getColumnTypeName(1), result.getMetaData().getColumnTypeName(3));
            assertThat(result.next()).isFalse();
        }
    }

    @ParameterizedTest
    @EnumSource(Database.class)
    void recommendedDecimalTypePreservesFractionalValues(Database db) throws Exception {
        Map<String, Object> settings = db.settings("decimal");
        TargetTable typed = new TargetTable("orders", List.of(
                new TargetField("id", "source_integer", true, TapstateType.INT64),
                new TargetField("amount", "DECIMAL(18,4)", false, TapstateType.DECIMAL,
                        new io.tapstate.core.common.NumericType(null, true, false, false,
                                new java.math.BigDecimal("-99999999999999.9999"),
                                new java.math.BigDecimal("99999999999999.9999"), 18, 4))),
                List.of(new TargetIndex(List.of("id"), true)));
        try (SinkWriter writer = new PdkSinkPort(connectors::get, new State()).open(new SinkConfig(
                db.connector, settings, WriteMode.UPSERT, DdlPolicy.FAIL, typed, NODE, OnFullLoad.FAIL, true))) {
            assertThat(writer.write(List.of(Envelope.insert(1L, "orders",
                    Map.of("id", 1L, "amount", new java.math.BigDecimal("123456789.1234")), null)))
                    .toCompletableFuture().get(30, TimeUnit.SECONDS).written()).isEqualTo(1);
        }
        try (Connection connection = db.connect(settings); var statement = connection.createStatement();
             var result = statement.executeQuery("SELECT \"amount\" FROM " + db.table(settings))) {
            assertThat(result.next()).isTrue();
            System.out.printf("%s decimal type=%s precision=%d scale=%d readback=%s%n", db,
                    result.getMetaData().getColumnTypeName(1), result.getMetaData().getPrecision(1),
                    result.getMetaData().getScale(1), result.getBigDecimal(1));
            assertThat(result.getBigDecimal(1)).isEqualByComparingTo("123456789.1234");
        }
    }

    private static void assertUniqueKey(Database db, Map<String, Object> settings, Connection connection) throws Exception {
        Map<String, java.util.SortedMap<Integer, String>> keys = new LinkedHashMap<>();
        if (db == Database.ORACLE) {
            try (var statement = connection.prepareStatement("SELECT i.INDEX_NAME, c.COLUMN_POSITION, c.COLUMN_NAME "
                    + "FROM ALL_INDEXES i JOIN ALL_IND_COLUMNS c "
                    + "ON c.INDEX_OWNER = i.OWNER AND c.INDEX_NAME = i.INDEX_NAME "
                    + "WHERE i.TABLE_OWNER = ? AND i.TABLE_NAME = 'orders' AND i.UNIQUENESS = 'UNIQUE'")) {
                statement.setString(1, settings.get("schema").toString());
                try (var result = statement.executeQuery()) {
                    while (result.next()) {
                        keys.computeIfAbsent(result.getString(1), ignored -> new java.util.TreeMap<>())
                                .put(result.getInt(2), result.getString(3));
                    }
                }
            }
        } else {
            try (var indexes = connection.getMetaData().getIndexInfo(connection.getCatalog(),
                    settings.get("schema").toString(), "orders", true, true)) {
                while (indexes.next()) {
                    if (indexes.getString("INDEX_NAME") != null) {
                        keys.computeIfAbsent(indexes.getString("INDEX_NAME"), ignored -> new java.util.TreeMap<>())
                                .put(indexes.getInt("ORDINAL_POSITION"), indexes.getString("COLUMN_NAME"));
                    }
                }
            }
        }
        assertThat(keys.values().stream().map(columns -> List.copyOf(columns.values())).toList())
                .as("exactly one unique key protects id alone, not a composite key containing id")
                .containsExactly(List.of("id"));
    }

    private static TargetTable target(boolean index) {
        return new TargetTable("orders", List.of(
                new TargetField("id", "source_integer", true, TapstateType.INT64),
                new TargetField("seq", "source_integer", false, TapstateType.INT64)),
                index ? List.of(new TargetIndex(List.of("id"), true)) : List.of());
    }

    private static void write(Database db, Map<String, Object> settings, State state, OnFullLoad policy,
            boolean fullLoad, TargetTable target, long id, long seq) throws Exception {
        try (SinkWriter writer = new PdkSinkPort(connectors::get, state).open(new SinkConfig(
                db.connector, settings, WriteMode.UPSERT, DdlPolicy.FAIL, target, NODE, policy, fullLoad))) {
            assertThat(writer.write(List.of(Envelope.insert(1L, "orders", Map.of("id", id, "seq", seq), null)))
                    .toCompletableFuture().get(30, TimeUnit.SECONDS).written()).isEqualTo(1);
        }
    }

    private static void seed(Database db, Map<String, Object> settings, Connection connection,
            boolean primaryKey, boolean populated) throws Exception {
        try (var statement = connection.createStatement()) {
            String type = db == Database.ORACLE ? "NUMBER(19)" : "BIGINT";
            statement.execute("CREATE TABLE " + db.table(settings) + " (\"id\" " + type
                    + (primaryKey ? " PRIMARY KEY" : "") + ", \"seq\" " + type + ")");
            if (populated) {
                statement.execute("INSERT INTO " + db.table(settings) + " VALUES (99, 99)");
            }
        }
    }

    private static Map<Long, Long> rows(Database db, Map<String, Object> settings, Connection connection) throws Exception {
        Map<Long, Long> result = new LinkedHashMap<>();
        try (var statement = connection.createStatement(); var rows = statement.executeQuery(
                "SELECT \"id\", \"seq\" FROM " + db.table(settings))) {
            while (rows.next()) {
                assertThat(result.put(rows.getLong(1), rows.getLong(2))).as("upsert must not duplicate a key").isNull();
            }
        }
        return result;
    }

    private static final class State implements KeyedStateStore {
        private final Map<String, byte[]> entries = new HashMap<>();
        public Optional<byte[]> load(String namespace, String key) { return Optional.ofNullable(entries.get(namespace + "/" + key)); }
        public void save(String namespace, String key, byte[] value) { entries.put(namespace + "/" + key, value); }
        public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] value) {
            return Optional.ofNullable(entries.putIfAbsent(namespace + "/" + key, value));
        }
        public void delete(String namespace, String key) { entries.remove(namespace + "/" + key); }
        public void dropNamespace(String namespace) { entries.keySet().removeIf(key -> key.startsWith(namespace + "/")); }
        public long count(String namespace) { return entries.keySet().stream().filter(key -> key.startsWith(namespace + "/")).count(); }
    }
}
