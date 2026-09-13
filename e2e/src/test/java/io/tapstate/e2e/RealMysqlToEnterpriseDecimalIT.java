package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.testsupport.DockerGate;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Real discovery, persisted schema, automatic DDL and CDC must preserve a decimal's full domain. */
@RequiresDocker
class RealMysqlToEnterpriseDecimalIT {
    private static final String TABLE = "orders";
    private static final String PIPELINE = "decimal_crossing";
    private static final Duration TIMEOUT = Duration.ofSeconds(90);

    @BeforeAll
    static void requireRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "oracle", "sqlserver");
    }

    static Stream<Arguments> targetsAndTiers() {
        return Stream.of("oracle", "sqlserver").flatMap(target -> Stream.of(Tiers.values())
                .map(tier -> Arguments.of(target, tier)));
    }

    @ParameterizedTest(name = "{0} / {1}")
    @MethodSource("targetsAndTiers")
    void persistedMysqlDecimalSchemaCreatesAnExactTargetAndCarriesUpdates(String target, Tiers tier)
            throws Exception {
        String suffix = target + "_" + tier.name().toLowerCase(Locale.ROOT) + "_"
                + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        Map<String, Object> sourceSettings = SharedMySql.settings("decimal_src_" + suffix);
        Map<String, Object> targetSettings = target.equals("oracle")
                ? SharedOracle.settings("decimal_tgt_" + suffix)
                : SharedSqlServer.settings("decimal_tgt_" + suffix);
        EnterpriseJdbcEndpoints reader = target.equals("oracle") ? new OracleEndpoints() : new SqlServerEndpoints();
        EndpointAddress address = new EndpointAddress("tgt_db", targetSettings);
        String store = SharedMongo.replicaSetUrl("decimal_store_" + suffix);
        try (reader; Connection source = SharedMySql.connect(sourceSettings);
                Connection destination = reader.connect(address)) {
            try (var statement = source.createStatement()) {
                statement.execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, amount DECIMAL(18,4) NOT NULL)");
            }
            insert(source, 1, "123456789.1234");
            insert(source, 2, "-0.0001");
            assertThat(tableExists(destination, address)).as("the product must create the target table").isFalse();

            try (ServerHandle server = tier.launch(store)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");
                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector(target, ConnectorJars.bytesFor(target));
                control.apply(Map.of("src_db.tap.yml", resource("src_db", "mysql", sourceSettings, true),
                        "tgt_db.tap.yml", resource("tgt_db", target, targetSettings, false)));
                control.discoverSchema("src_db", "mysql", sourceSettings);
                assertThat(control.sourceSchemaFields("src_db", TABLE)).contains("id", "amount");
            }
            // Reopen the same Mongo-backed store before applying the pipeline: an in-memory model
            // cannot carry precision/scale across this boundary on either fidelity tier.
            try (ServerHandle server = tier.launch(store)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.login("e2e", "e2e-password");
                assertThat(control.sourceSchemaFields("src_db", TABLE)).contains("id", "amount");
                control.apply(Map.of("src_db.tap.yml", resource("src_db", "mysql", sourceSettings, true),
                        "tgt_db.tap.yml", resource("tgt_db", target, targetSettings, false),
                        "pipeline.tap.yml", pipeline()));
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                awaitTable(destination, address, control);
                assertDecimalDomain(destination, address);
                List<String> primaryKey = primaryKey(destination, address);
                assertThat(primaryKey).as("automatic creation must preserve the source primary key").containsExactly("id");
                awaitRows(destination, reader.table(address, TABLE), control, Map.of(
                        1L, new BigDecimal("123456789.1234"), 2L, new BigDecimal("-0.0001")));
                Map<String, Object> originalKey = keyFor(destination, reader.table(address, TABLE), primaryKey, 1);

                // Exercise the full source precision, well beyond the smallmoney range.
                insert(source, 3, "99999999999999.9999");
                awaitRows(destination, reader.table(address, TABLE), control, Map.of(
                        1L, new BigDecimal("123456789.1234"), 2L, new BigDecimal("-0.0001"),
                        3L, new BigDecimal("99999999999999.9999")));
                try (var update = source.prepareStatement("UPDATE orders SET amount=? WHERE id=1")) {
                    update.setBigDecimal(1, new BigDecimal("-987654321.4321"));
                    assertThat(update.executeUpdate()).isEqualTo(1);
                }
                awaitRows(destination, reader.table(address, TABLE), control, Map.of(
                        1L, new BigDecimal("-987654321.4321"), 2L, new BigDecimal("-0.0001"),
                        3L, new BigDecimal("99999999999999.9999")));
                assertThat(primaryKey(destination, address)).isEqualTo(primaryKey);
                assertThat(keyFor(destination, reader.table(address, TABLE), primaryKey, 1)).isEqualTo(originalKey);
                assertDecimalDomain(destination, address);
            }
        }
    }

    private static String resource(String id, String connector, Map<String, Object> config, boolean source) {
        return "version: tapstate/v1\nkind: source\nid: " + id + "\nconnector: " + connector
                + "\nconfig: " + JsonWriter.write(config) + "\n" + (source ? "mode: cdc\ntables: [orders]\n" : "");
    }

    private static String pipeline() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: decimal_crossing
                source: src_db
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: decimal_rows
                    from: [orders]
                    type: map
                    fields: { id: "$id", amount: "$amount" }
                serve:
                  from: decimal_rows
                  sync:
                    - source: tgt_db
                """;
    }

    private static void insert(Connection source, long id, String amount) throws SQLException {
        try (var insert = source.prepareStatement("INSERT INTO orders (id, amount) VALUES (?, ?)")) {
            insert.setLong(1, id);
            insert.setBigDecimal(2, new BigDecimal(amount));
            assertThat(insert.executeUpdate()).isEqualTo(1);
        }
    }

    private static boolean tableExists(Connection connection, EndpointAddress address) throws SQLException {
        try (var tables = connection.getMetaData().getTables(
                connection.getCatalog(), address.text("schema"), TABLE, new String[]{"TABLE"})) {
            return tables.next();
        }
    }

    private static void awaitTable(Connection connection, EndpointAddress address, ControlPlane control) throws Exception {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (!tableExists(connection, address) && System.nanoTime() < deadline) {
            assertThat(control.failureCode(PIPELINE)).as("target creation must not fail in the product").isEmpty();
            Thread.sleep(250);
        }
        assertThat(tableExists(connection, address)).as("target table created from persisted discovery").isTrue();
    }

    private static void assertDecimalDomain(Connection connection, EndpointAddress address) throws SQLException {
        try (var columns = connection.getMetaData().getColumns(
                connection.getCatalog(), address.text("schema"), TABLE, "amount")) {
            assertThat(columns.next()).as("the discovered decimal column exists in the target").isTrue();
            int precision = columns.getInt("COLUMN_SIZE");
            int scale = columns.getInt("DECIMAL_DIGITS");
            assertThat(columns.getInt("DATA_TYPE")).as("the target uses exact decimal storage")
                    .isIn(Types.NUMERIC, Types.DECIMAL);
            assertThat(scale).as("the target preserves all four source fractional digits").isGreaterThanOrEqualTo(4);
            assertThat(precision - scale).as("the target preserves all fourteen source integer digits")
                    .isGreaterThanOrEqualTo(14);
        }
    }

    private static List<String> primaryKey(Connection connection, EndpointAddress address) throws SQLException {
        Map<Short, String> keys = new java.util.TreeMap<>();
        try (var columns = connection.getMetaData().getPrimaryKeys(
                connection.getCatalog(), address.text("schema"), TABLE)) {
            while (columns.next()) {
                keys.put(columns.getShort("KEY_SEQ"), columns.getString("COLUMN_NAME"));
            }
        }
        return new ArrayList<>(keys.values());
    }

    private static Map<String, Object> keyFor(Connection connection, String table, List<String> key, long id)
            throws SQLException {
        try (var query = connection.prepareStatement("SELECT * FROM " + table + " WHERE \"id\"=?")) {
            query.setLong(1, id);
            try (var result = query.executeQuery()) {
                assertThat(result.next()).isTrue();
                Map<String, Object> values = new LinkedHashMap<>();
                for (String column : key) {
                    values.put(column, result.getObject(column));
                }
                assertThat(result.next()).as("the update must not duplicate its key").isFalse();
                return values;
            }
        }
    }

    private static void awaitRows(Connection connection, String table, ControlPlane control,
            Map<Long, BigDecimal> expected) throws Exception {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        Map<Long, BigDecimal> actual;
        do {
            assertThat(control.failureCode(PIPELINE)).as("decimal delivery must not fail in the product").isEmpty();
            actual = new LinkedHashMap<>();
            try (var statement = connection.createStatement();
                    var rows = statement.executeQuery("SELECT \"id\", \"amount\" FROM " + table)) {
                while (rows.next()) {
                    assertThat(actual.put(rows.getLong(1), rows.getBigDecimal(2))).as("no duplicated source key").isNull();
                }
            }
            Map<Long, BigDecimal> observed = actual;
            if (observed.keySet().equals(expected.keySet()) && expected.entrySet().stream().allMatch(entry ->
                    observed.get(entry.getKey()) != null && observed.get(entry.getKey()).compareTo(entry.getValue()) == 0)) {
                return;
            }
            Thread.sleep(250);
        } while (System.nanoTime() < deadline);
        assertThat(actual).as("exact decimal values after snapshot or CDC; expected %s", expected).isEqualTo(expected);
    }
}
