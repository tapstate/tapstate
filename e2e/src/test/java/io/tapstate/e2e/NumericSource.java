package io.tapstate.e2e;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.testcontainers.containers.MySQLContainer;

/**
 * The one real numeric table the two type-gate witnesses are written against, and the workspace they
 * apply over it.
 *
 * <p>Shared because the pair only discriminates while the table is identical. One witness refuses
 * arithmetic over the decimal column and the other computes over the integral one and carries the
 * decimal through untouched; if each seeded its own table, a product that refused by table shape
 * rather than by column type would satisfy both and neither would notice. Here the only thing that
 * differs between them is the expression.
 */
final class NumericSource {

    static final String SOURCE_ID = "src_numeric";
    static final String TARGET_ID = "tgt_numeric";
    static final String TABLE = "amounts";

    /** Three rows, so a target that settles on the count settled on rows that were carried. */
    static final long ROWS = 3;

    /**
     * A value the column holds exactly and a binary float does not, ending in a digit only its scale
     * keeps. Anything that rounds it, and anything that drops its scale, reads back different.
     */
    static final BigDecimal AMOUNT = new BigDecimal("12345678.9010");

    /** Values chosen so doubling them cannot coincide with the value itself or with another row's. */
    static final List<Long> QUANTITIES = List.of(7L, 11L, 13L);

    private NumericSource() {
    }

    /**
     * One table carrying both numeric kinds: an integral column arithmetic survives, and an exact
     * fixed-point column it does not. Every row carries the same amount, so a reader that finds the
     * wrong one cannot have found it by picking the wrong row.
     */
    static void seed(MySQLContainer<?> mysql) throws Exception {
        try (Connection connection =
                DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
            seed(connection);
        }
    }

    /**
     * The same table in a database of the caller's own on the shared server, for a witness that reads
     * the table's changes: that server's user holds the replication grant a change-data-capture read
     * needs, and a container's default user does not.
     */
    static void seed(Map<String, Object> settings) throws Exception {
        try (Connection connection = SharedMySql.connect(settings)) {
            seed(connection);
        }
    }

    /** One writer for the table whichever server it lands on, so the pair cannot read two tables. */
    private static void seed(Connection connection) throws Exception {
        try (Statement statement = connection.createStatement()) {
            // A database on the shared server outlives the case that made it, so start from nothing.
            statement.execute("DROP TABLE IF EXISTS " + TABLE);
            statement.execute("CREATE TABLE " + TABLE
                    + " (id INT PRIMARY KEY, qty BIGINT, amount DECIMAL(18,4))");
        }
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO " + TABLE + " (id, qty, amount) VALUES (?, ?, ?)")) {
            for (int row = 0; row < QUANTITIES.size(); row++) {
                insert.setLong(1, row + 1L);
                insert.setLong(2, QUANTITIES.get(row));
                insert.setBigDecimal(3, AMOUNT);
                insert.addBatch();
            }
            insert.executeBatch();
        }
    }

    /** A logged-in control plane with both real connectors registered. */
    static ControlPlane connected(ServerHandle server) {
        ControlPlane control = new ControlPlane(server.baseUrl());
        control.bootstrapAndLogin("e2e", "e2e-password");
        control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
        control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
        return control;
    }

    /**
     * The connection settings the product hands the connector, keyed as its own spec names them. The
     * port is a number, not a string: the connector's config bean holds it as a numeric field, and
     * handing it a string is a cast failure inside the connector's config load.
     */
    static Map<String, Object> config(MySQLContainer<?> mysql) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("host", mysql.getHost());
        config.put("port", mysql.getMappedPort(MySQLContainer.MYSQL_PORT));
        config.put("database", mysql.getDatabaseName());
        config.put("username", mysql.getUsername());
        config.put("password", mysql.getPassword());
        return config;
    }

    /** Source, target and a pipeline whose map fields are the one thing that varies. */
    static Map<String, String> workspace(
            Map<String, Object> config, String targetUri, String pipelineId, String mapFields) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("src_numeric.tap.yml", sourceYaml(config));
        resources.put("tgt_numeric.tap.yml", targetYaml(targetUri));
        resources.put("pipeline.tap.yml", pipelineYaml(pipelineId, mapFields));
        return resources;
    }

    private static String sourceYaml(Map<String, Object> config) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ %s ]
                """
                .formatted(
                        SOURCE_ID,
                        config.get("host"),
                        config.get("port"),
                        config.get("database"),
                        config.get("username"),
                        config.get("password"),
                        TABLE);
    }

    private static String targetYaml(String targetUri) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s" }
                """
                .formatted(TARGET_ID, targetUri);
    }

    private static String pipelineYaml(String pipelineId, String mapFields) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: computed, from: [%s], type: map, fields: %s }
                serve:
                  from: computed
                  sync:
                    - source: %s
                """
                .formatted(pipelineId, SOURCE_ID, TABLE, mapFields, TARGET_ID);
    }
}
