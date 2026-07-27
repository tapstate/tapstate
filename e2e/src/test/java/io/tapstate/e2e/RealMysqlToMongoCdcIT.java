package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The witness that a real PDK connector carries the CDC / change-stream half across two databases.
 *
 * <p>The sibling {@link RealMysqlToMongoSnapshotIT} proves only the snapshot half: it seeds before
 * start and filters {@code op == 'r'}. This one drives the transition the first example was always
 * meant to show - seed a snapshot, start, then make real changes on the MySQL side AFTER start and
 * watch the binlog tail carry them to Mongo. The pipeline admits both a snapshot read ({@code op=r})
 * and a cdc insert ({@code op=i}), so the count crosses from the seeded N to N+M as the tail delivers.
 *
 * <p>Run on both fidelity tiers like the snapshot witness: embedded in this JVM, and against the
 * shipped boot jar in its own process - the connector loaded by the fat-jar the product ships, whose
 * manifest must carry the open its cglib config binding needs.
 *
 * <p>Gated on a directory of real connector jars ({@code -Dtapstate.e2e.connectors-dir}) and Docker,
 * exactly like the snapshot witness: naming no directory skips, naming one whose jars do not resolve
 * fails rather than skips. Needs binlog CDC on the source, so it grants the connector's user MySQL
 * replication privileges (mysql:8 has binlog/ROW/server-id on by default).
 */
class RealMysqlToMongoCdcIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final Duration TAIL_SETTLE = Duration.ofSeconds(3);
    private static final long SEEDED_ROWS = 5;
    private static final long CDC_ROWS = 3;
    private static final String TABLE = "orders";
    private static final String PIPELINE_ID = "mysql2mongo";

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void realMysqlCdcRowsReachRealMongo(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            seedMysqlOrders(mysql, SEEDED_ROWS);
            grantReplication(mysql);

            // One store and one target per tier: sharing them would let a later tier read the rows an
            // earlier one already landed and pass on the first poll without the connector writing a thing.
            String suffix = tier.name().toLowerCase(Locale.ROOT);
            String storeUri = SharedMongo.replicaSetUrl("real_cdc_store_" + suffix);
            String targetUri = SharedMongo.replicaSetUrl("real_cdc_target_" + suffix);

            try (ServerHandle server = tier.launch(storeUri);
                    MongoEndpoints mongo = new MongoEndpoints()) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");

                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

                Map<String, Object> mysqlConfig = mysqlConfig(mysql);
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put("src_mysql.tap.yml", sourceYaml(mysqlConfig));
                resources.put("tgt_mongo.tap.yml", targetYaml(targetUri));
                resources.put("pipeline.tap.yml", pipelineYaml());
                control.apply(resources);

                control.discoverSchema("src_mysql", "mysql", mysqlConfig);
                control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

                // Snapshot half: the seeded rows must arrive before any change is made.
                awaitCount(mongo, targetUri, SEEDED_ROWS);

                // Let the binlog tail position at the current end before changing anything - a null resume
                // offset starts the stream from now, so an insert that races the tail bring-up is missed.
                Thread.sleep(TAIL_SETTLE.toMillis());

                // CDC half: real inserts made AFTER start must reach Mongo through the binlog tail.
                insertMoreOrders(mysql, SEEDED_ROWS + 1, CDC_ROWS);
                awaitCount(mongo, targetUri, SEEDED_ROWS + CDC_ROWS);
            }
        }
    }

    private static void awaitCount(MongoEndpoints mongo, String targetUri, long expected) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        long last = -1;
        while (System.nanoTime() - deadline < 0) {
            last = mongo.count(EndpointAddress.uri(targetUri), TABLE);
            if (last == expected) {
                return;
            }
            sleep();
        }
        assertThat(last)
                .as("rows in the Mongo target %s (expected snapshot %d + cdc %d)", TABLE, SEEDED_ROWS, CDC_ROWS)
                .isEqualTo(expected);
    }

    private static void seedMysqlOrders(MySQLContainer<?> mysql, long rows) throws Exception {
        try (Connection connection =
                DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, name VARCHAR(64))");
            }
            insertOrders(connection, 1, rows);
        }
    }

    /** Inserts {@code rows} more orders starting at {@code firstId}, on a fresh connection, no CREATE. */
    private static void insertMoreOrders(MySQLContainer<?> mysql, long firstId, long rows) throws Exception {
        try (Connection connection =
                DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
            insertOrders(connection, firstId, rows);
        }
    }

    private static void insertOrders(Connection connection, long firstId, long rows) throws Exception {
        try (PreparedStatement insert =
                connection.prepareStatement("INSERT INTO " + TABLE + " (id, name) VALUES (?, ?)")) {
            for (long id = firstId; id < firstId + rows; id++) {
                insert.setLong(1, id);
                insert.setString(2, "order-" + id);
                insert.addBatch();
            }
            insert.executeBatch();
        }
    }

    /** Binlog CDC needs replication privileges the default test user lacks; grant them as root. */
    private static void grantReplication(MySQLContainer<?> mysql) throws Exception {
        try (Connection root = DriverManager.getConnection(mysql.getJdbcUrl(), "root", mysql.getPassword());
                Statement statement = root.createStatement()) {
            statement.execute("GRANT REPLICATION SLAVE, REPLICATION CLIENT, RELOAD, SELECT ON *.* TO '"
                    + mysql.getUsername() + "'@'%'");
            statement.execute("FLUSH PRIVILEGES");
        }
    }

    private static Map<String, Object> mysqlConfig(MySQLContainer<?> mysql) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("host", mysql.getHost());
        config.put("port", mysql.getMappedPort(MySQLContainer.MYSQL_PORT));
        config.put("database", mysql.getDatabaseName());
        config.put("username", mysql.getUsername());
        config.put("password", mysql.getPassword());
        return config;
    }

    private static String sourceYaml(Map<String, Object> config) {
        return """
                version: tapstate/v1
                kind: source
                id: src_mysql
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ orders ]
                """
                .formatted(
                        config.get("host"),
                        config.get("port"),
                        config.get("database"),
                        config.get("username"),
                        config.get("password"));
    }

    private static String targetYaml(String targetUri) {
        return """
                version: tapstate/v1
                kind: source
                id: tgt_mongo
                connector: mongodb
                config: { uri: "%s" }
                """
                .formatted(targetUri);
    }

    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: mysql2mongo
                source: src_mysql
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: rows_through, from: [orders], type: filter, expr: "op == 'r' || op == 'i'" }
                serve:
                  from: rows_through
                  sync:
                    - source: tgt_mongo
                """;
    }

    private static void sleep() {
        try {
            Thread.sleep(POLL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for the cdc rows to reach Mongo", e);
        }
    }
}
