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
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The witness that a real PDK connector moves real rows across two databases through the product.
 *
 * <p>The declarative examples move rows through a synthetic connector the harness builds; that proves
 * the product's chain but never a real connector's own behaviour - its schema discovery, its key, its
 * type mapping. This drives the shipped path with two real Tapdata connectors instead: real MySQL is
 * seeded over JDBC, the product discovers its model, derives the target model and key, creates the
 * Mongo collection and upserts into it, and the rows are counted back out of Mongo by a reader that is
 * not the product. It is the first time a real connector carries data end to end here - every earlier
 * "real chain" test faked the connector on both ends.
 *
 * <p>This witnesses the snapshot rows of a snapshot-and-CDC pipeline. The source needs replication
 * privileges to sample the binlog position before reading the table and to open the change stream;
 * the transform passes only snapshot events to the target. Delivery of later changes is witnessed by
 * the declarative example a-real-change-stream-carries-rows-written-after-start.
 *
 * <p>The crossing itself is also witnessed declaratively, by the example
 * real-mysql-rows-cross-to-a-real-mongo-target. What keeps this class is the timestamp claim below: a
 * seed spells its rows as strings and integers only, so a TIMESTAMP(3) column, the session time zone it
 * is read in, and an instant compared as an instant are all unsayable there. When the specification
 * grows those words this class has nothing left the example cannot say, and goes.
 *
 * <p>Run on both fidelity tiers. Embedded in this JVM, and - the one that matters here - against the
 * shipped boot jar in its own process: that is the connector loaded by the fat-jar the product actually
 * ships, whose manifest must carry the open its cglib config binding needs. The embedded tier gets that
 * open from the test fork instead, so it cannot witness a deliverable that omits it.
 *
 * <p>Gated on a directory of real connector jars ({@code -Dtapstate.e2e.connectors-dir}, the same
 * property the harness registers from) and on Docker for both databases. Naming no directory skips it,
 * so the default build stays green; naming one whose jars do not resolve fails rather than skips, so a
 * run meant to happen cannot pass by quietly not happening. The real-process tier launches the boot
 * jar, so the app module has to be built too ({@code -am}). Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors
 * </pre>
 */
class RealMysqlToMongoSnapshotIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(60);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final long SEEDED_ROWS = 5;
    private static final String TABLE = "orders";
    private static final String TARGET_COLLECTION = "player_address";
    private static final String PIPELINE_ID = "mysql2mongo";
    private static final String TIMESTAMP_COLUMN = "created_at";
    private static final Instant CREATED_AT = Instant.parse("2026-08-12T13:08:43.123Z");

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void realMysqlSnapshotRowsReachRealMongo(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            SharedMySql.grantReplication(mysql);
            seedMysqlOrders(mysql, SEEDED_ROWS);

            // One store and one target per tier: sharing them would let a later tier read the rows an
            // earlier one already landed and pass on the first poll without the connector writing a thing.
            String suffix = tier.name().toLowerCase(Locale.ROOT);
            String storeUri = SharedMongo.replicaSetUrl("real_mysql_store_" + suffix);
            String targetUri = SharedMongo.replicaSetUrl("real_mysql_target_" + suffix);

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

                // The target model and its key are read out of the source's own schema, so discovery has
                // to run before the sink is asked to create a collection and upsert into it.
                control.discoverSchema("src_mysql", "mysql", mysqlConfig);

                control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

                // Dialled as the uri this test already holds: the target is a store it created itself,
                // not a resource it applied and can be handed the settings of.
                EndpointAddress target = EndpointAddress.uri(targetUri);
                awaitCount(mongo, target, TARGET_COLLECTION, SEEDED_ROWS);
                assertThat(mongo.documents(target, TARGET_COLLECTION))
                        .as("the MySQL TIMESTAMP values read back from Mongo")
                        .allSatisfy(document -> assertThat(document.get(TIMESTAMP_COLUMN))
                                .isInstanceOf(Date.class)
                                .isEqualTo(Date.from(CREATED_AT)));
            }
        }
    }

    /** Reads the target the way a user would, from outside the product, until the rows are all there. */
    private static void awaitCount(
            MongoEndpoints mongo, EndpointAddress target, String collection, long expected) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        long last = -1;
        while (System.nanoTime() - deadline < 0) {
            last = mongo.count(target, collection);
            if (last == expected) {
                return;
            }
            sleep();
        }
        assertThat(last)
                .as("rows in the Mongo target %s after a snapshot of %d real MySQL rows", collection, expected)
                .isEqualTo(expected);
    }

    private static void seedMysqlOrders(MySQLContainer<?> mysql, long rows) throws Exception {
        try (Connection connection =
                DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("SET time_zone = '+00:00'");
                statement.execute("CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, name VARCHAR(64), "
                        + TIMESTAMP_COLUMN + " TIMESTAMP(3) NOT NULL)");
            }
            try (PreparedStatement insert =
                    connection.prepareStatement("INSERT INTO " + TABLE + " (id, name, " + TIMESTAMP_COLUMN
                            + ") VALUES (?, ?, ?)")) {
                for (long id = 1; id <= rows; id++) {
                    insert.setLong(1, id);
                    insert.setString(2, "order-" + id);
                    insert.setTimestamp(3, Timestamp.from(CREATED_AT), utcCalendar());
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }
    }

    private static Calendar utcCalendar() {
        return Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC));
    }

    /**
     * The connection settings the product hands the connector, keyed as its own spec names them. The port
     * is a number, not a string: the connector's config bean holds it as a numeric field, and handing it a
     * string is a cast failure inside the connector's config load.
     */
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
                  - { id: snapshot_rows, from: [orders], type: filter, expr: "op == 'r'" }
                serve:
                  from: snapshot_rows
                  sync:
                    - source: tgt_mongo
                      rename:
                        map: { orders: player_address }
                """;
    }

    private static void sleep() {
        try {
            Thread.sleep(POLL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for the snapshot to reach Mongo", e);
        }
    }
}
