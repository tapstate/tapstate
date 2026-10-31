package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
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
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A nest being served from the layer behind its memory says so, while it keeps running.
 *
 * <p>This is the third state the two words a pipeline publishes cannot express. {@code state} and
 * {@code error_count} together say running or failed; between them sits the case an operator most needs
 * told about and no reading reports: nothing has failed, everything is correct, and per-key state has
 * become a round trip on most events. That is a buffer backpressure cannot see - the queues stay short
 * and the throughput is whatever the storage sustains - so no other statistic moves when it happens.
 *
 * <p>What the assertions have to discriminate: asserting the run is alive is satisfied by a run that was
 * never under pressure at all, and asserting the alert alone would be satisfied by one that had died
 * screaming. Both are read, and the run is put under real pressure first - a budget at the smallest the
 * product accepts, against more roots than that - so the alert has something true to say.
 *
 * <p>Read out of the pipeline's own logs face rather than the process output, because that is where an
 * operator would look: an alert only visible to whoever can tail the server's stdout is an alert the
 * product does not actually publish.
 *
 * <p>Gated on Docker and on a directory of real connector jars, like its siblings. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=NestSaysWhenItIsServedFromTheColdLayerIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class NestSaysWhenItIsServedFromTheColdLayerIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(180);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final String PARENT_TABLE = "orders";
    private static final String CHILD_TABLE = "order_items";
    private static final String PIPELINE_ID = "alerting_orders";

    /** The code the product names this condition by; the alert is the code, not a sentence about it. */
    private static final String EXPECTED_CODE = "nest.state-served-from-cold-layer";

    /**
     * This invocation's pipeline id, which carries the tier so the two tiers do not share a nest's state,
     * and a base of its own so no two witnesses share one either.
     */
    private String pipelineId;

    /** The smallest budget the product accepts, against more roots than it - real pressure, not a hint. */
    private static final int MEMORY_BUDGET = 271;
    private static final int ROOTS = 700;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aRunServedFromTheColdLayerSaysSoAndKeepsGoing(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            seedMysql(mysql);
            grantReplication(mysql);

            String suffix = tier.name().toLowerCase(Locale.ROOT);
            pipelineId = PIPELINE_ID + "_" + suffix;
            String targetUri = SharedMongo.replicaSetUrl("alert_target_" + suffix);

            try (PreparedNativeStore storage = PreparedNativeStore.create(); ServerHandle server = tier.launch(storage);
                    MongoEndpoints mongo = new MongoEndpoints()) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                storage.loginAndAwaitBusiness(control);

                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

                Map<String, Object> mysqlConfig = mysqlConfig(mysql);
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put("src_orders.tap.yml", sourceYaml("src_orders", PARENT_TABLE, mysqlConfig));
                resources.put("src_items.tap.yml", sourceYaml("src_items", CHILD_TABLE, mysqlConfig));
                resources.put("tgt_mongo.tap.yml", targetYaml(targetUri));
                resources.put("pipeline.tap.yml", pipelineYaml(pipelineId));
                control.apply(resources);

                control.discoverSchema("src_orders", "mysql", mysqlConfig);
                control.discoverSchema("src_items", "mysql", mysqlConfig);

                control.lifecycle(pipelineId, LifecycleVerb.START);

                await(() -> mongo.documents(EndpointAddress.uri(targetUri), PARENT_TABLE).size() == ROOTS);
                await(() -> control.logs(pipelineId).contains(EXPECTED_CODE));

                assertThat(control.logs(pipelineId))
                        .as("the pipeline's own logs face, where an operator would look. A run under this "
                                + "much pressure has to say it is being served from behind its memory - "
                                + "nothing else moves when that happens.%n  metrics: %s",
                                control.metrics(pipelineId))
                        .contains(EXPECTED_CODE);

                // The other half: said without dying. An implementation that only reports at the point of
                // failure satisfies the assertion above only by failing, which these two forbid.
                assertThat(control.state(pipelineId))
                        .as("being served from the cold layer is a condition to report, not to stop on")
                        .contains(PipelineState.RUNNING);
                assertThat(control.errorCount(pipelineId))
                        .as("and not an error")
                        .contains(0L);
            }
        }
    }

    private static void await(BooleanSupplier reached) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (System.nanoTime() - deadline < 0) {
            if (reached.getAsBoolean()) {
                return;
            }
            sleep();
        }
    }

    /** More roots than the budget, one child each, so every root is worth reading back. */
    private static void seedMysql(MySQLContainer<?> mysql) throws Exception {
        try (Connection connection =
                DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(
                        "CREATE TABLE " + PARENT_TABLE + " (id INT PRIMARY KEY, name VARCHAR(64))");
                statement.execute("CREATE TABLE " + CHILD_TABLE
                        + " (id INT PRIMARY KEY, order_id INT, sku VARCHAR(64))");
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + PARENT_TABLE + " (id, name) VALUES (?, ?)")) {
                for (long id = 1; id <= ROOTS; id++) {
                    insert.setLong(1, id);
                    insert.setString(2, "order-" + id);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + CHILD_TABLE + " (id, order_id, sku) VALUES (?, ?, ?)")) {
                for (long id = 1; id <= ROOTS; id++) {
                    insert.setLong(1, id);
                    insert.setLong(2, id);
                    insert.setString(3, "sku-" + id);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }
    }

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

    private static String sourceYaml(String id, String table, Map<String, Object> config) {
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
                        id,
                        config.get("host"),
                        config.get("port"),
                        config.get("database"),
                        config.get("username"),
                        config.get("password"),
                        table);
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

    private static String pipelineYaml(String pipelineId) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: [ src_orders, src_items ]
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: order_doc
                    type: nest
                    entries_in_memory: %d
                    from: { o: orders, i: order_items }
                    root:
                      from: o
                      key: [ id ]
                      embed:
                        - { from: i, on: { order_id: id }, as: array, path: items, arrayKey: [ id ] }
                serve:
                  from: order_doc
                  sync:
                    - source: tgt_mongo
                """
                .formatted(pipelineId, MEMORY_BUDGET);
    }

    private static void sleep() {
        try {
            Thread.sleep(POLL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for the condition to be reported", e);
        }
    }
}
