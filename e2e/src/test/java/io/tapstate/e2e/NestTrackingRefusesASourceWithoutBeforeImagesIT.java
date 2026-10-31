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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A tree that follows structural key changes over a source configured without before images stops, and
 * the reading a user is shown says which end the problem is at.
 *
 * <p>Following a key change needs the row as it was: a row that moved and a row with an unrelated column
 * edited arrive looking identical - a row sitting where it now sits - and treating the first as the second
 * writes the element into its new place while leaving it in the old one, leaving a document that silently
 * disagrees with its source. So the run has to stop, and it does.
 *
 * <p><b>Where it stops is not where the tree's own refusal is.</b> The nest carries a check for an update
 * that arrives with no earlier row, but on MySQL it is never reached: the connector inspects the server's
 * configuration when it starts and refuses outright, so no update is ever produced to be checked. Measured
 * on {@code binlog_row_image=MINIMAL}, which is exactly the configuration the documentation names as
 * insufficient. This witness therefore pins the behaviour a user actually meets, not the one the tree
 * would apply to a source that got that far.
 *
 * <p>What the assertions have to discriminate: stopping is not enough. The connector states the problem
 * precisely - which server setting, and what it has to be - and that sentence is the whole value of the
 * failure. It reaches the user only if the code survives the trip from the connector to the read faces, so
 * the code is what is asserted. A run reporting the generic engine failure has stopped for a reason the
 * operator cannot act on without reading logs, which for a misconfigured source is the difference between
 * a two-minute fix and an investigation.
 *
 * <p>Gated on Docker and on a directory of real connector jars, like its siblings. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=NestTrackingRefusesASourceWithoutBeforeImagesIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class NestTrackingRefusesASourceWithoutBeforeImagesIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(180);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final String PARENT_TABLE = "orders";
    private static final String CHILD_TABLE = "order_items";
    private static final String PIPELINE_ID = "blind_tracking";

    /** The connector's own diagnosis, which is the part worth carrying all the way to the read faces. */
    private static final String EXPECTED_CODE = "connector.capture-failed";

    /**
     * This invocation's pipeline id, which carries the tier so the two tiers do not share a nest's state,
     * and a base of its own so no two witnesses share one either.
     */
    private String pipelineId;

    private static final long OLD_PARENT = 1;
    private static final long NEW_PARENT = 2;
    private static final long CHILD_ID = 1;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aSourceLoggingOnlyWhatChangedStopsTheRunWithAReasonToActOn(Tiers tier) throws Exception {
        // The configuration the documentation names as insufficient for following key changes.
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
                .withCommand("--binlog-row-image=MINIMAL", "--binlog-format=ROW")) {
            mysql.start();
            seedMysql(mysql);
            grantReplication(mysql);

            String suffix = tier.name().toLowerCase(Locale.ROOT);
            pipelineId = PIPELINE_ID + "_" + suffix;
            String targetUri = SharedMongo.replicaSetUrl("blind_target_" + suffix);

            // No reader for the target: the run never reaches it, and that is the point being pinned.
            try (PreparedNativeStore storage = PreparedNativeStore.create(); ServerHandle server = tier.launch(storage)) {
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

                await(() -> control.state(pipelineId).filter(PipelineState.FAILED::equals).isPresent());
                assertThat(control.state(pipelineId))
                        .as("a source that cannot supply earlier rows has to stop a tree that follows key "
                                + "changes, rather than let the documents diverge.%n  metrics: %s",
                                control.metrics(pipelineId))
                        .contains(PipelineState.FAILED);
                assertThat(control.errorCount(pipelineId))
                        .as("the failure is counted, not only entered")
                        .contains(1L);

                await(() -> control.failureCode(pipelineId).isPresent());
                assertThat(control.failureCode(pipelineId))
                        .as("the diagnosis, read from the status face. The connector names the exact server "
                                + "setting to change; a run reporting the generic engine failure has "
                                + "dropped that on the way and leaves the operator with logs to read.%n"
                                + "  logs: %s", control.logs(pipelineId))
                        .contains(EXPECTED_CODE);
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
                for (long id : List.of(OLD_PARENT, NEW_PARENT)) {
                    insert.setLong(1, id);
                    insert.setString(2, "order-" + id);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + CHILD_TABLE + " (id, order_id, sku) VALUES (?, ?, ?)")) {
                insert.setLong(1, CHILD_ID);
                insert.setLong(2, OLD_PARENT);
                insert.setString(3, "sku-1");
                insert.executeUpdate();
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

    /** The switch is on, which is what makes a source without earlier rows a refusal rather than an edit. */
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
                    from: { o: orders, i: order_items }
                    root:
                      from: o
                      key: [ id ]
                      embed:
                        - from: i
                          on: { order_id: id }
                          as: array
                          path: items
                          arrayKey: [ id ]
                          trackKeyChanges: true
                serve:
                  from: order_doc
                  sync:
                    - source: tgt_mongo
                """
                .formatted(pipelineId);
    }

    private static void sleep() {
        try {
            Thread.sleep(POLL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for the run to refuse the source", e);
        }
    }
}
