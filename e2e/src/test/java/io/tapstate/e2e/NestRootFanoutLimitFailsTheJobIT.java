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
 * A document that outgrows the elements one document may hold fails the run, and the run says which
 * bound it was - rather than being trimmed, spilled, or reported as a job that merely died.
 *
 * <p>This is the one bound the layer behind memory cannot absorb. How many documents there are and how
 * many keys a level holds are bounded by what stays resident: what does not fit is read back when it is
 * asked for. What is inside a single document is not, because a document is rendered whole, so however
 * much it has absorbed has to be in memory at once and no eviction reaches inside one.
 *
 * <p>What the assertions have to discriminate, and why there are three of them:
 * <ul>
 *   <li><b>The run stops.</b> An implementation that silently discards the elements past the limit keeps
 *       the pipeline RUNNING and produces a document that looks entirely reasonable - the right root, an
 *       array of the allowed width, every element in it correct. Nothing about the target says rows were
 *       dropped.</li>
 *   <li><b>The failure is counted.</b> A state without a count is a state nothing published.</li>
 *   <li><b>The code is the nest's own.</b> This is the one that was not free: a run that died of anything
 *       else satisfies both assertions above while saying nothing about this bound, and a nest whose coded
 *       failure degrades on the way to the read faces reports the generic engine failure - true, useless,
 *       and indistinguishable from a fault inside Jet. The placeholders this code carries are exactly what
 *       an operator needs (which root, how many elements, what the limit was), and all of it is lost with
 *       the code.</li>
 * </ul>
 *
 * <p>Gated on Docker and on a directory of real connector jars, like its siblings. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors -Dit.test=NestRootFanoutLimitFailsTheJobIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class NestRootFanoutLimitFailsTheJobIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(120);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final String PARENT_TABLE = "orders";
    private static final String CHILD_TABLE = "order_items";
    private static final String PIPELINE_ID = "fanout_orders";
    private static final String EXPECTED_CODE = "nest.root-fanout-limit-exceeded";

    /**
     * This invocation's pipeline id, which carries the tier so the two tiers do not share a nest's state,
     * and a base of its own so no two witnesses share one either. A nest keeps its state in the
     * deployment's configured database, addressed by a namespace built from the pipeline and step ids.
     */
    private String pipelineId;

    /** Two allowed against three seeded: the overflow is in the data rather than in the timing. */
    private static final int ELEMENT_LIMIT = 2;
    private static final int CHILDREN = 3;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aDocumentGrownPastWhatOneMayHoldStopsTheRunAndSaysWhy(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            seedMysql(mysql);
            grantReplication(mysql);

            String suffix = tier.name().toLowerCase(Locale.ROOT);
            pipelineId = PIPELINE_ID + "_" + suffix;
            String storeUri = SharedMongo.replicaSetUrl("fanout_store_" + suffix);
            String targetUri = SharedMongo.replicaSetUrl("fanout_target_" + suffix);

            try (ServerHandle server = tier.launch(storeUri);
                    MongoEndpoints mongo = new MongoEndpoints()) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");

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
                        .as("a document of %d elements under a limit of %d has to stop the run.%n"
                                + "  documents: %s%n  metrics: %s", CHILDREN, ELEMENT_LIMIT,
                                mongo.documents(EndpointAddress.uri(targetUri), PARENT_TABLE), control.metrics(pipelineId))
                        .contains(PipelineState.FAILED);
                assertThat(control.errorCount(pipelineId))
                        .as("the failure is counted, not only entered")
                        .contains(1L);

                await(() -> control.failureCode(pipelineId).isPresent());
                assertThat(control.failureCode(pipelineId))
                        .as("what killed the run, read from the status face rather than from a log.%n"
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

    /** One root with more children than the document is allowed to hold. */
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
                    "INSERT INTO " + PARENT_TABLE + " (id, name) VALUES (1, 'order-1')")) {
                insert.executeUpdate();
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + CHILD_TABLE + " (id, order_id, sku) VALUES (?, 1, ?)")) {
                for (int id = 1; id <= CHILDREN; id++) {
                    insert.setInt(1, id);
                    insert.setString(2, "sku-" + id);
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

    /** The nest writes down how wide one of its documents may be, which is the author's only way to. */
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
                    max_elements_per_document: %d
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
                .formatted(pipelineId, ELEMENT_LIMIT);
    }

    private static void sleep() {
        try {
            Thread.sleep(POLL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for the run to fail", e);
        }
    }
}
