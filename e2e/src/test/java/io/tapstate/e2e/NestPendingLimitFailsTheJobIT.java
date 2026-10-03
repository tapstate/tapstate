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
 * One key holding more changes for a parent that has not arrived than it is allowed to fails the run,
 * and says which bound it was.
 *
 * <p>This is the bound no other one reaches. What waits lives inside a single entry, so a budget counting
 * entries is satisfied however long one key's queue has grown - the entry is one entry - and a limit on
 * how wide a document may be counts elements where this counts every change held against them. Nothing
 * else in the capacity story sees this at all.
 *
 * <p>It fails rather than letting go, and that ordering is the point. Reaching this says only that much
 * arrived before the parent did, never that the parent is absent, so releasing on it would drop rows that
 * were going to be part of a document. Giving up on a wait is a judgement about whether the parent is ever
 * coming, and running out of room is not evidence either way.
 *
 * <p>What the assertions have to discriminate: an implementation that quietly drops the overflow keeps the
 * pipeline RUNNING and produces documents that look entirely reasonable - nothing about the target says
 * rows were lost, because those rows have no parent to appear under. So the reading is the pipeline's own,
 * and the code is asserted rather than only the state: a run that died of anything else - the elements one
 * document may hold, a connector, the job - satisfies "it stopped" while saying nothing about this bound.
 *
 * <p>Gated on Docker and on a directory of real connector jars, like its siblings. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors -Dit.test=NestPendingLimitFailsTheJobIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class NestPendingLimitFailsTheJobIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(240);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final String PARENT_TABLE = "orders";
    private static final String CHILD_TABLE = "order_items";
    private static final String PIPELINE_ID = "pending_orders";
    private static final String EXPECTED_CODE = "nest.pending-limit-exceeded";

    /**
     * This invocation's pipeline id, which carries the tier so the two tiers do not share a nest's state,
     * and a base of its own so no two witnesses share one either.
     */
    private String pipelineId;

    /**
     * What may be held for one key, and enough rows to go past it. The number is the product's own default:
     * this bound has no field an author can write, so the witness has to reach the shipped one rather than
     * turn it down - which is also the honest thing to exercise, since it is the number a deployment runs on.
     */
    private static final int PENDING_LIMIT = 10_000;
    private static final int ORPHANS = PENDING_LIMIT + 500;

    /** The root none of them name, and the one that does exist so the pipeline has something to do. */
    private static final long ABSENT_ROOT = 999_999;
    private static final long REAL_ROOT = 1;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void oneKeyHoldingMoreThanItMayStopsTheRunAndSaysWhy(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            seedMysql(mysql);
            grantReplication(mysql);

            String suffix = tier.name().toLowerCase(Locale.ROOT);
            pipelineId = PIPELINE_ID + "_" + suffix;
            String storeUri = SharedMongo.replicaSetUrl("pending_store_" + suffix);
            String targetUri = SharedMongo.replicaSetUrl("pending_target_" + suffix);

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

                await(() -> control.state(pipelineId).filter(PipelineState.FAILED::equals).isPresent()
                        && control.failureCode(pipelineId).filter(EXPECTED_CODE::equals).isPresent()
                        && control.errorCount(pipelineId).filter(count -> count == 1L).isPresent());
                assertThat(control.state(pipelineId))
                        .as("%d changes held for root %d, which is not in the table, against a limit of "
                                + "%d.%n  documents: %s%n  metrics: %s", ORPHANS, ABSENT_ROOT, PENDING_LIMIT,
                                mongo.documents(EndpointAddress.uri(targetUri), PARENT_TABLE).size(), control.metrics(pipelineId))
                        .contains(PipelineState.FAILED);
                assertThat(control.errorCount(pipelineId))
                        .as("the failure is counted, not only entered")
                        .contains(1L);

                await(() -> control.failureCode(pipelineId).isPresent());
                assertThat(control.failureCode(pipelineId))
                        .as("which bound it was. A run that died of anything else satisfies the state "
                                + "above and says nothing about this one.%n  logs: %s",
                                control.logs(pipelineId))
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

    /** One real root, and more children of a root that is not there than one key may hold. */
    private static void seedMysql(MySQLContainer<?> mysql) throws Exception {
        try (Connection connection =
                DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(
                        "CREATE TABLE " + PARENT_TABLE + " (id INT PRIMARY KEY, name VARCHAR(64))");
                // No foreign key: children of a row that is not there have to be expressible.
                statement.execute("CREATE TABLE " + CHILD_TABLE
                        + " (id INT PRIMARY KEY, order_id INT, sku VARCHAR(64))");
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + PARENT_TABLE + " (id, name) VALUES (?, ?)")) {
                insert.setLong(1, REAL_ROOT);
                insert.setString(2, "order-" + REAL_ROOT);
                insert.executeUpdate();
            }
            connection.setAutoCommit(false);
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + CHILD_TABLE + " (id, order_id, sku) VALUES (?, ?, ?)")) {
                for (int id = 1; id <= ORPHANS; id++) {
                    insert.setInt(1, id);
                    insert.setLong(2, ABSENT_ROOT);
                    insert.setString(3, "sku-" + id);
                    insert.addBatch();
                    if (id % 1000 == 0) {
                        insert.executeBatch();
                    }
                }
                insert.executeBatch();
            }
            connection.commit();
            connection.setAutoCommit(true);
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
                .formatted(pipelineId);
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
