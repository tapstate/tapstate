package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;

import org.bson.Document;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Traffic that reaches only some of a nest's instances still moves the durable frontier.
 *
 * <p>A nest's documents are partitioned by root key, so the vertex assembling them runs as several
 * instances and each sees only the keys it holds. What may be forgotten is decided by the lowest of what
 * every instance has promised - so an instance that receives nothing promises nothing new, and its stale
 * promise is the one the whole job is held to. Every other reading stays healthy while that happens: the
 * state is RUNNING, no error is counted, documents keep landing, throughput is whatever it was. The only
 * thing that moves is the offset, and it moves by not moving.
 *
 * <p><b>Which is why this reads the offset itself</b> - where the product keeps it, in the chain records
 * in the store, rather than a metric derived from it. Two readings with narrow work between them, and the
 * second has to be past the first. Asserting the documents arrived would pass on the broken run, because
 * the documents do arrive.
 *
 * <p><b>The baseline is taken after ordinary traffic, not after the snapshot.</b> A snapshot is read
 * straight through rather than through the ring, and the durable offset is only ever advanced from the
 * change-stream side - clamped, further, to the slowest consumer's acked position, which does not exist
 * until a consumer has acked something. So a reading taken when the snapshot has just finished finds no
 * offset at all, and would report the healthy product as broken. Broad traffic first, then the baseline,
 * then the narrow traffic this is actually about.
 *
 * <p>The narrow traffic is deliberately one root's worth: every change belongs to a single key, so one
 * instance does all the work and the rest have nothing to say. That is the shape the heartbeat exists for,
 * and a run without one cannot get past it.
 *
 * <p>Gated on Docker and on a directory of real connector jars, like its siblings. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=NestIdleInstanceDoesNotStallFrontierIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class NestIdleInstanceDoesNotStallFrontierIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(180);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final String PARENT_TABLE = "orders";
    private static final String CHILD_TABLE = "order_items";
    private static final String PIPELINE_ID = "idle_instances";

    /** Where the product keeps what it would resume from: one record per mining chain, in the store. */
    private static final String CHAIN_RECORDS = io.tapstate.adapters.mongostore.SystemCollections.SRS_CHAINS.collectionName();

    /**
     * This invocation's pipeline id, which carries the tier so the two tiers do not share a nest's state,
     * and a base of its own so no two witnesses share one either.
     */
    private String pipelineId;

    /** Enough roots that the assembler is run as several instances, each holding some of the keys. */
    private static final int ROOTS = 200;

    /** Spread across many roots, so consumers exist and have acked before the baseline is taken. */
    private static final int WARMUP_CHANGES = 60;

    /** Every change after the baseline belongs to this one root, so the rest of the instances idle. */
    private static final long BUSY_ROOT = 1;
    private static final int NARROW_CHANGES = 40;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void changesReachingOneInstanceStillMoveWhatTheRunWouldResumeFrom(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            seedMysql(mysql);
            grantReplication(mysql);

            String suffix = tier.name().toLowerCase(Locale.ROOT);
            pipelineId = PIPELINE_ID + "_" + suffix;
            String targetUri = SharedMongo.replicaSetUrl("idle_target_" + suffix);

            try (PreparedNativeStore storage = PreparedNativeStore.create(); ServerHandle server = tier.launch(storage);
                    MongoEndpoints mongo = new MongoEndpoints()) {
                String storeUri = storage.cacheUri();
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

                // Ordinary traffic across many roots, so a consumer exists and has acked - without which
                // there is nothing to clamp a durable offset to and none is written at all.
                spreadChanges(mysql);
                await(() -> !offsets(mongo, storeUri).isEmpty());
                List<String> before = offsets(mongo, storeUri);
                if (before.isEmpty()) {
                    throw new AssertionError("no chain record carried an offset after " + WARMUP_CHANGES
                            + " changes across the roots: " + mongo.documents(EndpointAddress.uri(storeUri), CHAIN_RECORDS)
                            + System.lineSeparator() + "  pipeline state: " + control.state(pipelineId)
                            + System.lineSeparator() + "  metrics: " + control.metrics(pipelineId));
                }

                // Narrow traffic: one root's worth, so one instance works and the others have nothing.
                changeOneRootRepeatedly(mysql);

                await(() -> advancedFrom(before, offsets(mongo, storeUri)));
                assertThat(advancedFrom(before, offsets(mongo, storeUri)))
                        .as("what the run would resume from, after %d changes that all belong to root %d. "
                                + "An instance holding none of them promises nothing new, and a run held to "
                                + "the lowest promise stops here while every other reading stays healthy.%n"
                                + "  before: %s%n  after:  %s%n  metrics: %s",
                                NARROW_CHANGES, BUSY_ROOT, before, offsets(mongo, storeUri),
                                control.metrics(pipelineId))
                        .isTrue();

                // Read after the offset, so a run that died on the way cannot satisfy the assertion above
                // by having stopped for the wrong reason.
                assertThat(control.state(pipelineId))
                        .as("the run has to be alive for a frontier reading to mean anything")
                        .contains(PipelineState.RUNNING);
                assertThat(control.errorCount(pipelineId)).contains(0L);
            }
        }
    }

    /** Whether any chain's offset is past what it was, compared record by record. */
    private static boolean advancedFrom(List<String> before, List<String> after) {
        if (after.size() != before.size()) {
            // A chain appearing or leaving is itself movement; the run is not standing still.
            return true;
        }
        for (int i = 0; i < after.size(); i++) {
            if (!after.get(i).equals(before.get(i))) {
                return true;
            }
        }
        return false;
    }

    /** Every chain record's offset, in a stable order, read out of the store the product writes. */
    private static List<String> offsets(MongoEndpoints mongo, String storeUri) {
        List<String> offsets = new ArrayList<>();
        for (Document record : mongo.documents(EndpointAddress.uri(storeUri), CHAIN_RECORDS)) {
            Document checkpoint = record.get("checkpoint", Document.class);
            Object offset = checkpoint == null ? null : checkpoint.get("token");
            if (offset != null) {
                offsets.add(record.get("_id") + "=" + offset);
            }
        }
        java.util.Collections.sort(offsets);
        return offsets;
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
                for (long id = 1; id <= ROOTS; id++) {
                    insert.setLong(1, id);
                    insert.setString(2, "order-" + id);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }
    }

    /** Changes spread over many roots, so the change stream is live and a consumer has acked. */
    private static void spreadChanges(MySQLContainer<?> mysql) throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
                PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO " + CHILD_TABLE + " (id, order_id, sku) VALUES (?, ?, ?)")) {
            for (int id = 1; id <= WARMUP_CHANGES; id++) {
                insert.setInt(1, id);
                insert.setLong(2, id);
                insert.setString(3, "sku-warm-" + id);
                insert.executeUpdate();
            }
        }
    }

    /** Changes that all belong to one root, so exactly one instance has anything to do with them. */
    private static void changeOneRootRepeatedly(MySQLContainer<?> mysql) throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
                PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO " + CHILD_TABLE + " (id, order_id, sku) VALUES (?, ?, ?)")) {
            for (int n = 1; n <= NARROW_CHANGES; n++) {
                insert.setInt(1, WARMUP_CHANGES + n);
                insert.setLong(2, BUSY_ROOT);
                insert.setString(3, "sku-narrow-" + n);
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
            throw new AssertionError("interrupted while waiting for the frontier to move", e);
        }
    }
}
