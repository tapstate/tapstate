package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;

import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * One engine falling silent does not stop the other engine's stream being forgettable.
 *
 * <p>What a run may forget is the lowest position everything above it has promised to have got past. A
 * stream that is not producing anything promises nothing new, and where that stale promise is taken into
 * the whole job's answer, the durable offset of the stream that <em>is</em> busy stops moving with it.
 * Two engines make this ordinary rather than exotic: the shipments database can be quiet for hours while
 * orders keep changing, and neither database knows the other exists.
 *
 * <p>Every other reading stays healthy while this happens - the state is RUNNING, no error is counted,
 * documents keep landing, throughput is whatever it was. The only thing that moves is the offset, and it
 * moves by not moving. <b>So this reads the offset itself</b>, where the product keeps it, rather than
 * anything derived from it. Asserting that the documents arrived would pass on the broken run, because
 * the documents do arrive.
 *
 * <p><b>The baseline is taken after traffic on both streams, not after the snapshot.</b> A snapshot is
 * read straight through rather than through the ring, and the durable offset is only ever advanced from
 * the change-stream side - clamped, further, to the slowest consumer's acked position, which does not
 * exist until a consumer has acked something. A reading taken when the snapshot has just finished finds
 * no offset at all and would report the healthy product as broken.
 *
 * <p><b>The anti-vacuous check matters more here than the assertion it guards.</b> This case is about one
 * chain of two being idle, so it says nothing at all if the idle chain is not there to be idle: a build
 * where the second engine never produced a chain record would sail through an assertion that "some chain
 * advanced", having witnessed nothing. Both chains are therefore required to be present in the baseline
 * before the narrow traffic starts, and the idle one is required to still be there, unmoved, at the end -
 * an idle chain that quietly disappeared is not the same product as one that stayed and was outrun.
 *
 * <p>Gated on Docker and on a directory of real connector jars, like its siblings. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=CrossEngineIdleStreamDoesNotStallFrontierIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class CrossEngineIdleStreamDoesNotStallFrontierIT {

    /**
     * Wider than the shared default, for the reason {@link Await} gives for taking a bound at all: this
     * drives two real engines through a snapshot and a change stream before the first reading is due.
     */
    private static final Duration BOUND = Duration.ofSeconds(180);

    private static final String ROOT_TABLE = "orders";
    private static final String CHILD_TABLE = "shipments";

    /** Where the product keeps what it would resume from: one record per mining chain, in the store. */
    private static final String CHAIN_RECORDS = "srs_meta";
    private static final String OFFSET_FIELD = "sourceReadOffset";

    private static final int ROOTS = 40;

    /** Traffic on both streams, so both chains carry an offset and a consumer has acked one. */
    private static final int WARMUP_PER_STREAM = 20;

    /** Traffic on one stream only - the shape this exists for. */
    private static final int NARROW_CHANGES = 40;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "postgres", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void changesOnOneEngineAloneStillMoveWhatTheRunWouldResumeFrom(Tiers tier) throws Exception {
        try (SharedPostgres.Fixture postgresFixture = SharedPostgres.fixture()) {
            String suffix = tier.name().toLowerCase(Locale.ROOT);
            String pipelineId = "cross_engine_idle_" + suffix;

            Map<String, Object> orders = SharedMySql.settings("idle_orders_" + suffix);
            Map<String, Object> shipments = postgresFixture.settings("idle_shipments_" + suffix);
            createTables(orders, shipments);
            seedRoots(orders);

            String storeUri = SharedMongo.replicaSetUrl("idlestream_store_" + suffix);
            String targetUri = SharedMongo.replicaSetUrl("idlestream_target_" + suffix);

            try (ServerHandle server = postgresFixture.launch(() -> tier.launch(storeUri));
                    MongoEndpoints mongo = new MongoEndpoints()) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");

                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("postgres", ConnectorJars.bytesFor("postgres"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

                Map<String, String> resources = new LinkedHashMap<>();
                resources.put("src_orders.tap.yml", mysqlSourceYaml(orders));
                resources.put("src_shipments.tap.yml", postgresSourceYaml(shipments));
                resources.put("tgt_mongo.tap.yml", targetYaml(targetUri));
                resources.put("pipeline.tap.yml", pipelineYaml(pipelineId));
                control.apply(resources);

                control.discoverSchema("src_orders", "mysql", orders);
                control.discoverSchema("src_shipments", "postgres", postgresDiscoveryConfig(shipments));

                control.lifecycle(pipelineId, LifecycleVerb.START);

                Await.until("every root to be assembled and written", BOUND,
                        () -> mongo.documents(EndpointAddress.uri(targetUri), ROOT_TABLE).size() == ROOTS,
                        () -> mongo.documents(EndpointAddress.uri(targetUri), ROOT_TABLE).size() + " documents");

                // Traffic on both streams, so both chains exist and carry an offset to compare against.
                changeOrders(orders, 1, WARMUP_PER_STREAM);
                insertShipments(shipments, 1, WARMUP_PER_STREAM);

                Await.until("both chains to be carrying a durable offset", BOUND,
                        () -> offsets(mongo, storeUri).size() >= 2,
                        () -> String.valueOf(mongo.documents(EndpointAddress.uri(storeUri), CHAIN_RECORDS)));
                Map<String, String> before = offsets(mongo, storeUri);
                assertThat(before)
                        .as("both chains have to be carrying an offset before one of them is asked to go "
                                + "quiet - a run where the second engine never produced a chain record has "
                                + "nothing idle in it, and would satisfy the assertion below having witnessed "
                                + "nothing.%n  chain records: %s",
                                mongo.documents(EndpointAddress.uri(storeUri), CHAIN_RECORDS))
                        .hasSizeGreaterThanOrEqualTo(2);

                // The narrow traffic: orders only. The shipments database is not touched again.
                changeOrders(orders, WARMUP_PER_STREAM + 1, NARROW_CHANGES);

                Await.until("a chain's durable offset to move while the other engine stays quiet", BOUND,
                        () -> !changed(before, offsets(mongo, storeUri)).isEmpty(),
                        () -> "before " + before + ", now " + offsets(mongo, storeUri));
                Map<String, String> after = offsets(mongo, storeUri);
                assertThat(changed(before, after))
                        .as("what the run would resume from, after %d changes that all belong to one of the "
                                + "two engines. A chain that received nothing promises nothing new, and a job "
                                + "held to the lowest promise stops here while every other reading stays "
                                + "healthy.%n  before: %s%n  after:  %s%n  metrics: %s",
                                NARROW_CHANGES, before, after, control.metrics(pipelineId))
                        .isNotEmpty();

                assertThat(after.keySet())
                        .as("the idle chain has to still be there at the end. One that disappeared instead of "
                                + "being outrun is a different product, and it would pass the assertion above "
                                + "for a reason that has nothing to do with what this is for.%n  before: %s%n"
                                + "  after:  %s", before, after)
                        .containsAll(before.keySet());

                // Read after the offset, so a run that died on the way cannot satisfy the assertions above
                // by having stopped for the wrong reason.
                assertThat(control.state(pipelineId))
                        .as("the run has to be alive for a frontier reading to mean anything")
                        .contains(PipelineState.RUNNING);
                assertThat(control.errorCount(pipelineId)).contains(0L);
            }
        }
    }

    /** Which chains report an offset different from the one they reported before. */
    private static Set<String> changed(Map<String, String> before, Map<String, String> after) {
        Set<String> moved = new LinkedHashSet<>();
        for (Map.Entry<String, String> entry : after.entrySet()) {
            String was = before.get(entry.getKey());
            if (was == null || !was.equals(entry.getValue())) {
                moved.add(entry.getKey());
            }
        }
        return moved;
    }

    /** Every chain record's offset by chain, read out of the store the product writes. */
    private static Map<String, String> offsets(MongoEndpoints mongo, String storeUri) {
        Map<String, String> offsets = new LinkedHashMap<>();
        for (Document record : mongo.documents(EndpointAddress.uri(storeUri), CHAIN_RECORDS)) {
            Object offset = record.get(OFFSET_FIELD);
            if (offset != null) {
                offsets.put(String.valueOf(record.get("_id")), String.valueOf(offset));
            }
        }
        return offsets;
    }

    // ---- fixtures -----------------------------------------------------------------------

    private static void createTables(Map<String, Object> orders, Map<String, Object> shipments)
            throws Exception {
        try (Connection connection = SharedMySql.connect(orders);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + ROOT_TABLE);
            statement.execute("CREATE TABLE " + ROOT_TABLE
                    + " (id INT PRIMARY KEY, customer VARCHAR(64))");
        }
        try (Connection connection = SharedPostgres.connect(shipments);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + CHILD_TABLE);
            statement.execute("CREATE TABLE " + CHILD_TABLE
                    + " (id INT PRIMARY KEY, order_id INT, carrier VARCHAR(64))");
            statement.execute("ALTER TABLE " + CHILD_TABLE + " REPLICA IDENTITY FULL");
        }
    }

    private static void seedRoots(Map<String, Object> orders) throws Exception {
        try (Connection connection = SharedMySql.connect(orders);
                Statement statement = connection.createStatement()) {
            for (int id = 1; id <= ROOTS; id++) {
                statement.execute("INSERT INTO " + ROOT_TABLE + " (id, customer) VALUES ("
                        + id + ", 'order-" + id + "')");
            }
        }
    }

    /** Changes on the stream that stays busy, spread across roots so consumers exist and ack. */
    private static void changeOrders(Map<String, Object> orders, int from, int count) throws Exception {
        try (Connection connection = SharedMySql.connect(orders);
                Statement statement = connection.createStatement()) {
            for (int n = 0; n < count; n++) {
                int id = (n % ROOTS) + 1;
                statement.execute("UPDATE " + ROOT_TABLE + " SET customer = 'customer-" + (from + n)
                        + "' WHERE id = " + id);
            }
        }
    }

    /** Warm-up traffic on the stream that afterwards goes quiet and stays quiet. */
    private static void insertShipments(Map<String, Object> shipments, int from, int count)
            throws Exception {
        try (Connection connection = SharedPostgres.connect(shipments);
                Statement statement = connection.createStatement()) {
            for (int n = 0; n < count; n++) {
                int id = from + n;
                statement.execute("INSERT INTO " + CHILD_TABLE + " (id, order_id, carrier) VALUES ("
                        + id + ", " + ((n % ROOTS) + 1) + ", 'carrier-" + id + "')");
            }
        }
    }

    /**
     * The settings a discovery call needs, spelled the way this connector spells them.
     *
     * <p>Two differences from what the shared server hands back, and both are the connector's own naming
     * rather than a choice made here: the account is "user" where MySQL says "username", and a table is
     * addressed by schema as well as by database. The harness publishes the account under one uniform
     * name across every store, which is what makes a fixture readable; a discovery call is given the
     * connector's settings instead, so the translation happens here. Passing the uniform spelling
     * through reaches the connector as no account at all, and it falls back to the operating system
     * user - which fails as a password error naming a user nobody configured.
     */
    private static Map<String, Object> postgresDiscoveryConfig(Map<String, Object> settings) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("host", settings.get("host"));
        config.put("port", settings.get("port"));
        config.put("database", settings.get("database"));
        config.put("schema", "public");
        config.put("user", settings.get("username"));
        config.put("password", settings.get("password"));
        return config;
    }
    // ---- resources ----------------------------------------------------------------------

    private static String mysqlSourceYaml(Map<String, Object> settings) {
        return """
                version: tapstate/v1
                kind: source
                id: src_orders
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ %s ]
                """
                .formatted(settings.get("host"), settings.get("port"), settings.get("database"),
                        settings.get("username"), settings.get("password"), ROOT_TABLE);
    }

    private static String postgresSourceYaml(Map<String, Object> settings) {
        // "user" where MySQL says "username", and a schema as well as a database - this connector's own
        // spelling rather than a choice made here.
        return """
                version: tapstate/v1
                kind: source
                id: src_shipments
                connector: postgres
                config: { host: %s, port: %s, database: %s, schema: public, user: %s, password: %s }
                mode: cdc
                tables: [ %s ]
                """
                .formatted(settings.get("host"), settings.get("port"), settings.get("database"),
                        settings.get("username"), settings.get("password"), CHILD_TABLE);
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
                source: [ src_orders, src_shipments ]
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: order_doc
                    type: nest
                    from: { o: %s, s: %s }
                    root:
                      from: o
                      key: [ id ]
                      embed:
                        - { from: s, on: { order_id: id }, as: array, path: %s, arrayKey: [ id ] }
                serve:
                  from: order_doc
                  sync:
                    - source: tgt_mongo
                """
                .formatted(pipelineId, ROOT_TABLE, CHILD_TABLE, CHILD_TABLE);
    }

}
