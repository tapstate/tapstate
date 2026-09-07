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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A key a level has never held is fetched from the layer behind its memory once, not twice.
 *
 * <p>A level reads a key's state out and writes it back, which is two reaches for the state. Only the
 * first of them can miss: after it, the key is resident. So a run over keys none of which was ever held
 * has a shape - two reaches, one trip - and that shape is the whole assertion. A write that fetches the
 * entry it is about to overwrite makes the second trip too, and nothing else about the run differs:
 * every document still lands, every reading stays healthy, and the only trace is a round trip to
 * another process, per key, per residency.
 *
 * <p><b>Why both readings, and not the trips alone.</b> Trips alone would also be satisfied by a level
 * that had stopped reading the state at all, which is one trip fewer for a reason nobody wants. So the
 * reaches are asserted beside them: the pair a level performs is unchanged, and what went away is one
 * of the two trips behind it. Neither number alone says that.
 *
 * <p><b>Why nothing is evicted here, unlike the budget witness.</b> An evicted key read back is a
 * second trip that is real work, and in this total it is indistinguishable from the one this case is
 * about. So the budget is set far above the roots seeded: every trip in the reading is then a first
 * touch and nothing else, which is what lets the number be an equality rather than a bound. The budget
 * witness does the opposite deliberately, and asserts a bound for that reason.
 *
 * <p><b>Why the roots carry no children.</b> A child arriving folds into a root that is already
 * resident: reaches, no trips. True, and uninteresting here - it moves the reaches away from twice the
 * roots without moving the trips. The child table is created and left empty, so the tree is the one the
 * sibling witnesses use and every event in the run is a root arriving where nothing was held.
 *
 * <p>Gated on Docker and on a directory of real connector jars, like its siblings. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors -Dit.test=NestReadsAFirstTouchKeyOnceNotTwiceIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class NestReadsAFirstTouchKeyOnceNotTwiceIT {

    /** Two real database engines through a snapshot, which is what the shared bound is too short for. */
    private static final Duration BOUND = Duration.ofSeconds(180);

    /**
     * How long the two readings have to stand still before they are read as final. The metrics face
     * publishes on its own cadence and lags what the target already holds, so a reading taken the moment
     * the documents settle is a reading of part of the run - and part of this run satisfies the equality
     * on its way to failing it.
     */
    private static final Duration STILL = Duration.ofSeconds(8);

    private static final String PARENT_TABLE = "orders";
    private static final String CHILD_TABLE = "order_items";
    private static final String PIPELINE_ID = "first_touch_orders";

    /**
     * What every per-namespace count of these two kinds is named with, before the namespace itself.
     * Written here rather than imported from the runtime that publishes them, for the reason the reader
     * beside them gives: a name taken from the publisher would agree with it by construction.
     */
    private static final String REACHES = "nestStateAccesses.";
    private static final String TRIPS = "nestStateBackfills.";

    /**
     * Far above the roots seeded, so nothing is ever evicted and every trip in the reading is a first
     * touch. This is the one setting that separates this witness from the budget one.
     */
    private static final int MEMORY_BUDGET = 100_000;

    /** Enough roots that a per-key number is not one sample, and few enough to stay a short run. */
    private static final int ROOTS = 400;

    /**
     * This invocation's pipeline id, which carries the tier so the two tiers do not share a nest's state,
     * and a base of its own so no two witnesses share one either.
     */
    private String pipelineId;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void everyRootArrivesWhereNothingIsHeldAndCostsOneTripBehindTheMap(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            seedMysql(mysql);
            grantReplication(mysql);

            String suffix = tier.name().toLowerCase(Locale.ROOT);
            pipelineId = PIPELINE_ID + "_" + suffix;
            String storeUri = SharedMongo.replicaSetUrl("first_touch_store_" + suffix);
            String targetUri = SharedMongo.replicaSetUrl("first_touch_target_" + suffix);

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

                Await.until("all " + ROOTS + " roots to land as documents", BOUND,
                        () -> mongo.documents(EndpointAddress.uri(targetUri), PARENT_TABLE).size() == ROOTS,
                        () -> "'" + PARENT_TABLE + "' holds "
                                + mongo.documents(EndpointAddress.uri(targetUri), PARENT_TABLE).size()
                                + ", pipeline state " + control.state(pipelineId)
                                + ", error count " + control.errorCount(pipelineId)
                                + ", metrics " + control.metrics(pipelineId));

                assertThat(control.state(pipelineId))
                        .as("a reading taken off a run that has died says nothing about what it cost")
                        .contains(PipelineState.RUNNING);
                assertThat(control.errorCount(pipelineId))
                        .as("nor one taken off a run that is counting errors")
                        .contains(0L);

                assertWhatOneFirstTouchCost(control);
            }
        }
    }

    /**
     * The two readings, taken once they have stopped moving, and asserted as the pair they are.
     *
     * <p>They are asserted exactly rather than as bounds because nothing here is left to the run's
     * timing: every root is seeded before the pipeline starts and none of them is ever evicted, so how
     * many drains the run happens to take moves neither number. What does move them is the design.
     */
    private void assertWhatOneFirstTouchCost(ControlPlane control) {
        long[] settled = {-1L, -1L};
        long[] unchangedSince = {System.nanoTime()};
        // Stillness rather than a target value: waiting until the expected number shows up would pass
        // through it on the way past, so a run counting too many would be read at the one moment it
        // happened to be right.
        Await.until("the state readings to stop moving for " + STILL, BOUND,
                () -> {
                    long reaches = control.metricTotal(pipelineId, REACHES).orElse(0L);
                    long trips = control.metricTotal(pipelineId, TRIPS).orElse(0L);
                    if (reaches != settled[0] || trips != settled[1]) {
                        settled[0] = reaches;
                        settled[1] = trips;
                        unchangedSince[0] = System.nanoTime();
                        return false;
                    }
                    return reaches > 0 && System.nanoTime() - unchangedSince[0] >= STILL.toNanos();
                },
                () -> "reaches " + settled[0] + ", trips " + settled[1]);

        long reaches = settled[0];
        long trips = settled[1];
        assertThat(reaches)
                .as("the state was reached %d times over %d roots, where the pair a level performs is to "
                        + "read a key's state out and write it back. This is asserted beside the trips "
                        + "below because trips alone would also be satisfied by a level that had stopped "
                        + "reading the state at all.%n  metrics: %s",
                        reaches, ROOTS, control.metrics(pipelineId))
                .isEqualTo(2L * ROOTS);
        assertThat(trips)
                .as("those %d reaches went behind the map %d times, over %d roots none of which was ever "
                        + "held. Only the first reach of a key can miss - after it the key is resident - "
                        + "so one trip per root is the whole cost, and %d would mean the write is "
                        + "fetching the entry it is about to overwrite: a round trip to another process, "
                        + "per key, per residency, with every document still correct and nothing failing. "
                        + "Nothing was evicted here - a budget of %d over %d roots - so no trip in this "
                        + "number is a key read back rather than a key touched for the first "
                        + "time.%n  metrics: %s",
                        reaches, trips, ROOTS, 2L * ROOTS, MEMORY_BUDGET, ROOTS, control.metrics(pipelineId))
                .isEqualTo((long) ROOTS);
    }

    /** Every seeded root, with the child table created and left empty. See the class note. */
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
}
