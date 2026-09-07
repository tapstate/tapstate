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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * More roots than the memory budget allows is not a failure. The run keeps going, every document lands,
 * and what does not fit in memory is carried by the layer behind it.
 *
 * <p>This is the half of the capacity story that says which bounds are bounds. One of them fails a run -
 * how wide a single document may be, which no eviction can reach inside of - and it has a witness of its
 * own. The rest are not limits at all but a working set: exceed it and entries move to the layer behind
 * memory, which is the design rather than a degradation. An implementation that treated the budget as a
 * limit would be turning an ordinary large pipeline into a dead one.
 *
 * <p>What the assertions have to discriminate: "it did not fail" is satisfied by a run that never started,
 * never assembled anything, or ignored the budget entirely. So three readings are taken together - every
 * seeded root reached the target as a document with its child in it, the run is RUNNING with nothing
 * counted against it, and the layer behind memory was actually read. The last is what separates this from
 * a run whose budget was simply not applied, and it is read as backfills rather than as a comparison of
 * entries against stored: measured, those two both report the full number of roots, because everything a
 * write-through store holds is both.
 *
 * <p>Gated on Docker and on a directory of real connector jars, like its siblings. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors -Dit.test=NestHoldsMoreThanItsMemoryBudgetIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class NestHoldsMoreThanItsMemoryBudgetIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(180);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final String PARENT_TABLE = "orders";
    private static final String CHILD_TABLE = "order_items";
    private static final String PIPELINE_ID = "budgeted_orders";

    /**
     * This invocation's pipeline id, which carries the tier so the two tiers do not share a nest's state,
     * and a base of its own so no two witnesses share one either.
     */
    private String pipelineId;

    private static final String EMBED_PATH = "items";

    /** The smallest budget the product accepts - the partition count it is spent across - and more roots. */
    private static final int MEMORY_BUDGET = 271;
    private static final int ROOTS = 700;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aNestHoldingMoreThanItsBudgetKeepsRunningAndLandsEverything(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            seedMysql(mysql);
            grantReplication(mysql);

            String suffix = tier.name().toLowerCase(Locale.ROOT);
            pipelineId = PIPELINE_ID + "_" + suffix;
            String storeUri = SharedMongo.replicaSetUrl("budget_store_" + suffix);
            String targetUri = SharedMongo.replicaSetUrl("budget_target_" + suffix);

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

                List<Document> documents = await(mongo, targetUri, all -> all.size() == ROOTS && whole(all));
                if (!(documents.size() == ROOTS && whole(documents))) {
                    throw new AssertionError("a budget of " + MEMORY_BUDGET + " over " + ROOTS
                            + " roots did not land every document: '" + PARENT_TABLE + "' holds "
                            + documents.size() + ", of which " + countWhole(documents) + " carry their child"
                            + System.lineSeparator() + "  pipeline state: " + control.state(pipelineId)
                            + ", error count: " + control.errorCount(pipelineId)
                            + System.lineSeparator() + "  metrics: " + control.metrics(pipelineId)
                            + System.lineSeparator() + "  logs: " + control.logs(pipelineId));
                }

                assertThat(control.state(pipelineId))
                        .as("exceeding the working set is the design, not a limit to die on")
                        .contains(PipelineState.RUNNING);
                assertThat(control.errorCount(pipelineId))
                        .as("nor a reason to count an error")
                        .contains(0L);
                assertLayerBehindMemoryWasRead(control);
                assertOnlyTheBudgetIsHeldInMemory(control);
            }
        }
    }

    /** Every document carries the one child seeded for it, which a run that dropped state would not. */
    private static boolean whole(List<Document> documents) {
        return countWhole(documents) == ROOTS;
    }

    private static long countWhole(List<Document> documents) {
        long whole = 0;
        for (Document document : documents) {
            if (elementsOf(document).size() == 1) {
                whole++;
            }
        }
        return whole;
    }

    /**
     * That the budget was applied at all, which "it did not fail" cannot say. Awaited, because the metrics
     * face publishes on its own cadence and lags what the target already holds.
     */
    private void assertLayerBehindMemoryWasRead(ControlPlane control) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        long backfills = 0L;
        while (System.nanoTime() - deadline < 0) {
            backfills = control.metricTotal(pipelineId, "nestStateBackfills.").orElse(0L);
            if (backfills > ROOTS) {
                break;
            }
            sleep();
        }
        assertThat(backfills)
                .as("reads that had to go to the layer behind memory, against the %d roots seeded under a "
                        + "budget of %d; a run that ignored the budget keeps everything resident and needs "
                        + "no more of these than the one a cold start gives each key.%n  metrics: %s",
                        ROOTS, MEMORY_BUDGET, control.metrics(pipelineId))
                .isGreaterThan(ROOTS);
    }

    /**
     * That the budget bounds what is <em>held</em>, which is the only question a budget is asked and the
     * one nothing here has ever put.
     *
     * <p><b>Why this is not the reading above.</b> Trips behind the map say the layer was reached; they do
     * not say anything left memory. Measured, they can be satisfied with every entry still resident: a key
     * touched for the first time reaches behind the map on the way in, so a cold start over 700 roots
     * clears a bound of 271 without a single eviction. Residency cannot be satisfied that way - an entry is
     * either in memory or it is not.
     *
     * <p>Read as the entries beside what the layer behind them holds, because either alone is satisfied by
     * a run that lost data: nothing resident and nothing stored is a pipeline that assembled nothing.
     */
    private void assertOnlyTheBudgetIsHeldInMemory(ControlPlane control) {
        long resident = control.metricTotal(pipelineId, "nestStateEntries.").orElse(0L);
        long stored = control.metricTotal(pipelineId, "nestStateStored.").orElse(0L);

        assertThat(stored)
                .describedAs("the layer behind the maps holds %d of the %d roots seeded. What is not "
                        + "resident has to be somewhere, and this is where; a budget that bounded memory "
                        + "by dropping state would satisfy every other reading here.%n  metrics: %s",
                        stored, ROOTS, control.metrics(pipelineId))
                .isEqualTo(ROOTS);
        assertThat(resident)
                .describedAs("%d of %d roots are still in memory under a budget of %d. A budget that "
                        + "bounds nothing is not a slower run - it is a member that fills up with every "
                        + "reading healthy, because the configuration reads back as the number that was "
                        + "asked for and the substrate reports nothing. The budget is spent per partition, "
                        + "so what should be left is near it rather than all of them.%n  metrics: %s",
                        resident, ROOTS, MEMORY_BUDGET, control.metrics(pipelineId))
                .isLessThanOrEqualTo(2L * MEMORY_BUDGET);
    }

    private static List<Document> await(
            MongoEndpoints mongo, String targetUri, Predicate<List<Document>> settled) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        List<Document> last = List.of();
        while (System.nanoTime() - deadline < 0) {
            last = mongo.documents(EndpointAddress.uri(targetUri), PARENT_TABLE);
            if (settled.test(last)) {
                return last;
            }
            sleep();
        }
        return last;
    }

    private static List<Document> elementsOf(Document root) {
        Object embedded = root.get(EMBED_PATH);
        if (!(embedded instanceof List<?> list)) {
            return List.of();
        }
        List<Document> elements = new java.util.ArrayList<>(list.size());
        for (Object element : list) {
            if (element instanceof Document document) {
                elements.add(document);
            }
        }
        return elements;
    }

    /** More roots than the budget, one child each, so a lost entry shows as a document short of its child. */
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
            throw new AssertionError("interrupted while waiting for the documents to settle", e);
        }
    }
}
