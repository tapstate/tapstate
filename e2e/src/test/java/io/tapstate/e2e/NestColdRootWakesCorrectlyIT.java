package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
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
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A root that has sunk below the memory budget wakes up whole: the document it produces holds what it
 * held before it sank, plus the change that woke it.
 *
 * <p>Every other witness runs on a budget nothing comes close to, so every root it touches stays resident
 * and the layer behind memory is never read. What that leaves untested is the only thing the layer is for.
 * A nest keeps more than the document per root - which children it has seen, which arrived before their
 * parent - and an implementation that stores the document and forgets the rest passes every assertion
 * about a root that never left memory, then answers a woken root with a document that has lost everything
 * it had.
 *
 * <p><b>Something has to have sunk for any of that to be exercised, and saying so is not free.</b> Seeding
 * more roots than the budget is an argument, not a reading. The two counts that look like they would settle
 * it do not: measured, both {@code nestStateEntries} and {@code nestStateStored} report the full number of
 * roots, because everything a write-through store holds is both an entry and stored - neither is a count of
 * what is resident. What does discriminate is {@code nestStateBackfills}: a read that had to go to the
 * layer behind memory, which is exactly what a resident entry never needs. It is asserted against the roots
 * rather than against zero, so a run whose budget was ignored - where a first touch of each key can still
 * read through - cannot pass on the cold start alone.
 *
 * <p>Gated on Docker and on a directory of real connector jars, like its siblings. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors -Dit.test=NestColdRootWakesCorrectlyIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class NestColdRootWakesCorrectlyIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(180);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final String PARENT_TABLE = "orders";
    private static final String CHILD_TABLE = "order_items";
    private static final String PIPELINE_ID = "cold_orders";

    /**
     * This invocation's pipeline id, which carries the tier so the two tiers do not share a nest's state,
     * and a base of its own so no two witnesses share one either.
     */
    private String pipelineId;

    private static final String EMBED_PATH = "items";

    /**
     * The smallest budget the product accepts, and more roots than it. The number is the partition count a
     * budget is spent across; below it the budget stops meaning what it says and the run is refused, so this
     * is the one value that forces the layer behind memory into use without being rejected outright.
     */
    private static final int MEMORY_BUDGET = 271;
    private static final int ROOTS = 700;

    /** The root whose child arrives after it has had every chance to sink: the first one seeded. */
    private static final long WOKEN_ROOT = 1;
    private static final long SNAPSHOT_CHILD = 1;
    private static final long LATE_CHILD = 100_000;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aRootReadBackFromTheLayerBehindMemoryKeepsWhatItHeld(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            seedMysql(mysql);
            grantReplication(mysql);

            String suffix = tier.name().toLowerCase(Locale.ROOT);
            pipelineId = PIPELINE_ID + "_" + suffix;
            String targetUri = SharedMongo.replicaSetUrl("cold_target_" + suffix);

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

                List<Document> assembled = await(mongo, targetUri, documents -> documents.size() == ROOTS
                        && elementIds(documents, WOKEN_ROOT).equals(List.of(SNAPSHOT_CHILD)));
                if (!(assembled.size() == ROOTS
                        && elementIds(assembled, WOKEN_ROOT).equals(List.of(SNAPSHOT_CHILD)))) {
                    throw new AssertionError(diagnose(control, mongo, targetUri,
                            "the seeded roots never assembled", assembled));
                }
                assertSomethingSank(control);

                insertLateChild(mysql);

                List<Document> woken = await(mongo, targetUri,
                        documents -> elementIds(documents, WOKEN_ROOT).contains(LATE_CHILD));
                if (!elementIds(woken, WOKEN_ROOT).contains(LATE_CHILD)) {
                    throw new AssertionError(diagnose(control, mongo, targetUri,
                            "the child of a sunk root never reached its document", woken));
                }
                assertThat(elementIds(woken, WOKEN_ROOT))
                        .as("document %d after being woken: what it held before it sank, and the new child",
                                WOKEN_ROOT)
                        .containsExactlyInAnyOrder(SNAPSHOT_CHILD, LATE_CHILD);
            }
        }
    }

    /**
     * That the layer behind memory is being read, taken as a reading rather than argued from the budget.
     *
     * <p>Awaited rather than read once: the observation the metrics face answers from is published on its
     * own cadence and lags what the target already holds, so a single read taken the moment the documents
     * settle can still say nothing has happened at all.
     */
    private void assertSomethingSank(ControlPlane control) {
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
                        + "budget of %d. A resident entry never needs one, so more of them than there are "
                        + "roots is what says entries were pushed out and read back rather than merely "
                        + "touched once on a cold start.%n  metrics: %s",
                        ROOTS, MEMORY_BUDGET, control.metrics(pipelineId))
                .isGreaterThan(ROOTS);
    }

    /** The ids of the elements under one root, in array order, or empty when the root holds none. */
    private static List<Long> elementIds(List<Document> documents, long rootId) {
        for (Document document : documents) {
            if (numberOf(identityOf(document)) == rootId) {
                List<Long> ids = new ArrayList<>();
                for (Document element : elementsOf(document)) {
                    ids.add(numberOf(identityOf(element)));
                }
                return ids;
            }
        }
        return List.of();
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

    private String diagnose(ControlPlane control, MongoEndpoints mongo, String targetUri, String what,
            List<Document> documents) {
        return what + ": '" + PARENT_TABLE + "' holds " + documents.size() + " documents, and document "
                + WOKEN_ROOT + " holds " + elementIds(documents, WOKEN_ROOT)
                + System.lineSeparator() + "  pipeline state: " + control.state(pipelineId)
                + ", error count: " + control.errorCount(pipelineId)
                + System.lineSeparator() + "  collections in the target: " + mongo.collections(targetUri)
                + System.lineSeparator() + "  metrics: " + control.metrics(pipelineId)
                + System.lineSeparator() + "  logs: " + control.logs(pipelineId);
    }

    private static List<Document> elementsOf(Document root) {
        Object embedded = root.get(EMBED_PATH);
        if (embedded == null) {
            return List.of();
        }
        if (!(embedded instanceof List<?> list)) {
            throw new AssertionError("document carries '" + EMBED_PATH + "' as "
                    + embedded.getClass().getSimpleName() + ", not an array - " + root);
        }
        List<Document> elements = new ArrayList<>(list.size());
        for (Object element : list) {
            if (!(element instanceof Document document)) {
                throw new AssertionError("an element is not a document: " + element);
            }
            elements.add(document);
        }
        return elements;
    }

    private static Object identityOf(Document document) {
        Object id = document.get("id");
        return id != null ? id : document.get("_id");
    }

    private static long numberOf(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.MIN_VALUE;
    }

    /** More roots than the budget, one child each, so every root has something to lose by sinking. */
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

    /** A child for the root seeded first, which by now has had every chance to be pushed out of memory. */
    private static void insertLateChild(MySQLContainer<?> mysql) throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
                PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO " + CHILD_TABLE + " (id, order_id, sku) VALUES (?, ?, ?)")) {
            insert.setLong(1, LATE_CHILD);
            insert.setLong(2, WOKEN_ROOT);
            insert.setString(3, "sku-late");
            insert.executeUpdate();
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

    /** The nest writes down a budget of its own, which is what puts the layer behind memory into use. */
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
