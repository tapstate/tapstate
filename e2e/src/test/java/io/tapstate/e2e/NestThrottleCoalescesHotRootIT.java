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
 * A document changing faster than it can usefully be sent is sent fewer times than it changed, and a
 * document changing slowly is not made to wait for that.
 *
 * <p>A nest re-sends the whole document for every change beneath it, so a root that many rows hang from
 * costs its whole size per row. What holds that down is a window: the first change of a quiet document
 * goes out on the spot and opens one, changes arriving inside it only mark the document dirty, and the
 * window's end sends whatever the document became. The write amplification is not removed - it is the
 * price of a sink that does not have to understand the tree - but the send rate is bounded.
 *
 * <p><b>Two ways to get this wrong, in opposite directions, and one assertion cannot see both.</b> A
 * window that never engages costs one send per change, which is the amplification with nothing holding
 * it down. A window applied to everything makes a document that changes once every few seconds wait for
 * a window that exists for documents changing hundreds of times a second - latency spent to save
 * nothing. So the two halves here are a hot root and a quiet one, and each is read as a count of sends
 * rather than of changes.
 *
 * <p>The counts are taken as differences across a phase, because the reading is one number for the whole
 * pipeline and what is being asked about is one root's share of it. Running the phases one after the
 * other, with only one root changing in each, is what makes the difference attributable.
 *
 * <p>The quiet half is the discriminating one and the fragile one: it asserts an exact count, and it can
 * only do that because its changes are spaced far wider than the window. Spaced close to it, ordinary
 * delivery jitter would put two in one window and cost a send that nothing is wrong with.
 *
 * <p>Gated on Docker and on a directory of real connector jars, like its siblings. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=NestThrottleCoalescesHotRootIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class NestThrottleCoalescesHotRootIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(180);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final String PARENT_TABLE = "orders";
    private static final String CHILD_TABLE = "order_items";
    private static final String EMBED_PATH = "items";
    private static final String PIPELINE_ID = "throttle_hot_root";

    /** The window the product runs on by default, and the one this witness is written against. */
    private static final long WINDOW_MILLIS = 50L;

    /**
     * How long the count of sends must hold still to count as settled, and how often it is asked. The
     * window is wider than the engine's metric collection interval on purpose - see awaitSettledSends.
     */
    private static final Duration SETTLE_WINDOW = Duration.ofSeconds(8);
    private static final Duration SETTLE_POLL = Duration.ofMillis(500);

    private static final long HOT_ROOT = 1L;
    private static final long QUIET_ROOT = 2L;

    /** How many changes the hot root takes. Far more than any window could let through in the time. */
    private static final int HOT_CHANGES = 2_000;

    /**
     * Sends allowed beyond what the window arithmetic gives, to absorb the coarseness of the reading
     * rather than any real slack: the count is published on an observation tick, and the phase's own
     * boundaries are wall-clock readings taken around it.
     */
    private static final int HOT_MARGIN = 40;

    /**
     * The quiet root's changes, and the gap between them. The gap is an order of magnitude wider than
     * the window on purpose - see the class note. Kept few because each one costs its own gap.
     */
    private static final int QUIET_CHANGES = 5;
    private static final Duration QUIET_GAP = Duration.ofMillis(600);

    /** This invocation's pipeline id, carrying the tier so the two do not share one nest's state. */
    private String pipelineId;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aDocumentChangingFasterThanItIsSentIsSentFewerTimesThanItChanged(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            seedMysql(mysql);
            grantReplication(mysql);

            String suffix = tier.name().toLowerCase(Locale.ROOT);
            pipelineId = PIPELINE_ID + "_" + suffix;
            String storeUri = SharedMongo.replicaSetUrl("throttle_store_" + suffix);
            String targetUri = SharedMongo.replicaSetUrl("throttle_target_" + suffix);
            Map<String, Object> mysqlConfig = mysqlConfig(mysql);

            try (ServerHandle server = tier.launch(storeUri);
                    MongoEndpoints mongo = new MongoEndpoints()) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");

                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

                Map<String, String> resources = new LinkedHashMap<>();
                resources.put("src_orders.tap.yml", sourceYaml("src_orders", PARENT_TABLE, mysqlConfig));
                resources.put("src_items.tap.yml", sourceYaml("src_items", CHILD_TABLE, mysqlConfig));
                resources.put("tgt_mongo.tap.yml", targetYaml(targetUri));
                resources.put("pipeline.tap.yml", pipelineYaml(pipelineId));
                control.apply(resources);

                control.discoverSchema("src_orders", "mysql", mysqlConfig);
                control.discoverSchema("src_items", "mysql", mysqlConfig);

                control.lifecycle(pipelineId, LifecycleVerb.START);

                // Settle before measuring: the initial load's own sends are not what is being counted.
                awaitDocuments(mongo, targetUri, 2);

                // ---- The hot half ----------------------------------------------------------------
                long beforeHot = awaitSettledSends(control);
                long hotStarted = System.nanoTime();
                insertChildren(mysql, HOT_ROOT, 1_000L, HOT_CHANGES);
                awaitElements(mongo, targetUri, HOT_ROOT, HOT_CHANGES + 1); // + the row seeded with it
                long hotElapsedMillis = Duration.ofNanos(System.nanoTime() - hotStarted).toMillis();
                long afterHot = awaitSettledSends(control);
                long hotSends = afterHot - beforeHot;

                // What the window allows over the time this actually took, plus room for the reading.
                long allowed = hotElapsedMillis / WINDOW_MILLIS + HOT_MARGIN;

                assertThat(hotSends)
                        .as("sends for %d changes to one document over %d ms. A window of %d ms allows "
                                + "about %d; one send per change would be %d, which is the amplification "
                                + "with nothing holding it down.%n  metrics: %s",
                                HOT_CHANGES, hotElapsedMillis, WINDOW_MILLIS, allowed, HOT_CHANGES,
                                control.metrics(pipelineId))
                        .isLessThanOrEqualTo(allowed);

                // Folding sends is only allowed to cost sends. The document has to be what it would have
                // been had every change been sent on its own.
                List<Document> hotDocuments = mongo.documents(EndpointAddress.uri(targetUri), PARENT_TABLE);
                assertThat(elementIds(hotDocuments, HOT_ROOT))
                        .as("the hot document after coalescing has to hold every change that was made to "
                                + "it - what is folded is the sending, never the content")
                        .hasSize(HOT_CHANGES + 1);

                // ---- The quiet half --------------------------------------------------------------
                // Starts from the hot half's settled reading rather than a fresh one: they are the same
                // moment, and taking it twice would only give the second one a chance to be different.
                for (int change = 0; change < QUIET_CHANGES; change++) {
                    insertChildren(mysql, QUIET_ROOT, 500_000L + change, 1);
                    sleep(QUIET_GAP);
                }
                awaitElements(mongo, targetUri, QUIET_ROOT, QUIET_CHANGES + 1);
                long quietSends = awaitSettledSends(control) - afterHot;

                assertThat(quietSends)
                        .as("sends for %d changes spaced %d ms apart, against a %d ms window. Each one "
                                + "arrives to a document that has been quiet for far longer than the "
                                + "window, so each goes out on its own: fewer sends than changes here is "
                                + "latency spent on a document that was never costing anything.%n"
                                + "  metrics: %s", QUIET_CHANGES, QUIET_GAP.toMillis(), WINDOW_MILLIS,
                                control.metrics(pipelineId))
                        .isEqualTo(QUIET_CHANGES);

                assertThat(control.state(pipelineId))
                        .as("none of this is an error state")
                        .contains(PipelineState.RUNNING);
                assertThat(control.errorCount(pipelineId)).contains(0L);
            }
        }
    }

    private long recordCount(ControlPlane control) {
        return control.recordCount(pipelineId).orElseThrow(() -> new AssertionError(
                "no record count is published for " + pipelineId + " - there is no reading of sends to "
                        + "compare. metrics: " + control.metrics(pipelineId)));
    }

    /**
     * Waits until the count of sends stops moving, and answers it. Every reading here has to be taken
     * this way, because the count is not published as it happens: it is whatever the job's metrics said
     * at their last collection, and collection is periodic. A reading taken the moment a phase's last
     * document lands is therefore a reading from before that phase finished, and the sends it missed do
     * not disappear - they turn up inside the next phase's difference and are attributed to it.
     *
     * <p>Measured: without this, a quiet phase of five changes was charged ninety-four sends, and the
     * hot phase it borrowed them from passed its own bound by being under-reported. Both halves were
     * wrong at once and both looked like data.
     *
     * <p>Stability is judged over a window wider than the collection interval, so "unchanged" means
     * unchanged across at least one collection rather than merely twice inside the same one.
     */
    private long awaitSettledSends(ControlPlane control) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        // Target delivery can precede the first asynchronous observation; only a real count starts the comparison.
        var first = control.recordCount(pipelineId);
        while (first.isEmpty() && System.nanoTime() - deadline < 0) {
            sleep(SETTLE_POLL);
            first = control.recordCount(pipelineId);
        }
        long last = first.orElseThrow(() -> new AssertionError(
                "no record count was published within the settlement deadline for " + pipelineId
                        + "; metrics: " + control.metrics(pipelineId)));
        long steadyFor = 0;
        while (System.nanoTime() - deadline < 0) {
            sleep(SETTLE_POLL);
            long now = recordCount(control);
            steadyFor = now == last ? steadyFor + SETTLE_POLL.toMillis() : 0;
            last = now;
            if (steadyFor >= SETTLE_WINDOW.toMillis()) {
                return last;
            }
        }
        throw new AssertionError("the count of sends never stopped moving for " + pipelineId
                + " - with no quiet reading, no phase's difference is attributable to that phase. "
                + "metrics: " + control.metrics(pipelineId));
    }

    private void awaitDocuments(MongoEndpoints mongo, String targetUri, int count) {
        await(() -> mongo.documents(EndpointAddress.uri(targetUri), PARENT_TABLE).size() >= count);
    }

    private void awaitElements(MongoEndpoints mongo, String targetUri, long rootId, int count) {
        await(() -> elementIds(mongo.documents(EndpointAddress.uri(targetUri), PARENT_TABLE), rootId).size() >= count);
    }

    private static void await(BooleanSupplier reached) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        while (System.nanoTime() - deadline < 0) {
            if (reached.getAsBoolean()) {
                return;
            }
            sleep(POLL);
        }
    }

    /** The ids of the elements under one root, or empty when the root is not there at all. */
    private static List<Long> elementIds(List<Document> documents, long rootId) {
        Document root = documentOrNull(documents, rootId);
        if (root == null) {
            return List.of();
        }
        List<Long> ids = new ArrayList<>();
        for (Document element : elementsOf(root)) {
            ids.add(numberOf(identityOf(element)));
        }
        return ids;
    }

    private static Document documentOrNull(List<Document> documents, long rootId) {
        for (Document document : documents) {
            if (numberOf(identityOf(document)) == rootId) {
                return document;
            }
        }
        return null;
    }

    /** The embedded array of a document; an absent path reads as an empty array rather than as a crash. */
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

    /** Two roots, one child each, so both documents exist before either is measured. */
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
                for (long id : List.of(HOT_ROOT, QUIET_ROOT)) {
                    insert.setLong(1, id);
                    insert.setString(2, "order-" + id);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + CHILD_TABLE + " (id, order_id, sku) VALUES (?, ?, ?)")) {
                for (long id : List.of(HOT_ROOT, QUIET_ROOT)) {
                    insert.setLong(1, id);
                    insert.setLong(2, id);
                    insert.setString(3, "sku-" + id);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }
    }

    /** Sends {@code rows} children under one root, each its own change to that root's document. */
    private static void insertChildren(MySQLContainer<?> mysql, long rootId, long firstId, int rows)
            throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(
                                mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
                PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO " + CHILD_TABLE + " (id, order_id, sku) VALUES (?, ?, ?)")) {
            for (int row = 0; row < rows; row++) {
                long id = firstId + row;
                insert.setLong(1, id);
                insert.setLong(2, rootId);
                insert.setString(3, "sku-" + id);
                insert.addBatch();
            }
            insert.executeBatch();
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
                        - from: i
                          on: { order_id: id }
                          as: array
                          path: items
                          arrayKey: [ id ]
                serve:
                  from: order_doc
                  sync:
                    - source: tgt_mongo
                """
                .formatted(pipelineId);
    }

    private static void sleep(Duration howLong) {
        try {
            Thread.sleep(howLong.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for the run to settle", e);
        }
    }
}
