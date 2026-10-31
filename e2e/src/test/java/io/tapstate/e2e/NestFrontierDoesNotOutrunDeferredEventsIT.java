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
import java.util.Optional;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A change the tree is holding for a parent that has not arrived survives a restart, even though the
 * frontier has already moved past it and the source will never send it again.
 *
 * <p>The frontier does not promise that everything below it has been emitted. It promises that everything
 * below it is either at a sink <em>or held somewhere it survives a restart from</em>. That is what lets a
 * child whose parent has not arrived be taken off the stream: it goes into this level's state, the state is
 * written through to a store, and the bound is free to pass it. Pinning the bound underneath it instead
 * would hold the source's read offset on a row that may never come and burn the source's retention window
 * while every count reads healthy.
 *
 * <p><b>That promise is only as good as its ordering, and only a restart can read it.</b> If the bound is
 * reported before the write-through has settled, nothing looks wrong: the child is still in memory, the
 * document is still correct, the counts are still healthy. The cost is paid later and once - a restart
 * resumes the source <em>above</em> that change, so it is never replayed, and it was never written down
 * either. It is gone from both places at once. The parent arrives afterwards and its document is built
 * without it: one element short, {@code error_count == 0}, {@code state} RUNNING, nothing to see.
 *
 * <p>So the assertions come in two halves and neither works alone:
 *
 * <ul>
 *   <li><b>Before the restart</b> - the frontier for the child's table moves while the child is still
 *       held. Without this the run never entered the state being tested: a level that kept the bound
 *       underneath its held child would make everything after it pass for the wrong reason, since the
 *       source would simply replay the change.</li>
 *   <li><b>After the restart</b> - the parent is inserted and its document contains the child. The child
 *       can only have come from durable state; the stream resumed above it. This is the half that fails
 *       when the bound ran ahead of the write.</li>
 * </ul>
 *
 * <p>The restart is a real one at both fidelities: the server handle is closed and a new one launched
 * against the same store, so the in-process tier drops its whole application context and the real-process
 * tier's process is destroyed. What comes back is a different server that has never seen this pipeline,
 * reading the same store - which is the situation being witnessed.
 *
 * <p>Gated on Docker and on a directory of real connector jars, like its siblings. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=NestFrontierDoesNotOutrunDeferredEventsIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class NestFrontierDoesNotOutrunDeferredEventsIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(180);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final String PARENT_TABLE = "orders";
    private static final String CHILD_TABLE = "order_items";
    private static final String EMBED_PATH = "items";
    private static final String PIPELINE_ID = "deferred_frontier";

    /** Two parents that exist from the start, so the run has something to settle on before the test begins. */
    private static final List<Long> SETTLED_PARENTS = List.of(1L, 2L);

    /** The parent that does not exist while its child arrives, and is inserted only after the restart. */
    private static final long LATE_PARENT = 7L;

    /** The child that waits for it. Its position is the one the frontier has to move past. */
    private static final long DEFERRED_CHILD = 99L;
    private static final String DEFERRED_SKU = "sku-deferred";

    /**
     * How many rows one round of ordinary traffic carries. Several rather than one: a position is published
     * on an observation tick, so a single row can be acked inside the same reading and leave two readings
     * that ought to differ looking identical.
     */
    private static final int TRAFFIC_ROWS = 4;

    /** The two rounds' id ranges, kept apart from each other and from the row sent after the restart. */
    private static final long FIRST_ROUND_FIRST_ID = 1001L;
    private static final long SECOND_ROUND_FIRST_ID = 2001L;

    /** A row sent after the restart, purely to prove the restarted run is reading its source at all. */
    private static final long LIVENESS_CHILD = 3001L;
    private static final String LIVENESS_SKU = "sku-after-restart";

    /** This invocation's pipeline id, carrying the tier so the two do not share one nest's state. */
    private String pipelineId;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aChangeHeldForAnAbsentParentOutlivesTheRunThatWasHoldingIt(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            seedMysql(mysql);
            grantReplication(mysql);

            String suffix = tier.name().toLowerCase(Locale.ROOT);
            pipelineId = PIPELINE_ID + "_" + suffix;
            String targetUri = SharedMongo.replicaSetUrl("deferred_target_" + suffix);
            Map<String, Object> mysqlConfig = mysqlConfig(mysql);

            try (PreparedNativeStore storage = PreparedNativeStore.create(); MongoEndpoints mongo = new MongoEndpoints()) {
                // Both readings outlive the run that took them: the failure message after the restart is
                // only legible beside the two positions that made the restart meaningful.
                String heldAt;
                String movedTo;
                try (ServerHandle server = tier.launch(storage)) {
                    ControlPlane control = new ControlPlane(server.baseUrl());
                    storage.loginAndAwaitBusiness(control);

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

                    // Settle first. Sending the deferred child into a run that has not placed its ordinary
                    // documents yet would leave "held for a missing parent" indistinguishable from "not
                    // assembled yet".
                    awaitDocuments(mongo, targetUri, SETTLED_PARENTS.size());
                    assertThat(rootIds(mongo.documents(EndpointAddress.uri(targetUri), PARENT_TABLE)))
                            .as("the documents that exist before anything is deferred.%n  metrics: %s",
                                    control.metrics(pipelineId))
                            .containsExactlyInAnyOrderElementsOf(SETTLED_PARENTS);

                    // The change that gets held: a child naming a parent row that does not exist.
                    insertChild(mysql, DEFERRED_CHILD, LATE_PARENT, DEFERRED_SKU);

                    // Ordinary traffic behind it, in two rounds. These rows sit after the held one in the
                    // source's log, so the frontier reaching them is the frontier being past it.
                    //
                    // Two rounds because the baseline has to be a real position, and a position exists only
                    // once something has been acked. Read before any of this, there is none at all: a run
                    // that has finished its initial load and has nothing flowing has acked nothing, and the
                    // read face says so by publishing no position rather than a starting one. Measured -
                    // three minutes of waiting for a reading that was never going to appear.
                    List<Long> firstRound = insertTraffic(mysql, FIRST_ROUND_FIRST_ID, TRAFFIC_ROWS);
                    awaitTraffic(mongo, targetUri, firstRound);
                    assertThat(placedTraffic(mongo.documents(EndpointAddress.uri(targetUri), PARENT_TABLE), firstRound))
                            .as("the first round of ordinary traffic has to be through before a frontier "
                                    + "reading means anything.%n  metrics: %s", control.metrics(pipelineId))
                            .containsExactlyInAnyOrderElementsOf(firstRound);
                    heldAt = awaitPosition(control, CHILD_TABLE);

                    List<Long> secondRound = insertTraffic(mysql, SECOND_ROUND_FIRST_ID, TRAFFIC_ROWS);
                    awaitTraffic(mongo, targetUri, secondRound);

                    // Half one, and it is a precondition rather than the point: the bound moves while the
                    // child is still held. A level that pinned the frontier beneath a held child would stop
                    // here, and everything after it would pass for the wrong reason - the source would
                    // simply replay the change.
                    movedTo = awaitPositionPast(control, CHILD_TABLE, heldAt);
                    assertThat(movedTo)
                            .as("the durable position for '%s'. It has to move past a change the tree is "
                                    + "still holding: holding one is not a reason to pin the source's read "
                                    + "offset on a row that may never come.%n  metrics: %s",
                                    CHILD_TABLE, control.metrics(pipelineId))
                            .isNotEqualTo(heldAt);
                    assertThat(elementIds(mongo.documents(EndpointAddress.uri(targetUri), PARENT_TABLE), LATE_PARENT))
                            .as("the deferred child must not be in any document yet - its parent row does "
                                    + "not exist, so there is nothing for it to hang from")
                            .isEmpty();
                    assertThat(control.state(pipelineId))
                            .as("holding a change is not an error state")
                            .contains(PipelineState.RUNNING);
                }

                // The restart. What comes back has never seen this pipeline and reads the same store; the
                // source resumes above the deferred change, so nothing will replay it.
                try (ServerHandle restarted = tier.launch(storage)) {
                    ControlPlane control = new ControlPlane(restarted.baseUrl());
                    // The restarted server reopens the same prepared installation.
                    storage.loginAndAwaitBusiness(control);

                    await(() -> control.state(pipelineId).filter(PipelineState.RUNNING::equals).isPresent());

                    // The state alone does not say the restarted server is doing anything. An observation
                    // outlives the server that wrote it, so right after a restart the read face is still
                    // reporting what the previous one last published - RUNNING, whatever this one is doing.
                    // A row proves it: send one under a parent that already exists and wait for it to land.
                    //
                    // This is what separates the two ways the last assertion can fail. Without it, "the
                    // restarted run never resumed" and "the held change was lost" both read as an absent
                    // document, and the failure message would have to guess which.
                    insertChild(mysql, LIVENESS_CHILD, SETTLED_PARENTS.get(0), LIVENESS_SKU);
                    awaitTraffic(mongo, targetUri, List.of(LIVENESS_CHILD));
                    assertThat(placedTraffic(mongo.documents(EndpointAddress.uri(targetUri), PARENT_TABLE), List.of(LIVENESS_CHILD)))
                            .as("a row sent after the restart has to arrive, or the restarted run is not "
                                    + "reading its source at all and nothing after this means anything.%n"
                                    + "  state: %s%n  metrics: %s%n  logs: %s",
                                    control.state(pipelineId), control.metrics(pipelineId),
                                    control.logs(pipelineId))
                            .containsExactly(LIVENESS_CHILD);

                    // Half two, the discriminating one. The parent finally arrives. The child can only come
                    // from state written before the restart: the stream resumed above it.
                    insertParent(mysql, LATE_PARENT);

                    awaitElement(mongo, targetUri, LATE_PARENT, DEFERRED_CHILD);
                    List<Document> documents = mongo.documents(EndpointAddress.uri(targetUri), PARENT_TABLE);
                    assertThat(elementIds(documents, LATE_PARENT))
                            .as("the document for the late parent, built after a restart. Its child was held "
                                    + "before the restart and the frontier moved past it (%s -> %s), so the "
                                    + "source resumed above it and never sent it again. An empty array here "
                                    + "is a change that was promised durable and was not: reported below the "
                                    + "bound before the write-through settled, gone from the stream and from "
                                    + "the state at once, with nothing reporting it.%n  documents: %s%n"
                                    + "  metrics: %s%n  logs: %s",
                                    heldAt, movedTo, documents,
                                    control.metrics(pipelineId), control.logs(pipelineId))
                            .containsExactly(DEFERRED_CHILD);

                    Document element = elementsOf(documentFor(documents, LATE_PARENT)).get(0);
                    assertThat(element.get("sku"))
                            .as("the payload the held change carried, not a shell rebuilt from its key")
                            .isEqualTo(DEFERRED_SKU);
                    assertThat(control.errorCount(pipelineId))
                            .as("nothing here is an error: the run held a change, restarted, and placed it")
                            .contains(0L);
                }
            }
        }
    }

    private void awaitDocuments(MongoEndpoints mongo, String targetUri, int count) {
        await(() -> mongo.documents(EndpointAddress.uri(targetUri), PARENT_TABLE).size() >= count);
    }

    private void awaitElement(MongoEndpoints mongo, String targetUri, long rootId, long elementId) {
        await(() -> elementIds(mongo.documents(EndpointAddress.uri(targetUri), PARENT_TABLE), rootId).contains(elementId));
    }

    /** Sends one round of ordinary traffic, spread over the parents that exist, and answers what it sent. */
    private static List<Long> insertTraffic(MySQLContainer<?> mysql, long firstId, int rows) throws Exception {
        List<Long> sent = new ArrayList<>(rows);
        for (int row = 0; row < rows; row++) {
            long id = firstId + row;
            insertChild(mysql, id, SETTLED_PARENTS.get(row % SETTLED_PARENTS.size()), "sku-traffic-" + id);
            sent.add(id);
        }
        return sent;
    }

    /** Waits until every one of these element ids is somewhere among the documents. */
    private void awaitTraffic(MongoEndpoints mongo, String targetUri, List<Long> ids) {
        await(() -> placedTraffic(mongo.documents(EndpointAddress.uri(targetUri), PARENT_TABLE), ids).size() == ids.size());
    }

    /** Which of these element ids are present anywhere, whichever document they landed under. */
    private static List<Long> placedTraffic(List<Document> documents, List<Long> ids) {
        List<Long> placed = new ArrayList<>();
        for (Document root : documents) {
            for (Document element : elementsOf(root)) {
                long id = numberOf(identityOf(element));
                if (ids.contains(id)) {
                    placed.add(id);
                }
            }
        }
        return placed;
    }

    /** Waits until a position exists for this table at all, and answers it. */
    private String awaitPosition(ControlPlane control, String table) {
        await(() -> control.durablePosition(pipelineId, table).isPresent());
        Optional<String> position = control.durablePosition(pipelineId, table);
        if (position.isEmpty()) {
            throw new AssertionError("no durable position was ever published for '" + table
                    + "' - without one there is no frontier to watch move. metrics: "
                    + control.metrics(pipelineId));
        }
        return position.get();
    }

    /**
     * Waits until this table's position differs from {@code from}, and answers the new one. Difference
     * rather than order: a position is the connector's own opaque string, and nothing here may assume a
     * shape for it.
     */
    private String awaitPositionPast(ControlPlane control, String table, String from) {
        await(() -> control.durablePosition(pipelineId, table).filter(now -> !now.equals(from)).isPresent());
        return control.durablePosition(pipelineId, table).orElse(from);
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

    private static List<Long> rootIds(List<Document> documents) {
        List<Long> ids = new ArrayList<>(documents.size());
        for (Document document : documents) {
            ids.add(numberOf(identityOf(document)));
        }
        return ids;
    }

    /** The ids of the elements under one root, in array order, or empty when the root is not there at all. */
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

    private static Document documentFor(List<Document> documents, long rootId) {
        Document document = documentOrNull(documents, rootId);
        if (document == null) {
            throw new AssertionError("no document for root " + rootId + " among " + documents);
        }
        return document;
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

    /** Two parents with a child each, and no row for the parent this test defers a child on. */
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
                for (long id : SETTLED_PARENTS) {
                    insert.setLong(1, id);
                    insert.setString(2, "order-" + id);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + CHILD_TABLE + " (id, order_id, sku) VALUES (?, ?, ?)")) {
                for (long id : SETTLED_PARENTS) {
                    insert.setLong(1, id);
                    insert.setLong(2, id);
                    insert.setString(3, "sku-" + id);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }
    }

    private static void insertChild(MySQLContainer<?> mysql, long id, long parentId, String sku)
            throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(
                                mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
                PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO " + CHILD_TABLE + " (id, order_id, sku) VALUES (?, ?, ?)")) {
            insert.setLong(1, id);
            insert.setLong(2, parentId);
            insert.setString(3, sku);
            insert.executeUpdate();
        }
    }

    private static void insertParent(MySQLContainer<?> mysql, long id) throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(
                                mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
                PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO " + PARENT_TABLE + " (id, name) VALUES (?, ?)")) {
            insert.setLong(1, id);
            insert.setString(2, "order-" + id);
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

    private static void sleep() {
        try {
            Thread.sleep(POLL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for the run to settle", e);
        }
    }
}
