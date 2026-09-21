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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A child row that changes which parent it hangs under leaves the document it was in and arrives in the
 * other one - both halves of the move, over two real sources.
 *
 * <p>This is the first witness that a structural key change is carried end to end rather than only through
 * the state layer. The join key is the key saying which parent an element belongs to, so changing it is not
 * an update to the element: the element has to be taken out of one document and put into another, and the
 * two documents are assembled by different keys and so, in general, on different instances.
 *
 * <p>What the assertions have to discriminate: asserting only that the new parent gained the element is
 * satisfied by an implementation that attaches without detaching - which leaves the element in both
 * documents at once, the ghost this feature exists to prevent. The old parent's array is therefore read
 * too, and read for emptiness rather than for a smaller count, because "one fewer" would also be satisfied
 * by an implementation that removed the wrong element. Both documents are read after the move, in the same
 * observation, since a run that settles one before the other would let a stale read pass for either.
 *
 * <p>The embed carries {@code trackKeyChanges}, without which the product is documented to leave the ghost
 * behind rather than to fail - so a run of this witness with the flag off is expected to fail on the old
 * parent's array, which is what makes the flag's presence here load-bearing rather than decorative.
 *
 * <p>Gated on Docker and on a directory of real connector jars, like its siblings. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors -Dit.test=NestReparentMovesTheChildIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class NestReparentMovesTheChildIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(90);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final String PARENT_TABLE = "orders";
    private static final String CHILD_TABLE = "order_items";
    private static final String PIPELINE_ID = "reparented_orders";

    /**
     * This invocation's pipeline id, which carries the tier so the two tiers do not share a nest's state.
     *
     * <p>A nest keeps its state in the deployment's configured database, addressed by a namespace built
     * from the pipeline and step ids. Both tiers in this witness use the same configured database, so
     * giving each its own control store and target still leaves operator state shared, so the second tier
     * starts on the state the first one
     * finished with. A witness that ends where its own snapshot would have put it cannot see this; one
     * that changes something can.
     *
     * <p>The base has to be its own too, for the same reason one level up: three witnesses here shared
     * one id, and the tier suffix left them sharing it still. Nothing collides while only one of them
     * is ever run.
     */
    private String pipelineId;
    private static final String EMBED_PATH = "items";

    private static final long OLD_PARENT = 1;
    private static final long NEW_PARENT = 2;
    private static final long CHILD_ID = 1;
    private static final String CHILD_SKU = "sku-1";

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aChildWhoseParentKeyChangesLeavesOneDocumentAndArrivesInTheOther(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            seedMysql(mysql);
            grantReplication(mysql);

            String suffix = tier.name().toLowerCase(Locale.ROOT);
            pipelineId = PIPELINE_ID + "_" + suffix;
            String storeUri = SharedMongo.replicaSetUrl("reparent_store_" + suffix);
            String targetUri = SharedMongo.replicaSetUrl("reparent_target_" + suffix);

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

                // Settle before moving anything: a move applied while the element had not yet been placed
                // would be witnessing the initial assembly a second time rather than a re-parent. Both
                // documents have to be present, not merely the first - an absent document and an empty
                // array read the same through the arrays alone, so waiting on the arrays would let the
                // move start before the destination existed.
                List<Document> before = await(mongo, targetUri, NestReparentMovesTheChildIT::placed);
                if (!placed(before)) {
                    throw new AssertionError(diagnose(control, mongo, targetUri,
                            "the element never landed under its first parent", before));
                }

                reparent(mysql);

                List<Document> after = await(mongo, targetUri, NestReparentMovesTheChildIT::moved);
                if (!moved(after)) {
                    throw new AssertionError(diagnose(control, mongo, targetUri,
                            "the element did not move from one document to the other", after));
                }
                assertMoved(after);
            }
        }
    }

    /** Both roots present, the child assembled under the first of them and the other still empty. */
    private static boolean placed(List<Document> documents) {
        return documents.size() == 2
                && elementIds(documents, OLD_PARENT).equals(List.of(CHILD_ID))
                && elementIds(documents, NEW_PARENT).isEmpty();
    }

    /** Both roots present, the child now under the second of them and nothing left under the first. */
    private static boolean moved(List<Document> documents) {
        return documents.size() == 2
                && elementIds(documents, NEW_PARENT).equals(List.of(CHILD_ID))
                && elementIds(documents, OLD_PARENT).isEmpty();
    }

    /**
     * The discriminating half, read once over both documents. The arrival on its own is satisfied by an
     * implementation that never detaches; the departure on its own is satisfied by one that drops the
     * element entirely. Only the pair says the element moved.
     */
    private static void assertMoved(List<Document> documents) {
        assertThat(elementIds(documents, OLD_PARENT))
                .as("elements left under document %d, the parent the child no longer names", OLD_PARENT)
                .isEmpty();

        List<Document> arrived = elementsOf(documentFor(documents, NEW_PARENT));
        assertThat(arrived)
                .as("elements under document %d, the parent the child now names", NEW_PARENT)
                .hasSize(1);
        assertThat(numberOf(identityOf(arrived.get(0))))
                .as("the element that arrived is the one that left, not a second copy")
                .isEqualTo(CHILD_ID);
        assertThat(arrived.get(0).get("sku"))
                .as("the payload the moved element carried with it")
                .isEqualTo(CHILD_SKU);
        assertThat(numberOf(arrived.get(0).get("order_id")))
                .as("the parent the moved element itself names")
                .isEqualTo(NEW_PARENT);
    }

    /** The ids of the elements under one root, in array order, or empty when the root holds none. */
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

    private List<Document> await(
            MongoEndpoints mongo, String targetUri, java.util.function.Predicate<List<Document>> settled) {
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

    private String diagnose(
            ControlPlane control, MongoEndpoints mongo, String targetUri, String what,
            List<Document> documents) {
        return what + ": '" + PARENT_TABLE + "' holds " + documents
                + System.lineSeparator() + "  pipeline state: " + control.state(pipelineId)
                + ", error count: " + control.errorCount(pipelineId)
                + System.lineSeparator() + "  collections in the target: " + mongo.collections(targetUri)
                + System.lineSeparator() + "  metrics: " + control.metrics(pipelineId)
                + System.lineSeparator() + "  logs: " + control.logs(pipelineId);
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

    /** Two parents and one leaf child, the child hanging under the first of them. */
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
                insert.setString(3, CHILD_SKU);
                insert.executeUpdate();
            }
        }
    }

    /** The move itself: the child keeps its own identity and names the other parent. */
    private static void reparent(MySQLContainer<?> mysql) throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
                PreparedStatement update = connection.prepareStatement(
                        "UPDATE " + CHILD_TABLE + " SET order_id = ? WHERE id = ?")) {
            update.setLong(1, NEW_PARENT);
            update.setLong(2, CHILD_ID);
            update.executeUpdate();
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

    /** The embed tracks its structural keys, which is what turns a changed join key into a move. */
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
            throw new AssertionError("interrupted while waiting for the documents to settle", e);
        }
    }
}
