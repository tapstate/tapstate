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
 * The witness that a nest assembles whole documents from two real sources - the first time the
 * stateful half of the product carries anything end to end.
 *
 * <p>Everything before this proved a nest could be built: the tree compiles, the vertices are drawn,
 * the DAG is accepted. None of it proved a document ever comes out. Two real single-table capture
 * units feed two separate chains into one job here, and what lands in Mongo is read back by something
 * that is not the product.
 *
 * <p>What the assertions have to discriminate: counting the documents is not enough. A nest that never
 * ran at all - roots passed straight through, every array empty - produces exactly the two documents a
 * count expects. So the arrays are read: the root that owns three children must hold three, the root
 * that owns one must hold one, and each element's fields must match the row it came from. The second
 * root is what separates "children were attached" from "children were attached to whoever came first",
 * which an implementation ignoring the join key satisfies while the first root's assertion still passes.
 *
 * <p>Read mode is {@code snapshot_and_cdc} rather than {@code snapshot_only} deliberately: a stateful
 * node needs every row to carry its order, and a source reading no chain of its own supplies none. The
 * seeded rows still arrive as snapshot reads; the change stream is what puts them on a chain.
 *
 * <p>Gated on Docker and on a directory of real connector jars, like its flat siblings
 * {@link RealMysqlToMongoSnapshotIT} and {@link RealMysqlToMongoCdcIT}. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors -Dit.test=NestAssemblesParentAndChildrenIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 *
 * <p>The last flag is not optional and the run is not skippable with {@code -DskipTests}. Building the
 * reactor up to this module runs every upstream module's integration phase too, and a name that matches
 * nothing there ends the build before this test is reached; {@code -DskipTests} skips the integration
 * tests along with the unit tests, so the run reports success having executed nothing.
 */
class NestAssemblesParentAndChildrenIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(90);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final String PARENT_TABLE = "orders";
    private static final String CHILD_TABLE = "order_items";
    private static final String PIPELINE_ID = "nested_orders";
    private static final String EMBED_PATH = "items";

    /**
     * Two roots, one holding three children and one holding a single child. Both counts matter: three
     * catches an array that never filled, and one catches an array that filled from the wrong parent.
     */
    private static final List<Item> ITEMS = List.of(
            new Item(1, 1, "sku-1"),
            new Item(2, 1, "sku-2"),
            new Item(3, 1, "sku-3"),
            new Item(4, 2, "sku-4"));

    private static final long ROOT_COUNT = 2;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aNestOverTwoRealSourcesAssemblesItsChildrenIntoTheParentDocument(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            seedMysql(mysql);
            grantReplication(mysql);

            // One store and one target per tier: sharing them would let a later tier read the documents
            // an earlier one already landed and pass without the nest assembling a thing.
            String suffix = tier.name().toLowerCase(Locale.ROOT);
            String storeUri = SharedMongo.replicaSetUrl("nest_store_" + suffix);
            String targetUri = SharedMongo.replicaSetUrl("nest_target_" + suffix);

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
                resources.put("pipeline.tap.yml", pipelineYaml());
                control.apply(resources);

                // Both sources are discovered: the parent's model resolves the target the sink writes,
                // and the child's is where an embedded element's own key comes from.
                control.discoverSchema("src_orders", "mysql", mysqlConfig);
                control.discoverSchema("src_items", "mysql", mysqlConfig);

                control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

                List<Document> documents = awaitDocuments(mongo, targetUri);
                if (!settled(documents)) {
                    throw new AssertionError(diagnose(control, mongo, targetUri, documents));
                }
                assertAssembled(documents);
            }
        }
    }

    /**
     * Waits for the target to settle at the shape the assertions describe, then hands over what it
     * holds. Waiting on the root count alone would read a document whose array is still filling - the
     * children arrive on the other chain, after the root that carries them - and report a timing gap as
     * a wrong document. On timeout the last observation is handed over anyway, so the failure is the
     * real difference rather than "waited too long".
     */
    private static List<Document> awaitDocuments(MongoEndpoints mongo, String targetUri) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        List<Document> last = List.of();
        while (System.nanoTime() - deadline < 0) {
            last = mongo.documents(targetUri, PARENT_TABLE);
            if (settled(last)) {
                return last;
            }
            sleep();
        }
        return last;
    }

    /**
     * What the target and the pipeline actually look like when the wait ran out. "Zero documents" names
     * a symptom shared by a pipeline that failed, a pipeline still starting, and rows written to a
     * collection nobody looked in - so the reading that separates them is taken here rather than left
     * for a rerun with more logging.
     */
    private static String diagnose(
            ControlPlane control, MongoEndpoints mongo, String targetUri, List<Document> documents) {
        return "the nest never assembled what was seeded: expected " + ROOT_COUNT + " documents holding "
                + ITEMS.size() + " elements between them, and '" + PARENT_TABLE + "' holds " + documents
                + System.lineSeparator() + "  pipeline state: " + control.state(PIPELINE_ID)
                + ", error count: " + control.errorCount(PIPELINE_ID)
                + System.lineSeparator() + "  collections in the target: " + mongo.collections(targetUri)
                + System.lineSeparator() + "  metrics: " + control.metrics(PIPELINE_ID)
                + System.lineSeparator() + "  logs: " + control.logs(PIPELINE_ID);
    }

    /** Whether every root is present and every child is attached somewhere, so waiting can stop. */
    private static boolean settled(List<Document> documents) {
        if (documents.size() != ROOT_COUNT) {
            return false;
        }
        int attached = 0;
        for (Document document : documents) {
            if (!(document.get(EMBED_PATH) instanceof List<?> elements)) {
                return false;
            }
            attached += elements.size();
        }
        return attached == ITEMS.size();
    }

    /**
     * The discriminating half. Each root's array must hold exactly the children whose foreign key names
     * it, with the fields those rows carried - the whole of what assembling a document means, and none
     * of it visible to a count.
     */
    private static void assertAssembled(List<Document> documents) {
        assertThat(documents)
                .as("documents in the Mongo target %s", PARENT_TABLE)
                .hasSize((int) ROOT_COUNT);

        for (long rootId : List.of(1L, 2L)) {
            Document root = documentFor(documents, rootId);
            assertThat(root.get("name"))
                    .as("the root fields of document %d, which an array-only document would not carry", rootId)
                    .isEqualTo("order-" + rootId);

            List<Document> elements = elementsOf(root, rootId);
            List<Item> expected = ITEMS.stream().filter(item -> item.orderId() == rootId).toList();
            assertThat(elements)
                    .as("elements at '%s' of document %d, whose children are %s", EMBED_PATH, rootId, expected)
                    .hasSize(expected.size());

            for (Item item : expected) {
                Document element = elementFor(elements, item.id(), rootId);
                assertThat(numberOf(element.get("order_id")))
                        .as("the parent named by element %d of document %d", item.id(), rootId)
                        .isEqualTo(item.orderId());
                assertThat(element.get("sku"))
                        .as("the payload of element %d of document %d", item.id(), rootId)
                        .isEqualTo(item.sku());
            }
        }
    }

    /** The document for a root, addressed by the key field the parent table declares. */
    private static Document documentFor(List<Document> documents, long rootId) {
        for (Document document : documents) {
            if (numberOf(identityOf(document)) == rootId) {
                return document;
            }
        }
        throw new AssertionError("no document for root " + rootId + " among " + documents);
    }

    /** The element of an array, addressed by the child table's own key. */
    private static Document elementFor(List<Document> elements, long elementId, long rootId) {
        for (Document element : elements) {
            if (numberOf(identityOf(element)) == elementId) {
                return element;
            }
        }
        throw new AssertionError(
                "no element " + elementId + " in document " + rootId + " among " + elements);
    }

    /**
     * The embedded array of a document. An absent path is the failure this test exists to catch - the
     * root reached the sink and nothing was ever attached to it - so it is reported as that rather than
     * as a null dereference.
     */
    private static List<Document> elementsOf(Document root, long rootId) {
        Object embedded = root.get(EMBED_PATH);
        if (embedded == null) {
            throw new AssertionError("document " + rootId + " carries no '" + EMBED_PATH
                    + "': the root reached the sink with nothing assembled under it - " + root);
        }
        if (!(embedded instanceof List<?> list)) {
            throw new AssertionError("document " + rootId + " carries '" + EMBED_PATH
                    + "' as " + embedded.getClass().getSimpleName() + ", not an array - " + root);
        }
        List<Document> elements = new ArrayList<>(list.size());
        for (Object element : list) {
            if (!(element instanceof Document document)) {
                throw new AssertionError("an element of document " + rootId + " is not a document: " + element);
            }
            elements.add(document);
        }
        return elements;
    }

    /** A row's identity as the target holds it, whether the sink kept the source field or mapped it. */
    private static Object identityOf(Document document) {
        Object id = document.get("id");
        return id != null ? id : document.get("_id");
    }

    /** MySQL integers reach Mongo as one of several widths; compare them as one. */
    private static long numberOf(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.MIN_VALUE;
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
                for (long id = 1; id <= ROOT_COUNT; id++) {
                    insert.setLong(1, id);
                    insert.setString(2, "order-" + id);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + CHILD_TABLE + " (id, order_id, sku) VALUES (?, ?, ?)")) {
                for (Item item : ITEMS) {
                    insert.setLong(1, item.id());
                    insert.setLong(2, item.orderId());
                    insert.setString(3, item.sku());
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }
    }

    /** Binlog CDC needs replication privileges the default test user lacks; grant them as root. */
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

    /** One capture unit per table: two single-table sources are what puts two chains into one job. */
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

    /**
     * The parent source leads the list: the target model a sink writes is resolved from the first source
     * whose schema is discovered, and the document being assembled is the parent's.
     */
    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: nested_orders
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
                """;
    }

    private static void sleep() {
        try {
            Thread.sleep(POLL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for the assembled documents", e);
        }
    }

    /** One child row, as it is seeded and as it must come back inside its parent's array. */
    private record Item(long id, long orderId, String sku) {
    }
}
