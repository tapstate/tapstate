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
 * The witness that a change to a child row moves the array inside an already assembled document, one
 * operation at a time - an insert, an update and a delete, each read back before the next is made.
 *
 * <p>Its siblings all assemble once from seeded rows and stop. What a nest is for is the second event
 * onwards: a document that exists is rewritten in place as its children change, and every failure mode
 * worth catching lives in how that rewrite happens rather than in whether it happens at all.
 *
 * <p>What the assertions have to discriminate:
 * <ul>
 *   <li><b>The update is in place.</b> An implementation that removes the old element and appends a new
 *       one produces an array with the right contents and the wrong order, and - between the two halves -
 *       a length that dips. So the order is captured before the update and required to be identical
 *       after it, element for element.</li>
 *   <li><b>The delete removes the one named.</b> Asserting the length alone is satisfied by deleting the
 *       wrong element, so every survivor is checked to be untouched: same ids in the same order, same
 *       payloads.</li>
 *   <li><b>The insert reaches an existing document.</b> A nest that only assembles at start-up leaves the
 *       array at three forever, which no assertion about contents would notice if the contents were only
 *       read once.</li>
 * </ul>
 *
 * <p>The order the seeded rows first arrive in is not asserted - a snapshot read is free to return them
 * in any order. What is asserted is that the order does not change once it exists, which is the property
 * the in-place claim is actually about.
 *
 * <p>Gated on Docker and on a directory of real connector jars, like its flat siblings
 * {@link RealMysqlToMongoSnapshotIT} and {@link RealMysqlToMongoCdcIT}. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors -Dit.test=NestChildCdcMutatesTheArrayIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 *
 * <p>The last flag is not optional and the run is not skippable with {@code -DskipTests}. Building the
 * reactor up to this module runs every upstream module's integration phase too, and a name that matches
 * nothing there ends the build before this test is reached; {@code -DskipTests} skips the integration
 * tests along with the unit tests, so the run reports success having executed nothing.
 */
class NestChildCdcMutatesTheArrayIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(120);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final String PARENT_TABLE = "orders";
    private static final String CHILD_TABLE = "order_items";
    private static final String PIPELINE_ID = "nested_orders";
    private static final String EMBED_PATH = "items";
    private static final long ROOT_ID = 1;

    /** The rows the snapshot brings, before any change is made. */
    private static final List<Item> SEEDED =
            List.of(new Item(1, "sku-1"), new Item(2, "sku-2"), new Item(3, "sku-3"));

    private static final Item INSERTED = new Item(4, "sku-4");
    private static final long UPDATED_ID = 2;
    private static final String UPDATED_SKU = "sku-2-changed";
    private static final long DELETED_ID = 3;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void insertingUpdatingAndDeletingAChildRewritesTheArrayInPlace(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            seedMysql(mysql);
            grantReplication(mysql);

            // One store and one target per tier: sharing them would let a later tier read the documents
            // an earlier one already landed and pass without the nest assembling a thing.
            String suffix = tier.name().toLowerCase(Locale.ROOT);
            String storeUri = SharedMongo.replicaSetUrl("cdc_store_" + suffix);
            String targetUri = SharedMongo.replicaSetUrl("cdc_target_" + suffix);

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

                control.discoverSchema("src_orders", "mysql", mysqlConfig);
                control.discoverSchema("src_items", "mysql", mysqlConfig);

                control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

                // 1. The snapshot assembles what was seeded.
                Document assembled = await(control, mongo, targetUri,
                        document -> idsOf(elementsOf(document)).size() == SEEDED.size(),
                        "the seeded children never assembled");
                assertPayloads(assembled, SEEDED);

                // 2. An insert reaches a document that already exists.
                insertItem(mysql, INSERTED);
                Document grown = await(control, mongo, targetUri,
                        document -> idsOf(elementsOf(document)).contains(INSERTED.id()),
                        "the inserted child never reached the assembled document");
                List<Long> orderBefore = idsOf(elementsOf(grown));
                assertThat(orderBefore)
                        .as("the array after the insert holds every child and nothing else")
                        .containsExactlyInAnyOrder(1L, 2L, 3L, 4L);
                assertPayloads(grown, withPayload(SEEDED, INSERTED));

                // 3. The update rewrites one element without disturbing the array around it.
                updateItemSku(mysql, UPDATED_ID, UPDATED_SKU);
                Document changed = await(control, mongo, targetUri,
                        document -> UPDATED_SKU.equals(skuOf(document, UPDATED_ID)),
                        "the updated child never changed inside the document");
                assertThat(idsOf(elementsOf(changed)))
                        .as("an update that removed and re-appended would move element %d to the end", UPDATED_ID)
                        .containsExactlyElementsOf(orderBefore);
                assertPayloads(changed, expectedAfterUpdate());

                // 4. The delete removes exactly the element named.
                deleteItem(mysql, DELETED_ID);
                Document shrunk = await(control, mongo, targetUri,
                        document -> !idsOf(elementsOf(document)).contains(DELETED_ID),
                        "the deleted child never left the document");
                List<Long> orderAfter = new ArrayList<>(orderBefore);
                orderAfter.remove(DELETED_ID);
                assertThat(idsOf(elementsOf(shrunk)))
                        .as("only element %d leaves, and the survivors keep their places", DELETED_ID)
                        .containsExactlyElementsOf(orderAfter);
                assertPayloads(shrunk, expectedAfterDelete());
            }
        }
    }

    /** What the array must hold once the update has landed: the same rows, one of them carrying new text. */
    private static List<Item> expectedAfterUpdate() {
        List<Item> expected = new ArrayList<>();
        for (Item item : withPayload(SEEDED, INSERTED)) {
            expected.add(item.id() == UPDATED_ID ? new Item(item.id(), UPDATED_SKU) : item);
        }
        return expected;
    }

    /** What the array must hold once the delete has landed: everything above, minus the row named. */
    private static List<Item> expectedAfterDelete() {
        return expectedAfterUpdate().stream().filter(item -> item.id() != DELETED_ID).toList();
    }

    private static List<Item> withPayload(List<Item> base, Item extra) {
        List<Item> all = new ArrayList<>(base);
        all.add(extra);
        return all;
    }

    /**
     * Waits for the document to reach a shape, then hands it over. Each step waits for its own change
     * rather than for a fixed pause: a change stream has no deadline, and a sleep long enough to be safe
     * is a sleep long enough to hide a rewrite that happened twice. On timeout the pipeline is read so
     * the failure says what it was doing rather than only that it took too long.
     */
    private static Document await(ControlPlane control, MongoEndpoints mongo, String targetUri,
            Predicate<Document> reached, String what) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        Document last = null;
        while (System.nanoTime() - deadline < 0) {
            List<Document> documents = mongo.documents(targetUri, PARENT_TABLE);
            for (Document document : documents) {
                if (numberOf(identityOf(document)) == ROOT_ID) {
                    last = document;
                }
            }
            if (last != null && reached.test(last)) {
                return last;
            }
            sleep();
        }
        throw new AssertionError(what + ": the document for root " + ROOT_ID + " is " + last
                + System.lineSeparator() + "  pipeline state: " + control.state(PIPELINE_ID)
                + ", error count: " + control.errorCount(PIPELINE_ID)
                + System.lineSeparator() + "  collections in the target: " + mongo.collections(targetUri)
                + System.lineSeparator() + "  metrics: " + control.metrics(PIPELINE_ID)
                + System.lineSeparator() + "  logs: " + control.logs(PIPELINE_ID));
    }

    /**
     * Every element the array must hold, with the payload the row behind it carries. Checking payloads and
     * not only ids is what separates "the element is still there" from "the element is still what it was":
     * a rewrite that touched its neighbours passes an id check and fails this one.
     */
    private static void assertPayloads(Document document, List<Item> expected) {
        List<Document> elements = elementsOf(document);
        assertThat(idsOf(elements))
                .as("the elements at '%s'", EMBED_PATH)
                .containsExactlyInAnyOrderElementsOf(expected.stream().map(Item::id).toList());
        for (Item item : expected) {
            assertThat(skuOf(document, item.id()))
                    .as("the payload of element %d", item.id())
                    .isEqualTo(item.sku());
        }
    }

    private static String skuOf(Document document, long elementId) {
        for (Document element : elementsOf(document)) {
            if (numberOf(identityOf(element)) == elementId) {
                Object sku = element.get("sku");
                return sku == null ? null : sku.toString();
            }
        }
        return null;
    }

    private static List<Long> idsOf(List<Document> elements) {
        return elements.stream().map(element -> numberOf(identityOf(element))).toList();
    }

    /** The embedded array of a document, or an empty list while it has not been assembled yet. */
    private static List<Document> elementsOf(Document document) {
        if (!(document.get(EMBED_PATH) instanceof List<?> list)) {
            return List.of();
        }
        List<Document> elements = new ArrayList<>(list.size());
        for (Object element : list) {
            if (element instanceof Document nested) {
                elements.add(nested);
            }
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
                insert.setLong(1, ROOT_ID);
                insert.setString(2, "order-" + ROOT_ID);
                insert.executeUpdate();
            }
            for (Item item : SEEDED) {
                insertItem(connection, item);
            }
        }
    }

    private static void insertItem(MySQLContainer<?> mysql, Item item) throws Exception {
        try (Connection connection =
                DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
            insertItem(connection, item);
        }
    }

    private static void insertItem(Connection connection, Item item) throws Exception {
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO " + CHILD_TABLE + " (id, order_id, sku) VALUES (?, ?, ?)")) {
            insert.setLong(1, item.id());
            insert.setLong(2, ROOT_ID);
            insert.setString(3, item.sku());
            insert.executeUpdate();
        }
    }

    private static void updateItemSku(MySQLContainer<?> mysql, long id, String sku) throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
                PreparedStatement update = connection.prepareStatement(
                        "UPDATE " + CHILD_TABLE + " SET sku = ? WHERE id = ?")) {
            update.setString(1, sku);
            update.setLong(2, id);
            update.executeUpdate();
        }
    }

    private static void deleteItem(MySQLContainer<?> mysql, long id) throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
                PreparedStatement delete =
                        connection.prepareStatement("DELETE FROM " + CHILD_TABLE + " WHERE id = ?")) {
            delete.setLong(1, id);
            delete.executeUpdate();
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
            throw new AssertionError("interrupted while waiting for the assembled document", e);
        }
    }

    /** One child row, as it is seeded or changed and as it must come back inside its parent's array. */
    private record Item(long id, String sku) {
    }
}
