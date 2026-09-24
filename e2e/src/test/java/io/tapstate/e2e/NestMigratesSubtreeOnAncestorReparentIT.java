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
 * A row in the middle of a tree that changes which parent it hangs under takes its whole subtree with it.
 *
 * <p>The sibling witness moves a leaf. This one moves a row that is itself a parent, which is the case the
 * leaf cannot expose: everything below the moved row was assembled under the old root's key and has to be
 * carried across, and nothing about the moved row itself says how much is hanging off it.
 *
 * <p>What the assertions have to discriminate: an implementation that moves the row and leaves its
 * descendants behind passes every assertion about the old root - it really is empty - and passes a count of
 * the new root's array too, because the moved row is there. What it does not do is bring the leaves, and
 * they vanish without a trace: no error, no dead letter, and a document that looks structurally right.
 * So the elements below the moved row are read by identity at the new root, and that is the load-bearing
 * assertion here.
 *
 * <p>Gated on Docker and on a directory of real connector jars, like its siblings. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=NestMigratesSubtreeOnAncestorReparentIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class NestMigratesSubtreeOnAncestorReparentIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(120);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final String ROOT_TABLE = "customers";
    private static final String MIDDLE_TABLE = "orders";
    private static final String LEAF_TABLE = "order_items";
    private static final String PIPELINE_ID = "migrated_subtree";

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
    private static final String MIDDLE_PATH = "orders";
    private static final String LEAF_PATH = "items";

    private static final long OLD_ROOT = 1;
    private static final long NEW_ROOT = 2;
    private static final long MIDDLE_ID = 10;
    private static final List<Long> LEAF_IDS = List.of(100L, 101L);

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aMiddleRowThatChangesItsParentCarriesItsDescendantsAcross(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            seedMysql(mysql);
            grantReplication(mysql);

            String suffix = tier.name().toLowerCase(Locale.ROOT);
            pipelineId = PIPELINE_ID + "_" + suffix;
            String storeUri = SharedMongo.replicaSetUrl("subtree_store_" + suffix);
            String targetUri = SharedMongo.replicaSetUrl("subtree_target_" + suffix);

            try (ServerHandle server = tier.launch(storeUri);
                    MongoEndpoints mongo = new MongoEndpoints()) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");

                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

                Map<String, Object> mysqlConfig = mysqlConfig(mysql);
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put("src_customers.tap.yml", sourceYaml("src_customers", ROOT_TABLE, mysqlConfig));
                resources.put("src_orders.tap.yml", sourceYaml("src_orders", MIDDLE_TABLE, mysqlConfig));
                resources.put("src_items.tap.yml", sourceYaml("src_items", LEAF_TABLE, mysqlConfig));
                resources.put("tgt_mongo.tap.yml", targetYaml(targetUri));
                resources.put("pipeline.tap.yml", pipelineYaml(pipelineId));
                control.apply(resources);

                control.discoverSchema("src_customers", "mysql", mysqlConfig);
                control.discoverSchema("src_orders", "mysql", mysqlConfig);
                control.discoverSchema("src_items", "mysql", mysqlConfig);

                control.lifecycle(pipelineId, LifecycleVerb.START);

                List<Document> before = await(mongo, targetUri,
                        NestMigratesSubtreeOnAncestorReparentIT::assembledUnderOldRoot);
                if (!assembledUnderOldRoot(before)) {
                    throw new AssertionError(diagnose(control, mongo, targetUri,
                            "the subtree never assembled under its first root", before));
                }

                reparentMiddleRow(mysql);

                List<Document> after = await(mongo, targetUri,
                        NestMigratesSubtreeOnAncestorReparentIT::migrated);
                if (!migrated(after)) {
                    throw new AssertionError(diagnose(control, mongo, targetUri,
                            "the subtree did not arrive whole under the other root", after));
                }
                assertMigrated(after);
            }
        }
    }

    /** Both roots present, the middle row and both its leaves assembled under the first of them. */
    private static boolean assembledUnderOldRoot(List<Document> documents) {
        return documents.size() == 2
                && middleIds(documents, OLD_ROOT).equals(List.of(MIDDLE_ID))
                && leafIds(documents, OLD_ROOT, MIDDLE_ID).equals(LEAF_IDS)
                && middleIds(documents, NEW_ROOT).isEmpty();
    }

    /** The middle row and both its leaves now under the other root, and nothing left under the first. */
    private static boolean migrated(List<Document> documents) {
        return documents.size() == 2
                && middleIds(documents, OLD_ROOT).isEmpty()
                && middleIds(documents, NEW_ROOT).equals(List.of(MIDDLE_ID))
                && leafIds(documents, NEW_ROOT, MIDDLE_ID).equals(LEAF_IDS);
    }

    /**
     * The discriminating half. The first assertion is satisfied by an implementation that drops the subtree
     * on the floor; the second by one that never detaches. Only the third says the descendants travelled -
     * and it is the one an implementation that moves the row alone fails, silently and with no other signal.
     */
    private static void assertMigrated(List<Document> documents) {
        assertThat(middleIds(documents, OLD_ROOT))
                .as("elements left under document %d, the root the middle row no longer names", OLD_ROOT)
                .isEmpty();
        assertThat(middleIds(documents, NEW_ROOT))
                .as("elements under document %d, the root the middle row now names", NEW_ROOT)
                .containsExactly(MIDDLE_ID);
        assertThat(leafIds(documents, NEW_ROOT, MIDDLE_ID))
                .as("the descendants of element %d after it moved to document %d", MIDDLE_ID, NEW_ROOT)
                .isEqualTo(LEAF_IDS);
    }

    /** The ids of the middle-level elements under one root, in array order. */
    private static List<Long> middleIds(List<Document> documents, long rootId) {
        Document root = documentOrNull(documents, rootId);
        return root == null ? List.of() : idsOf(elementsAt(root, MIDDLE_PATH));
    }

    /** The ids of the leaves hanging under one middle element of one root, in array order. */
    private static List<Long> leafIds(List<Document> documents, long rootId, long middleId) {
        Document root = documentOrNull(documents, rootId);
        if (root == null) {
            return List.of();
        }
        for (Document middle : elementsAt(root, MIDDLE_PATH)) {
            if (numberOf(identityOf(middle)) == middleId) {
                return idsOf(elementsAt(middle, LEAF_PATH));
            }
        }
        return List.of();
    }

    private static List<Long> idsOf(List<Document> elements) {
        List<Long> ids = new ArrayList<>(elements.size());
        for (Document element : elements) {
            ids.add(numberOf(identityOf(element)));
        }
        return ids;
    }

    private List<Document> await(
            MongoEndpoints mongo, String targetUri, Predicate<List<Document>> settled) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        List<Document> last = List.of();
        while (System.nanoTime() - deadline < 0) {
            last = mongo.documents(EndpointAddress.uri(targetUri), ROOT_TABLE);
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
        return what + ": '" + ROOT_TABLE + "' holds " + documents
                + System.lineSeparator() + "  pipeline state: " + control.state(pipelineId)
                + ", error count: " + control.errorCount(pipelineId)
                + System.lineSeparator() + "  collections in the target: " + mongo.collections(targetUri)
                + System.lineSeparator() + "  metrics: " + control.metrics(pipelineId)
                + System.lineSeparator() + "  logs: " + control.logs(pipelineId);
    }

    private static Document documentOrNull(List<Document> documents, long rootId) {
        for (Document document : documents) {
            if (numberOf(identityOf(document)) == rootId) {
                return document;
            }
        }
        return null;
    }

    /** The embedded array at one path of a document; an absent path reads as empty rather than crashing. */
    private static List<Document> elementsAt(Document parent, String path) {
        Object embedded = parent.get(path);
        if (embedded == null) {
            return List.of();
        }
        if (!(embedded instanceof List<?> list)) {
            throw new AssertionError("'" + path + "' is "
                    + embedded.getClass().getSimpleName() + ", not an array - " + parent);
        }
        List<Document> elements = new ArrayList<>(list.size());
        for (Object element : list) {
            if (!(element instanceof Document document)) {
                throw new AssertionError("an element at '" + path + "' is not a document: " + element);
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

    /** Two roots, one middle row under the first of them, and two leaves under that middle row. */
    private static void seedMysql(MySQLContainer<?> mysql) throws Exception {
        try (Connection connection =
                DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE " + ROOT_TABLE + " (id INT PRIMARY KEY, name VARCHAR(64))");
                statement.execute("CREATE TABLE " + MIDDLE_TABLE
                        + " (id INT PRIMARY KEY, customer_id INT, code VARCHAR(64))");
                statement.execute("CREATE TABLE " + LEAF_TABLE
                        + " (id INT PRIMARY KEY, order_id INT, sku VARCHAR(64))");
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + ROOT_TABLE + " (id, name) VALUES (?, ?)")) {
                for (long id : List.of(OLD_ROOT, NEW_ROOT)) {
                    insert.setLong(1, id);
                    insert.setString(2, "customer-" + id);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + MIDDLE_TABLE + " (id, customer_id, code) VALUES (?, ?, ?)")) {
                insert.setLong(1, MIDDLE_ID);
                insert.setLong(2, OLD_ROOT);
                insert.setString(3, "order-" + MIDDLE_ID);
                insert.executeUpdate();
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + LEAF_TABLE + " (id, order_id, sku) VALUES (?, ?, ?)")) {
                for (long id : LEAF_IDS) {
                    insert.setLong(1, id);
                    insert.setLong(2, MIDDLE_ID);
                    insert.setString(3, "sku-" + id);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }
    }

    /** The move: the middle row keeps its identity and its leaves, and names the other root. */
    private static void reparentMiddleRow(MySQLContainer<?> mysql) throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
                PreparedStatement update = connection.prepareStatement(
                        "UPDATE " + MIDDLE_TABLE + " SET customer_id = ? WHERE id = ?")) {
            update.setLong(1, NEW_ROOT);
            update.setLong(2, MIDDLE_ID);
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

    /** The middle embed tracks its keys; the leaf below it is what has to travel when the middle moves. */
    private static String pipelineYaml(String pipelineId) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: [ src_customers, src_orders, src_items ]
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: customer_doc
                    type: nest
                    from: { c: customers, o: orders, i: order_items }
                    root:
                      from: c
                      key: [ id ]
                      embed:
                        - from: o
                          on: { customer_id: id }
                          as: array
                          path: orders
                          arrayKey: [ id ]
                          trackKeyChanges: true
                          embed:
                            - { from: i, on: { order_id: id }, as: array, path: items, arrayKey: [ id ] }
                serve:
                  from: customer_doc
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
            throw new AssertionError("interrupted while waiting for the subtree to migrate", e);
        }
    }
}
