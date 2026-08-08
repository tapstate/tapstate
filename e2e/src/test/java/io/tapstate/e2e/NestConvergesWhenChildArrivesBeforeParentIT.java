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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The witness that children read before their parent exists are kept, and converge into a document the
 * moment the parent shows up.
 *
 * <p>Two tables are read by two chains and nothing sequences them, so this is the ordinary case rather
 * than the awkward one. Every other nest test seeds both tables before starting and lets arrival order
 * fall where it may; a run in which the parent happened to land first passes and says nothing about the
 * run in which it did not. Here the parent is not merely late - at start-up it <em>does not exist</em>,
 * and it arrives afterwards over the change stream while its children came through the snapshot.
 *
 * <p>What the assertions have to discriminate, in both directions:
 * <ul>
 *   <li><b>Nothing is published early.</b> A document assembled from children alone would be a root that
 *       was never read - so before the parent is inserted the target must hold no document for it. This
 *       half is a bounded observation rather than a proof, which is why the other half carries the
 *       weight.</li>
 *   <li><b>Nothing was thrown away.</b> An implementation that discards a child whose parent is not yet
 *       resolved passes every other test in the suite and fails here: the document appears when the
 *       parent is inserted, and its array is empty. So the array is read, element for element.</li>
 * </ul>
 *
 * <p>Read mode is {@code snapshot_and_cdc} rather than {@code snapshot_only} deliberately: a stateful
 * node needs every row to carry its order, and a source reading no chain of its own supplies none. It is
 * also what lets the parent arrive at all - it is inserted after the snapshot has been taken.
 *
 * <p>Gated on Docker and on a directory of real connector jars, like its flat siblings
 * {@link RealMysqlToMongoSnapshotIT} and {@link RealMysqlToMongoCdcIT}. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=NestConvergesWhenChildArrivesBeforeParentIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 *
 * <p>The last flag is not optional and the run is not skippable with {@code -DskipTests}. Building the
 * reactor up to this module runs every upstream module's integration phase too, and a name that matches
 * nothing there ends the build before this test is reached; {@code -DskipTests} skips the integration
 * tests along with the unit tests, so the run reports success having executed nothing.
 */
class NestConvergesWhenChildArrivesBeforeParentIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(120);
    private static final Duration POLL = Duration.ofMillis(250);

    /**
     * How long the target is watched for a document that must not appear. Long enough that the pipeline
     * has started, taken its snapshot of both tables and had the orphaned children in hand throughout.
     */
    private static final Duration NOTHING_YET = Duration.ofSeconds(20);

    private static final String PARENT_TABLE = "orders";
    private static final String CHILD_TABLE = "order_items";
    private static final String PIPELINE_ID = "nested_orders";
    private static final String EMBED_PATH = "items";
    private static final long ROOT_ID = 7;

    /** Seeded before the pipeline starts, while the parent they name does not exist. */
    private static final List<Item> ORPHANS =
            List.of(new Item(71, "sku-71"), new Item(72, "sku-72"), new Item(73, "sku-73"));

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void childrenReadBeforeTheirParentExistsConvergeWhenItArrives(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            seedOrphansOnly(mysql);
            grantReplication(mysql);

            // One store and one target per tier: sharing them would let a later tier read the documents
            // an earlier one already landed and pass without the nest assembling a thing.
            String suffix = tier.name().toLowerCase(Locale.ROOT);
            String storeUri = SharedMongo.replicaSetUrl("orphan_store_" + suffix);
            String targetUri = SharedMongo.replicaSetUrl("orphan_target_" + suffix);

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

                // 1. Children with no parent must not become a document of their own.
                assertNothingIsPublishedYet(control, mongo, targetUri);

                // 2. The parent arrives over the change stream, and what was held converges onto it.
                insertParent(mysql);
                Document converged = awaitDocument(control, mongo, targetUri);
                assertConverged(converged);
            }
        }
    }

    /**
     * Watches the target for as long as a document could plausibly have been published, and requires that
     * none was. Also reads the pipeline while doing it: "no documents" is equally true of a pipeline that
     * died on start-up, and a dead pipeline would make the second half of this test meaningless rather
     * than passing.
     */
    private static void assertNothingIsPublishedYet(
            ControlPlane control, MongoEndpoints mongo, String targetUri) {
        long deadline = System.nanoTime() + NOTHING_YET.toNanos();
        while (System.nanoTime() - deadline < 0) {
            List<Document> documents = mongo.documents(targetUri, PARENT_TABLE);
            assertThat(documents)
                    .as("a document was published for a parent that has not been read yet - "
                            + "children alone are not a root")
                    .isEmpty();
            sleep();
        }
        assertThat(control.state(PIPELINE_ID))
                .as("the pipeline has to still be running for 'nothing published' to mean anything")
                .contains(PipelineState.RUNNING);
        assertThat(control.errorCount(PIPELINE_ID))
                .as("an orphaned child is left-outer data, not an error")
                .contains(0L);
    }

    /** Waits for the document the parent's arrival should produce, whole. */
    private static Document awaitDocument(ControlPlane control, MongoEndpoints mongo, String targetUri) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        List<Document> last = List.of();
        while (System.nanoTime() - deadline < 0) {
            last = mongo.documents(targetUri, PARENT_TABLE);
            if (last.size() == 1 && elementsOf(last.get(0)).size() == ORPHANS.size()) {
                return last.get(0);
            }
            sleep();
        }
        throw new AssertionError("the held children never converged onto their parent: '" + PARENT_TABLE
                + "' holds " + last
                + System.lineSeparator() + "  pipeline state: " + control.state(PIPELINE_ID)
                + ", error count: " + control.errorCount(PIPELINE_ID)
                + System.lineSeparator() + "  collections in the target: " + mongo.collections(targetUri)
                + System.lineSeparator() + "  metrics: " + control.metrics(PIPELINE_ID)
                + System.lineSeparator() + "  logs: " + control.logs(PIPELINE_ID));
    }

    /**
     * The discriminating half. The document must carry the parent's own fields and every child that was
     * read before it existed, with the payload each of those rows had - which is exactly what an
     * implementation that discarded them cannot produce, while still producing the document.
     */
    private static void assertConverged(Document document) {
        assertThat(numberOf(identityOf(document)))
                .as("the document is the parent that arrived last")
                .isEqualTo(ROOT_ID);
        assertThat(document.get("name"))
                .as("the root fields, which a document assembled from children alone would not carry")
                .isEqualTo("order-" + ROOT_ID);

        List<Document> elements = elementsOf(document);
        assertThat(elements.stream().map(element -> numberOf(identityOf(element))).toList())
                .as("every child read before the parent existed is in the document")
                .containsExactlyInAnyOrderElementsOf(ORPHANS.stream().map(Item::id).toList());

        for (Item orphan : ORPHANS) {
            assertThat(skuOf(elements, orphan.id()))
                    .as("the payload element %d was holding while it waited", orphan.id())
                    .isEqualTo(orphan.sku());
        }
    }

    private static String skuOf(List<Document> elements, long elementId) {
        for (Document element : elements) {
            if (numberOf(identityOf(element)) == elementId) {
                Object sku = element.get("sku");
                return sku == null ? null : sku.toString();
            }
        }
        return null;
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

    /** Both tables exist; only the child one has rows, and they name a parent that is not there. */
    private static void seedOrphansOnly(MySQLContainer<?> mysql) throws Exception {
        try (Connection connection =
                DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(
                        "CREATE TABLE " + PARENT_TABLE + " (id INT PRIMARY KEY, name VARCHAR(64))");
                statement.execute("CREATE TABLE " + CHILD_TABLE
                        + " (id INT PRIMARY KEY, order_id INT, sku VARCHAR(64))");
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + CHILD_TABLE + " (id, order_id, sku) VALUES (?, ?, ?)")) {
                for (Item orphan : ORPHANS) {
                    insert.setLong(1, orphan.id());
                    insert.setLong(2, ROOT_ID);
                    insert.setString(3, orphan.sku());
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }
    }

    private static void insertParent(MySQLContainer<?> mysql) throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
                PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO " + PARENT_TABLE + " (id, name) VALUES (?, ?)")) {
            insert.setLong(1, ROOT_ID);
            insert.setString(2, "order-" + ROOT_ID);
            insert.executeUpdate();
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
     * whose schema is discovered, and the document being assembled is the parent's - which holds even
     * when that table has no rows in it yet.
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
            throw new AssertionError("interrupted while watching the target", e);
        }
    }

    /** One child row, seeded while the parent it names does not exist. */
    private record Item(long id, String sku) {
    }
}
