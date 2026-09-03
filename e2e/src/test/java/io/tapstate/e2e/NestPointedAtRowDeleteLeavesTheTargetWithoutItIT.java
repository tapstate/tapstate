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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The witness that deleting a row the documents point at takes it out of them <b>in the target</b>, rather
 * than leaving every document holding its last known value for ever.
 *
 * <p><b>Why this has to be end-to-end, when a case over the operator already asserts the same sentence.</b>
 * A document that has stopped carrying a field and a document that never carried one are the same document.
 * Everything inside the engine sees only that document, so a case there can assert the field is not in it
 * and pass while nothing downstream ever hears that it went. Every write into a keyed target applies a row
 * by setting the fields in it - an upsert into a document store is exactly that - so a field that stops
 * being produced is simply never mentioned again and stays at its last value for as long as the row lives.
 * The write succeeds, the document that arrives is right, and no count anywhere moves. <b>The target is the
 * only place the difference exists</b>, which is why this case reads the target and not the emission.
 *
 * <p>Measured before this was fixed: the field stood in the target for more than fifteen minutes and
 * survived a restart of the pipeline, while the operator had not emitted it since the deletion.
 *
 * <p><b>The reference is at the root on purpose, and that is the discriminating choice.</b> The same
 * deletion one level down - a row pointed at from inside an array - converges without any of this, because
 * the array holding it is itself a field of the document and is written whole, so the element inside it is
 * replaced along with it. Only a top-level field can go missing in a way nothing can reach. A case written
 * at depth passes against the defect.
 *
 * <p>What the assertions have to discriminate:
 * <ul>
 *   <li><b>The control comes first.</b> Both documents are required to carry the row <em>before</em> the
 *       deletion. Without it, "the field is absent" is satisfied by documents that never had it - which is
 *       what a reference that never resolved produces, and it looks identical from here.</li>
 *   <li><b>Two documents, not one.</b> A single document is satisfied by anything that happened to redraw
 *       the document the row was last seen in; two share the row, so both have to follow.</li>
 *   <li><b>The neighbour is untouched.</b> A document pointing at a row that was <em>not</em> deleted keeps
 *       its field, so this cannot pass by dropping the field from everything.</li>
 *   <li><b>Absent, not null.</b> A null is a value, and a target handed one writes it over what is there;
 *       absent is how a document says the row it named is not there to show.</li>
 * </ul>
 *
 * <p>Gated on Docker and on a directory of real connector jars, like its siblings. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=NestPointedAtRowDeleteLeavesTheTargetWithoutItIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 *
 * <p>The last flag is not optional and the run is not skippable with {@code -DskipTests}: building the
 * reactor up to this module runs every upstream module's integration phase too, and a name matching nothing
 * there ends the build before this test is reached, while {@code -DskipTests} would report success having
 * executed nothing.
 */
class NestPointedAtRowDeleteLeavesTheTargetWithoutItIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(120);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final String ORDER_TABLE = "orders";
    private static final String CUSTOMER_TABLE = "customers";
    private static final String PIPELINE_ID = "pointed_at_delete";
    private static final String EMBED_PATH = "customer";

    /** The row two documents point at, and which is then deleted. */
    private static final long SHARED_CUSTOMER = 1;
    private static final String SHARED_NAME = "Ada";

    /** The row the third document points at, which is never deleted and must keep its field. */
    private static final long OTHER_CUSTOMER = 2;
    private static final String OTHER_NAME = "Brian";

    private static final List<Long> ORDERS_ON_SHARED = List.of(10L, 11L);
    private static final long ORDER_ON_OTHER = 12;

    private String pipelineId;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void deletingTheRowTheyPointAtLeavesEveryDocumentWithoutIt(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            seedMysql(mysql);
            grantReplication(mysql);

            // One store and one target per tier: sharing them would let a later tier read what an earlier
            // one already landed and pass without assembling anything.
            String suffix = tier.name().toLowerCase(Locale.ROOT);
            pipelineId = PIPELINE_ID + "_" + suffix;
            String storeUri = SharedMongo.replicaSetUrl("pointed_at_store_" + suffix);
            String targetUri = SharedMongo.replicaSetUrl("pointed_at_target_" + suffix);

            try (ServerHandle server = tier.launch(storeUri);
                    MongoEndpoints mongo = new MongoEndpoints()) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");

                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

                Map<String, Object> mysqlConfig = mysqlConfig(mysql);
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put("src_orders.tap.yml", sourceYaml("src_orders", ORDER_TABLE, mysqlConfig));
                resources.put("src_customers.tap.yml",
                        sourceYaml("src_customers", CUSTOMER_TABLE, mysqlConfig));
                resources.put("tgt_mongo.tap.yml", targetYaml(targetUri));
                resources.put("pipeline.tap.yml", pipelineYaml(pipelineId));
                control.apply(resources);

                control.discoverSchema("src_orders", "mysql", mysqlConfig);
                control.discoverSchema("src_customers", "mysql", mysqlConfig);

                control.lifecycle(pipelineId, LifecycleVerb.START);

                // 1. The control, and it is not optional. Both documents have to be seen carrying the row
                //    before it is deleted, or the assertion below is satisfied by documents that never had
                //    it - which is exactly what a reference that never resolved produces.
                for (long order : ORDERS_ON_SHARED) {
                    Document carried = await(control, mongo, targetUri, order,
                            document -> SHARED_NAME.equals(embeddedName(document)),
                            "the row pointed at never reached the document");
                    assertThat(embeddedName(carried))
                            .as("document %d carries the row it points at, before anything is deleted", order)
                            .isEqualTo(SHARED_NAME);
                }
                await(control, mongo, targetUri, ORDER_ON_OTHER,
                        document -> OTHER_NAME.equals(embeddedName(document)),
                        "the neighbour's own row never reached its document");

                // 2. Delete the row two of them point at. Nothing touches the order rows themselves: a
                //    convergence that needs the document's own row to change again is not convergence.
                deleteCustomer(mysql, SHARED_CUSTOMER);

                // 3. Both documents that pointed at it lose the field - in the target, which is the only
                //    place "the field stopped being produced" and "the field is gone" differ at all.
                for (long order : ORDERS_ON_SHARED) {
                    Document converged = await(control, mongo, targetUri, order,
                            document -> !document.containsKey(EMBED_PATH),
                            "the deleted row never left the document in the target");
                    assertThat(converged.containsKey(EMBED_PATH))
                            .as("document %d no longer shows the deleted row. Keeping its last value is "
                                    + "what a target updated by setting the fields it was given does, and "
                                    + "those documents are complete, consistent, and wrong only against a "
                                    + "source nobody here asks again", order)
                            .isFalse();
                    assertThat(converged.get(EMBED_PATH))
                            .as("absent rather than null: a null is a value a target writes over what is "
                                    + "there, and it would read as a row that exists and is empty")
                            .isNull();
                }

                // 4. And the document pointing at the row that was not deleted still has its own.
                Document untouched = documentFor(mongo, targetUri, ORDER_ON_OTHER);
                assertThat(embeddedName(untouched))
                        .as("the neighbour keeps the row it points at - without this, dropping the field "
                                + "from every document would pass")
                        .isEqualTo(OTHER_NAME);
            }
        }
    }

    /**
     * Waits for one document to reach a shape, then hands it over. Each step waits for its own change
     * rather than for a fixed pause: a change stream has no deadline, and a sleep long enough to be safe is
     * a sleep long enough to hide a rewrite that happened twice. On timeout the pipeline is read, so the
     * failure says what it was doing rather than only that it took too long.
     */
    private Document await(ControlPlane control, MongoEndpoints mongo, String targetUri, long order,
            Predicate<Document> reached, String what) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        Document last = null;
        while (System.nanoTime() - deadline < 0) {
            last = documentFor(mongo, targetUri, order);
            if (last != null && reached.test(last)) {
                return last;
            }
            sleep();
        }
        throw new AssertionError(what + ": the document for order " + order + " is " + last
                + System.lineSeparator() + "  pipeline state: " + control.state(pipelineId)
                + ", error count: " + control.errorCount(pipelineId)
                + System.lineSeparator() + "  collections in the target: " + mongo.collections(targetUri)
                + System.lineSeparator() + "  metrics: " + control.metrics(pipelineId)
                + System.lineSeparator() + "  logs: " + control.logs(pipelineId));
    }

    private static Document documentFor(MongoEndpoints mongo, String targetUri, long order) {
        Document found = null;
        for (Document document : mongo.documents(EndpointAddress.uri(targetUri), ORDER_TABLE)) {
            if (numberOf(identityOf(document)) == order) {
                found = document;
            }
        }
        return found;
    }

    /** The name on the embedded row, or null where the document carries no such field at all. */
    private static String embeddedName(Document document) {
        if (document == null || !(document.get(EMBED_PATH) instanceof Document embedded)) {
            return null;
        }
        Object name = embedded.get("name");
        return name == null ? null : name.toString();
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
                statement.execute("CREATE TABLE " + CUSTOMER_TABLE
                        + " (id INT PRIMARY KEY, name VARCHAR(64))");
                statement.execute("CREATE TABLE " + ORDER_TABLE
                        + " (id INT PRIMARY KEY, placed_on VARCHAR(64), customer_id INT)");
            }
            insertCustomer(connection, SHARED_CUSTOMER, SHARED_NAME);
            insertCustomer(connection, OTHER_CUSTOMER, OTHER_NAME);
            for (long order : ORDERS_ON_SHARED) {
                insertOrder(connection, order, SHARED_CUSTOMER);
            }
            insertOrder(connection, ORDER_ON_OTHER, OTHER_CUSTOMER);
        }
    }

    private static void insertCustomer(Connection connection, long id, String name) throws Exception {
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO " + CUSTOMER_TABLE + " (id, name) VALUES (?, ?)")) {
            insert.setLong(1, id);
            insert.setString(2, name);
            insert.executeUpdate();
        }
    }

    private static void insertOrder(Connection connection, long id, long customer) throws Exception {
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO " + ORDER_TABLE + " (id, placed_on, customer_id) VALUES (?, ?, ?)")) {
            insert.setLong(1, id);
            insert.setString(2, "2026-01-0" + id % 9);
            insert.setLong(3, customer);
            insert.executeUpdate();
        }
    }

    private static void deleteCustomer(MySQLContainer<?> mysql, long id) throws Exception {
        try (Connection connection =
                        DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
                PreparedStatement delete =
                        connection.prepareStatement("DELETE FROM " + CUSTOMER_TABLE + " WHERE id = ?")) {
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
     * The order source leads the list: the target model a sink writes is resolved from the first source
     * whose schema is discovered, and the document being assembled is the order's.
     *
     * <p>The embed points the other way round from the usual one, and says so by which side of {@code on:}
     * names a table's own identity: {@code id} is what identifies a customer, so the order is the one
     * pointing and the customer is the row being pointed at. Nothing declares a direction.
     */
    private static String pipelineYaml(String pipelineId) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: [ src_orders, src_customers ]
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: order_doc
                    type: nest
                    from: { o: orders, c: customers }
                    root:
                      from: o
                      key: [ id ]
                      embed:
                        - { from: c, on: { id: customer_id }, as: object, path: customer }
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
            throw new AssertionError("interrupted while waiting for the assembled document", e);
        }
    }
}
