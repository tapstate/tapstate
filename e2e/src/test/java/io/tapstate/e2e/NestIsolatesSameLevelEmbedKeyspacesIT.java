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
 * The witness that two embeds hanging off the same parent keep their keyspaces apart, over real sources.
 *
 * <p>A root with two non-leaf embeds side by side is a shape none of the other nest tests has, and it is
 * the one where a bare key stops being unambiguous: each branch stores which root a parent row belongs
 * to, and if those two stores share a keyspace then a mapping written by one is a mapping the other
 * reads. Tables auto-increment from 1, so the collision is not a corner case - it is what ordinary data
 * looks like.
 *
 * <p>The fixture makes such a read wrong rather than harmless: policy 77 belongs to one customer and
 * order 77 to the other, crossed again at 88. A branch answered by the other branch's mapping delivers
 * its children to the other root while both roots still hold the right number of elements, so counting
 * sees nothing. Only walking each path and reading which parent an element landed under does.
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
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors -Dit.test=NestIsolatesSameLevelEmbedKeyspacesIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 *
 * <p>The last flag is not optional and the run is not skippable with {@code -DskipTests}. Building the
 * reactor up to this module runs every upstream module's integration phase too, and a name that matches
 * nothing there ends the build before this test is reached; {@code -DskipTests} skips the integration
 * tests along with the unit tests, so the run reports success having executed nothing.
 */
class NestIsolatesSameLevelEmbedKeyspacesIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(120);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final String ROOT_TABLE = "customers";
    private static final String POLICY_TABLE = "policies";
    private static final String CLAIM_TABLE = "claims";
    private static final String ORDER_TABLE = "orders";
    private static final String ITEM_TABLE = "items";
    private static final String PIPELINE_ID = "customer_two_branches";

    private static final List<Row> CUSTOMERS = List.of(new Row(1, 0, "first"), new Row(2, 0, "second"));

    /** The collision, and it is crossed: 77 is a policy of customer 1 and an order of customer 2. */
    private static final List<Row> POLICIES = List.of(new Row(77, 1, "p77"), new Row(88, 2, "p88"));
    private static final List<Row> ORDERS = List.of(new Row(77, 2, "o77"), new Row(88, 1, "o88"));

    private static final List<Row> CLAIMS = List.of(new Row(700, 77, "c700"), new Row(800, 88, "c800"));
    private static final List<Row> ITEMS = List.of(new Row(7000, 77, "i7000"), new Row(8000, 88, "i8000"));

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aMappingOneBranchStoredUnderAKeyDoesNotAnswerTheOtherBranchsLookup(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            seedMysql(mysql);
            grantReplication(mysql);

            // One store and one target per tier: sharing them would let a later tier read the documents
            // an earlier one already landed and pass without the nest assembling a thing.
            String suffix = tier.name().toLowerCase(Locale.ROOT);
            String storeUri = SharedMongo.replicaSetUrl("branches_store_" + suffix);
            String targetUri = SharedMongo.replicaSetUrl("branches_target_" + suffix);

            try (ServerHandle server = tier.launch(storeUri);
                    MongoEndpoints mongo = new MongoEndpoints()) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");

                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

                Map<String, Object> mysqlConfig = mysqlConfig(mysql);
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put("src_customers.tap.yml", sourceYaml("src_customers", ROOT_TABLE, mysqlConfig));
                resources.put("src_policies.tap.yml", sourceYaml("src_policies", POLICY_TABLE, mysqlConfig));
                resources.put("src_claims.tap.yml", sourceYaml("src_claims", CLAIM_TABLE, mysqlConfig));
                resources.put("src_orders.tap.yml", sourceYaml("src_orders", ORDER_TABLE, mysqlConfig));
                resources.put("src_items.tap.yml", sourceYaml("src_items", ITEM_TABLE, mysqlConfig));
                resources.put("tgt_mongo.tap.yml", targetYaml(targetUri));
                resources.put("pipeline.tap.yml", pipelineYaml());
                control.apply(resources);

                // Every source is discovered: the root's model resolves the target the sink writes, and
                // each other level's is where an embedded element's own key comes from.
                control.discoverSchema("src_customers", "mysql", mysqlConfig);
                control.discoverSchema("src_policies", "mysql", mysqlConfig);
                control.discoverSchema("src_claims", "mysql", mysqlConfig);
                control.discoverSchema("src_orders", "mysql", mysqlConfig);
                control.discoverSchema("src_items", "mysql", mysqlConfig);

                control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

                List<Document> documents = awaitDocuments(mongo, targetUri);
                if (!settled(documents)) {
                    throw new AssertionError(diagnose(control, mongo, targetUri, documents));
                }
                assertBranchesKeptApart(documents);
            }
        }
    }

    /**
     * Waits for both branches of both roots to be filled, then hands over what the target holds. Waiting
     * on the root count alone would read a document while one branch is still arriving - five chains land
     * independently - and report a timing gap as a mix-up. On timeout the last observation is handed over
     * anyway, so the failure is the real difference rather than "waited too long".
     */
    private static List<Document> awaitDocuments(MongoEndpoints mongo, String targetUri) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        List<Document> last = List.of();
        while (System.nanoTime() - deadline < 0) {
            last = mongo.documents(targetUri, ROOT_TABLE);
            if (settled(last)) {
                return last;
            }
            sleep();
        }
        return last;
    }

    /**
     * What the target and the pipeline actually look like when the wait ran out. "Zero documents" names a
     * symptom shared by a pipeline that failed, a pipeline still starting, and rows written to a
     * collection nobody looked in - so the reading that separates them is taken here rather than left for
     * a rerun with more logging.
     */
    private static String diagnose(
            ControlPlane control, MongoEndpoints mongo, String targetUri, List<Document> documents) {
        return "the nest never filled both branches of both roots: '" + ROOT_TABLE + "' holds " + documents
                + System.lineSeparator() + "  pipeline state: " + control.state(PIPELINE_ID)
                + ", error count: " + control.errorCount(PIPELINE_ID)
                + System.lineSeparator() + "  collections in the target: " + mongo.collections(targetUri)
                + System.lineSeparator() + "  metrics: " + control.metrics(PIPELINE_ID)
                + System.lineSeparator() + "  logs: " + control.logs(PIPELINE_ID);
    }

    /** Whether both branches of both roots have landed everything, so waiting can stop. */
    private static boolean settled(List<Document> documents) {
        if (documents.size() != CUSTOMERS.size()) {
            return false;
        }
        int policies = 0;
        int claims = 0;
        int orders = 0;
        int items = 0;
        for (Document document : documents) {
            List<Document> policyElements = arrayOrNull(document, "policies");
            List<Document> orderElements = arrayOrNull(document, "orders");
            if (policyElements == null || orderElements == null) {
                return false;
            }
            policies += policyElements.size();
            orders += orderElements.size();
            for (Document policy : policyElements) {
                List<Document> nested = arrayOrNull(policy, "claims");
                if (nested == null) {
                    return false;
                }
                claims += nested.size();
            }
            for (Document order : orderElements) {
                List<Document> nested = arrayOrNull(order, "items");
                if (nested == null) {
                    return false;
                }
                items += nested.size();
            }
        }
        return policies == POLICIES.size() && claims == CLAIMS.size()
                && orders == ORDERS.size() && items == ITEMS.size();
    }

    /**
     * The discriminating half. Each root's two branches must hold the parents whose foreign key names
     * that root, and each of those parents its own children - which is precisely what one branch reading
     * the other's mapping gets wrong while every count stays right.
     */
    private static void assertBranchesKeptApart(List<Document> documents) {
        assertThat(documents)
                .as("documents in the Mongo target %s", ROOT_TABLE)
                .hasSize(CUSTOMERS.size());

        for (Row customer : CUSTOMERS) {
            Document root = elementFor(documents, customer.id(), "customer " + customer.id());
            String whose = "customer " + customer.id();
            assertBranch(root, whose, "policies", POLICIES, "claims", CLAIMS);
            assertBranch(root, whose, "orders", ORDERS, "items", ITEMS);
        }
    }

    /**
     * One branch of one root: the parents it must hold, and beneath each of those the children that name
     * it. Reading the children as well as the parents is what makes a swapped mapping visible - a branch
     * can hold the right parent and still have been handed the other branch's descendants.
     */
    private static void assertBranch(Document root, String whose, String branch, List<Row> parentRows,
            String childPath, List<Row> childRows) {
        long rootId = numberOf(identityOf(root));
        List<Document> parents = arrayAt(root, branch, whose);
        assertThat(idsOf(parents))
                .as("the '%s' of %s, which the other branch's key of the same value must not answer",
                        branch, whose)
                .containsExactlyInAnyOrderElementsOf(childIdsOf(parentRows, rootId));

        for (Document parent : parents) {
            long parentId = numberOf(identityOf(parent));
            String under = "'" + branch + "' " + parentId + " of " + whose;
            assertThat(idsOf(arrayAt(parent, childPath, under)))
                    .as("the '%s' under %s", childPath, under)
                    .containsExactlyInAnyOrderElementsOf(childIdsOf(childRows, parentId));
        }
    }

    /** The ids of the seeded rows that hang under one parent, which is what an array at that path must hold. */
    private static List<Long> childIdsOf(List<Row> rows, long parentId) {
        return rows.stream().filter(row -> row.parentId() == parentId).map(Row::id).toList();
    }

    private static List<Long> idsOf(List<Document> elements) {
        return elements.stream().map(element -> numberOf(identityOf(element))).toList();
    }

    /** The document or element with an id, addressed by the key its table declares. */
    private static Document elementFor(List<Document> elements, long id, String what) {
        for (Document element : elements) {
            if (numberOf(identityOf(element)) == id) {
                return element;
            }
        }
        throw new AssertionError("no " + what + " among " + elements);
    }

    /**
     * The array a parent carries at a path. An absent path is a failure this test exists to catch - the
     * level above assembled and nothing was ever attached beneath it - so it is reported as that rather
     * than as a null dereference.
     */
    private static List<Document> arrayAt(Document parent, String path, String where) {
        List<Document> elements = arrayOrNull(parent, path);
        if (elements == null) {
            Object embedded = parent.get(path);
            throw new AssertionError(embedded == null
                    ? where + " carries no '" + path + "': it reached the sink with nothing assembled "
                            + "beneath it - " + parent
                    : where + " carries '" + path + "' as " + embedded.getClass().getSimpleName()
                            + ", not an array - " + parent);
        }
        return elements;
    }

    /** The array at a path, or null when the path is absent or is not an array of documents. */
    private static List<Document> arrayOrNull(Document parent, String path) {
        if (!(parent.get(path) instanceof List<?> list)) {
            return null;
        }
        List<Document> elements = new ArrayList<>(list.size());
        for (Object element : list) {
            if (!(element instanceof Document document)) {
                return null;
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

    /**
     * Five tables: a root, two branches hanging off it, and a child under each branch. The two branch
     * tables share a range of key values on purpose - that is the whole fixture.
     */
    private static void seedMysql(MySQLContainer<?> mysql) throws Exception {
        try (Connection connection =
                DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword())) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE " + ROOT_TABLE + " (id INT PRIMARY KEY, name VARCHAR(64))");
                statement.execute("CREATE TABLE " + POLICY_TABLE
                        + " (id INT PRIMARY KEY, customer_id INT, plan VARCHAR(64))");
                statement.execute("CREATE TABLE " + CLAIM_TABLE
                        + " (id INT PRIMARY KEY, policy_id INT, note VARCHAR(64))");
                statement.execute("CREATE TABLE " + ORDER_TABLE
                        + " (id INT PRIMARY KEY, customer_id INT, ref VARCHAR(64))");
                statement.execute("CREATE TABLE " + ITEM_TABLE
                        + " (id INT PRIMARY KEY, order_id INT, sku VARCHAR(64))");
            }
            insertRoots(connection);
            insertChildren(connection, POLICY_TABLE, "customer_id", "plan", POLICIES);
            insertChildren(connection, CLAIM_TABLE, "policy_id", "note", CLAIMS);
            insertChildren(connection, ORDER_TABLE, "customer_id", "ref", ORDERS);
            insertChildren(connection, ITEM_TABLE, "order_id", "sku", ITEMS);
        }
    }

    private static void insertRoots(Connection connection) throws Exception {
        try (PreparedStatement insert =
                connection.prepareStatement("INSERT INTO " + ROOT_TABLE + " (id, name) VALUES (?, ?)")) {
            for (Row customer : CUSTOMERS) {
                insert.setLong(1, customer.id());
                insert.setString(2, customer.label());
                insert.addBatch();
            }
            insert.executeBatch();
        }
    }

    private static void insertChildren(
            Connection connection, String table, String parentColumn, String labelColumn, List<Row> rows)
            throws Exception {
        try (PreparedStatement insert = connection.prepareStatement("INSERT INTO " + table + " (id, "
                + parentColumn + ", " + labelColumn + ") VALUES (?, ?, ?)")) {
            for (Row row : rows) {
                insert.setLong(1, row.id());
                insert.setLong(2, row.parentId());
                insert.setString(3, row.label());
                insert.addBatch();
            }
            insert.executeBatch();
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

    /** One capture unit per table: five single-table sources are what puts five chains into one job. */
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
     * The root source leads the list: the target model a sink writes is resolved from the first source
     * whose schema is discovered, and the document being assembled is the root's. Two embeds sit at the
     * same level under it, each with a child of its own, so each compiles to a resolver of its own.
     */
    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: customer_two_branches
                source: [ src_customers, src_policies, src_claims, src_orders, src_items ]
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: customer_doc
                    type: nest
                    from: { c: customers, p: policies, cl: claims, o: orders, i: items }
                    root:
                      from: c
                      key: [ id ]
                      embed:
                        - from: p
                          on: { customer_id: id }
                          as: array
                          path: policies
                          arrayKey: [ id ]
                          embed:
                            - { from: cl, on: { policy_id: id }, as: array, path: claims, arrayKey: [ id ] }
                        - from: o
                          on: { customer_id: id }
                          as: array
                          path: orders
                          arrayKey: [ id ]
                          embed:
                            - { from: i, on: { order_id: id }, as: array, path: items, arrayKey: [ id ] }
                serve:
                  from: customer_doc
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

    /** One seeded row: its own key, the key of the level above it, and a payload to tell it apart. */
    private record Row(long id, long parentId, String label) {
    }
}
