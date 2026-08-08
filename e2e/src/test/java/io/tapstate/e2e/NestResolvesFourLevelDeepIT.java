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
 * The witness that a nest four levels deep assembles from four real sources - the shape that separates a
 * cascade of resolvers from a single one.
 *
 * <p>Its sibling {@link NestAssemblesParentAndChildrenIT} proves a document comes out, and it is two
 * levels deep: every child row it carries names the root directly. An implementation that resolves the
 * root once, at the moment a child arrives, satisfies it. Here the deepest table names only its
 * immediate parent - a document row carries a claim id and no customer id anywhere - so the root exists
 * in no field of the row that has to reach it. A claim cannot look the root up from inside its own
 * partition, and a job graph has no cycle to send it back around; the key has to be carried down the
 * tree a level at a time as each level resolves. Without this test the cascade is a decision no run
 * checks.
 *
 * <p>What the assertions have to discriminate: not whether the deepest rows arrive but where. An
 * implementation that hangs every descendant off whichever ancestor was at hand emits documents holding
 * exactly the rows seeded here, at the wrong paths, and satisfies any count of them. So the document is
 * walked by path and what is found at each level must be the elements whose foreign key names that
 * parent, and nothing else. Two customers, and a claim that owns no documents at all, are what make
 * "wrong parent" and "everything piled onto the first root" visible.
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
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors -Dit.test=NestResolvesFourLevelDeepIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 *
 * <p>The last flag is not optional and the run is not skippable with {@code -DskipTests}. Building the
 * reactor up to this module runs every upstream module's integration phase too, and a name that matches
 * nothing there ends the build before this test is reached; {@code -DskipTests} skips the integration
 * tests along with the unit tests, so the run reports success having executed nothing.
 */
class NestResolvesFourLevelDeepIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(120);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final String ROOT_TABLE = "customers";
    private static final String POLICY_TABLE = "policies";
    private static final String CLAIM_TABLE = "claims";
    private static final String DOCUMENT_TABLE = "documents";
    private static final String PIPELINE_ID = "customer_dossier";

    /** Two roots: one root is satisfied by an implementation that piles everything onto whoever came first. */
    private static final List<Row> CUSTOMERS = List.of(new Row(1, 0, "first"), new Row(2, 0, "second"));

    private static final List<Row> POLICIES =
            List.of(new Row(10, 1, "p10"), new Row(11, 1, "p11"), new Row(20, 2, "p20"));

    /** Policy 11 gets a claim of its own so "policy 10 took it" is a failure and not a shape nobody seeded. */
    private static final List<Row> CLAIMS =
            List.of(new Row(100, 10, "c100"), new Row(101, 11, "c101"), new Row(200, 20, "c200"));

    /** Claim 101 is deliberately left without documents: an empty array is an assertion too. */
    private static final List<Row> DOCUMENTS =
            List.of(new Row(1000, 100, "d1000"), new Row(1001, 100, "d1001"), new Row(2000, 200, "d2000"));

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aRowFourLevelsDownReachesTheRootItsOwnFieldsNeverName(Tiers tier) throws Exception {
        try (MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))) {
            mysql.start();
            seedMysql(mysql);
            grantReplication(mysql);

            // One store and one target per tier: sharing them would let a later tier read the documents
            // an earlier one already landed and pass without the nest assembling a thing.
            String suffix = tier.name().toLowerCase(Locale.ROOT);
            String storeUri = SharedMongo.replicaSetUrl("deep_store_" + suffix);
            String targetUri = SharedMongo.replicaSetUrl("deep_target_" + suffix);

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
                resources.put("src_documents.tap.yml", sourceYaml("src_documents", DOCUMENT_TABLE, mysqlConfig));
                resources.put("tgt_mongo.tap.yml", targetYaml(targetUri));
                resources.put("pipeline.tap.yml", pipelineYaml());
                control.apply(resources);

                // Every source is discovered: the root's model resolves the target the sink writes, and
                // each other level's is where an embedded element's own key comes from.
                control.discoverSchema("src_customers", "mysql", mysqlConfig);
                control.discoverSchema("src_policies", "mysql", mysqlConfig);
                control.discoverSchema("src_claims", "mysql", mysqlConfig);
                control.discoverSchema("src_documents", "mysql", mysqlConfig);

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
     * holds. Waiting on the root count alone would read a document whose deepest arrays are still
     * filling - four chains land independently - and report a timing gap as a wrong document. On timeout
     * the last observation is handed over anyway, so the failure is the real difference rather than
     * "waited too long".
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
        return "the nest never assembled the tree that was seeded: expected " + CUSTOMERS.size()
                + " documents holding " + POLICIES.size() + " policies, " + CLAIMS.size() + " claims and "
                + DOCUMENTS.size() + " documents between them, and '" + ROOT_TABLE + "' holds " + documents
                + System.lineSeparator() + "  pipeline state: " + control.state(PIPELINE_ID)
                + ", error count: " + control.errorCount(PIPELINE_ID)
                + System.lineSeparator() + "  collections in the target: " + mongo.collections(targetUri)
                + System.lineSeparator() + "  metrics: " + control.metrics(PIPELINE_ID)
                + System.lineSeparator() + "  logs: " + control.logs(PIPELINE_ID);
    }

    /**
     * Whether every level has landed everything it owns, so waiting can stop. Counting each level rather
     * than only the roots is what keeps a half-filled tree from being read as a wrong one: the deepest
     * rows travel three hops and arrive well after the roots they belong to.
     */
    private static boolean settled(List<Document> documents) {
        if (documents.size() != CUSTOMERS.size()) {
            return false;
        }
        int policies = 0;
        int claims = 0;
        int leaves = 0;
        for (Document document : documents) {
            List<Document> policyElements = arrayOrNull(document, "policies");
            if (policyElements == null) {
                return false;
            }
            policies += policyElements.size();
            for (Document policy : policyElements) {
                List<Document> claimElements = arrayOrNull(policy, "claims");
                if (claimElements == null) {
                    return false;
                }
                claims += claimElements.size();
                for (Document claim : claimElements) {
                    List<Document> leafElements = arrayOrNull(claim, "documents");
                    if (leafElements == null) {
                        return false;
                    }
                    leaves += leafElements.size();
                }
            }
        }
        return policies == POLICIES.size() && claims == CLAIMS.size() && leaves == DOCUMENTS.size();
    }

    /**
     * The discriminating half. Each level's array must hold exactly the rows whose foreign key names the
     * element it hangs under - the whole of what resolving a deep tree means, and none of it visible to a
     * count of documents or of elements.
     */
    private static void assertAssembled(List<Document> documents) {
        assertThat(documents)
                .as("documents in the Mongo target %s", ROOT_TABLE)
                .hasSize(CUSTOMERS.size());

        for (Row customer : CUSTOMERS) {
            Document root = elementFor(documents, customer.id(), "customer " + customer.id());
            assertThat(root.get("name"))
                    .as("the root fields of customer %d, which an array-only document would not carry", customer.id())
                    .isEqualTo(customer.label());

            List<Document> policies = arrayAt(root, "policies", "customer " + customer.id());
            assertThat(idsOf(policies))
                    .as("the policies of customer %d, and none belonging to the other customer", customer.id())
                    .containsExactlyInAnyOrderElementsOf(childIdsOf(POLICIES, customer.id()));

            for (Document policy : policies) {
                long policyId = numberOf(policy.get("id"));
                String where = "policy " + policyId + " of customer " + customer.id();

                List<Document> claims = arrayAt(policy, "claims", where);
                assertThat(idsOf(claims))
                        .as("the claims of %s, three levels below the root that carries them", where)
                        .containsExactlyInAnyOrderElementsOf(childIdsOf(CLAIMS, policyId));

                for (Document claim : claims) {
                    long claimId = numberOf(claim.get("id"));
                    String under = "claim " + claimId + " of " + where;

                    List<Document> leaves = arrayAt(claim, "documents", under);
                    assertThat(idsOf(leaves))
                            .as("the documents of %s, whose rows name no customer at all", under)
                            .containsExactlyInAnyOrderElementsOf(childIdsOf(DOCUMENTS, claimId));

                    for (Document leaf : leaves) {
                        long leafId = numberOf(leaf.get("id"));
                        assertThat(leaf.get("title"))
                                .as("the payload of document %d under %s", leafId, under)
                                .isEqualTo(labelOf(DOCUMENTS, leafId));
                    }
                }
            }
        }
    }

    /** The ids of the seeded rows that hang under one parent, which is what an array at that path must hold. */
    private static List<Long> childIdsOf(List<Row> rows, long parentId) {
        return rows.stream().filter(row -> row.parentId() == parentId).map(Row::id).toList();
    }

    private static String labelOf(List<Row> rows, long id) {
        return rows.stream().filter(row -> row.id() == id).map(Row::label).findFirst().orElseThrow();
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
     * Four tables, each naming only the one above it. {@code documents} carrying no customer column is
     * the whole point of the fixture: it is what a single resolver cannot follow.
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
                statement.execute("CREATE TABLE " + DOCUMENT_TABLE
                        + " (id INT PRIMARY KEY, claim_id INT, title VARCHAR(64))");
            }
            insertRoots(connection);
            insertChildren(connection, POLICY_TABLE, "customer_id", "plan", POLICIES);
            insertChildren(connection, CLAIM_TABLE, "policy_id", "note", CLAIMS);
            insertChildren(connection, DOCUMENT_TABLE, "claim_id", "title", DOCUMENTS);
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

    /** One capture unit per table: four single-table sources are what puts four chains into one job. */
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
     * whose schema is discovered, and the document being assembled is the root's. Each embed joins on the
     * key of the level directly above it and on nothing else.
     */
    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: customer_dossier
                source: [ src_customers, src_policies, src_claims, src_documents ]
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: customer_doc
                    type: nest
                    from: { c: customers, p: policies, cl: claims, d: documents }
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
                            - from: cl
                              on: { policy_id: id }
                              as: array
                              path: claims
                              arrayKey: [ id ]
                              embed:
                                - from: d
                                  on: { claim_id: id }
                                  as: array
                                  path: documents
                                  arrayKey: [ id ]
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
