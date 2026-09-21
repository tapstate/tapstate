package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Two runs of one pipeline, each with a source and a store of its own, and the second does not serve the
 * first one's documents.
 *
 * <p>A pipeline is identified by the id its specification gives it, and so are the nest step and the view
 * inside it. Two runs of the same specification therefore agree on every one of those names while
 * agreeing on nothing else - different databases, different stores, different data. Anything the runtime
 * keeps under those names, rather than under something belonging to the run, is inherited by the second
 * run from the first.
 *
 * <p>This is the last surface that could explain a leak measured across two runs of one example: the
 * second run's source held the seeded value, its store began empty, and the step that writes the other
 * value never executed - yet the document it served carried the first run's value. Source scoping was
 * ruled out separately by {@link ACdcSourceReadsOnlyItsOwnDatabaseIT}, and the databases were measured to
 * be distinct, which leaves what the runtime carries between runs.
 *
 * <p>What the assertions have to discriminate:
 * <ul>
 *   <li><b>The second run must actually run.</b> A pipeline that failed to start serves nothing, which
 *       would satisfy "does not show the first run's value" for the wrong reason. So the second run is
 *       first held to serving its own seeded value.</li>
 *   <li><b>The value read back is one only the first run ever writes.</b> A value both runs could produce
 *       would not say which run produced it.</li>
 *   <li><b>The array is read as well as the scalar.</b> Inheriting an assembled document and inheriting a
 *       scalar are different failures, and the second is invisible to an assertion about children.</li>
 * </ul>
 *
 * <p>Gated on Docker and real connector jars. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=NestStateOutlivesARunAndIsDiscardedBetweenThemIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class NestStateOutlivesARunAndIsDiscardedBetweenThemIT {

    private static final String PARENTS = "orders";
    private static final String CHILDREN = "order_items";
    // A pipeline id of its own per test. The two tests would otherwise collide through exactly the
    // mechanism under examination: each starts by running the shared id once and writing the first run's
    // value under it, so whichever ran second would inherit before its own first run had begun.
    private static final String VIEW = "order_state";
    private static final String PIPELINE_ID = "assembled_orders_leak";
    private static final String BOUNDARY_PIPELINE_ID = "assembled_orders_boundary";
    private static final String CONFIGURED_PIPELINE_ID = "assembled_orders_configured_database";
    private static final String CONFIGURED_VIEW = "configured_order_state";
    private static final String CONFIGURED_STATE_A = "issue431_e2e_state_a";
    private static final String CONFIGURED_STATE_B = "issue431_e2e_state_b";

    private static final String SEEDED = "seeded-customer";
    /** Written only by the first run, so reading it in the second says where it came from. */
    private static final String ONLY_THE_FIRST_RUN = "written-by-the-first-run";

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @Test
    void aSecondRunOfTheSamePipelineInheritsTheFirstsAssemblyWhenTheStateIsLeftInPlace() throws Exception {
        // The first run assembles, then changes its parent row to a value nothing else ever writes.
        Map<String, Object> firstSource = SharedMySql.settings("nest_inherit_src_one");
        seed(firstSource);
        String firstWarehouse = SharedMongo.replicaSetUrl("nest_inherit_warehouse_one");
        try (ServerHandle server = Tiers.IN_PROCESS.launch(
                        SharedMongo.replicaSetUrl("nest_inherit_state_one"));
                MongoEndpoints mongo = new MongoEndpoints()) {
            start(server, firstSource, firstWarehouse);
            EndpointAddress warehouse = EndpointAddress.uri(firstWarehouse);
            awaitCustomer(mongo, warehouse, SEEDED, "the first run's own assembly");

            update(firstSource, ONLY_THE_FIRST_RUN);
            awaitCustomer(mongo, warehouse, ONLY_THE_FIRST_RUN, "the first run's own change");
        }

        // The second run: same pipeline id, same nest step id, same view id - and nothing else in common.
        Map<String, Object> secondSource = SharedMySql.settings("nest_inherit_src_two");
        seed(secondSource);
        String secondWarehouse = SharedMongo.replicaSetUrl("nest_inherit_warehouse_two");
        try (ServerHandle server = Tiers.IN_PROCESS.launch(
                        SharedMongo.replicaSetUrl("nest_inherit_state_two"));
                MongoEndpoints mongo = new MongoEndpoints()) {
            EndpointAddress warehouse = EndpointAddress.uri(secondWarehouse);
            assertThat(mongo.collections(secondWarehouse))
                    .as("the second run's store before it starts")
                    .isEmpty();

            start(server, secondSource, secondWarehouse);

            // What a deployment is promised: the assembly this pipeline id had is still there, so the
            // second run serves it without having read a single row of its own source.
            awaitCustomer(mongo, warehouse, ONLY_THE_FIRST_RUN,
                    "the assembly the first run left under this pipeline id");

            Document served = only(mongo.documents(warehouse, VIEW));
            assertThat(served.getString("customer"))
                    .as("the customer the second run serves with nest state left in place - it is the "
                            + "first run's value, from a source this run never read")
                    .isEqualTo(ONLY_THE_FIRST_RUN);
        }
    }

    /**
     * Two servers receive different deployment settings while every operator identity stays equal. The
     * database-location assertions are what make this a wiring witness: fixing the adapter while ignoring
     * the server setting, or reverting the assembly root to the compatibility default, leaves the named
     * database empty and fails before later source events could mask the leak.
     */
    @Test
    void configuredOperatorStateDatabasesKeepEqualPipelineIdentitiesApart() throws Exception {
        dropDatabases(CONFIGURED_STATE_A, CONFIGURED_STATE_B, SharedMongo.OPERATOR_STATE_DATABASE);

        Map<String, Object> firstSource = SharedMySql.settings("nest_configured_src_one");
        seed(firstSource);
        String firstWarehouse = SharedMongo.replicaSetUrl("nest_configured_warehouse_one");
        try (ServerHandle server = Tiers.IN_PROCESS.launch(
                        SharedMongo.replicaSetUrl("nest_configured_control_one"), CONFIGURED_STATE_A);
                MongoEndpoints mongo = new MongoEndpoints()) {
            start(server, firstSource, firstWarehouse, CONFIGURED_PIPELINE_ID, CONFIGURED_VIEW);
            EndpointAddress warehouse = EndpointAddress.uri(firstWarehouse);
            awaitCustomer(mongo, warehouse, CONFIGURED_VIEW, SEEDED,
                    "the first configured deployment's assembly");
            update(firstSource, ONLY_THE_FIRST_RUN);
            awaitCustomer(mongo, warehouse, CONFIGURED_VIEW, ONLY_THE_FIRST_RUN,
                    "the first configured deployment's change");
        }

        assertThat(operatorStateDocuments(CONFIGURED_STATE_A))
                .as("state written through the first server's deployment setting")
                .isPositive();
        assertThat(operatorStateDocuments(CONFIGURED_STATE_B)).isZero();
        assertThat(operatorStateDocuments(SharedMongo.OPERATOR_STATE_DATABASE))
                .as("the compatibility default was not selected by either explicit deployment")
                .isZero();

        Map<String, Object> secondSource = SharedMySql.settings("nest_configured_src_two");
        seed(secondSource);
        String secondWarehouse = SharedMongo.replicaSetUrl("nest_configured_warehouse_two");
        try (ServerHandle server = Tiers.IN_PROCESS.launch(
                        SharedMongo.replicaSetUrl("nest_configured_control_two"), CONFIGURED_STATE_B);
                MongoEndpoints mongo = new MongoEndpoints()) {
            start(server, secondSource, secondWarehouse, CONFIGURED_PIPELINE_ID, CONFIGURED_VIEW);
            EndpointAddress warehouse = EndpointAddress.uri(secondWarehouse);
            awaitCustomer(mongo, warehouse, CONFIGURED_VIEW, SEEDED,
                    "the second configured deployment's own assembly");
            assertThat(customer(mongo, warehouse, CONFIGURED_VIEW))
                    .as("the equal pipeline identity does not inherit the first deployment's value")
                    .isEqualTo(SEEDED);
        }

        assertThat(operatorStateDocuments(CONFIGURED_STATE_A)).isPositive();
        assertThat(operatorStateDocuments(CONFIGURED_STATE_B))
                .as("state written through the second server's deployment setting")
                .isPositive();
        assertThat(operatorStateDocuments(SharedMongo.OPERATOR_STATE_DATABASE)).isZero();
    }

    /**
     * The same two runs, with the second one's pipeline and view named differently - and nothing leaks.
     *
     * <p>Its sibling above changes exactly one thing relative to this: whether the two runs agree on those
     * names. Run as a pair they say where the boundary is, which is what turns "a leak happened" into
     * something anyone can act on. Neither is worth much alone: this one passing proves only that two
     * unrelated pipelines do not collide, which nobody doubted.
     */
    @Test
    void discardingTheStateBetweenRunsIsWhatMakesTheSecondServeItsOwnData() throws Exception {
        Map<String, Object> firstSource = SharedMySql.settings("nest_boundary_src_one");
        seed(firstSource);
        String firstWarehouse = SharedMongo.replicaSetUrl("nest_boundary_warehouse_one");
        try (ServerHandle server = Tiers.IN_PROCESS.launch(
                        SharedMongo.replicaSetUrl("nest_boundary_state_one"));
                MongoEndpoints mongo = new MongoEndpoints()) {
            start(server, firstSource, firstWarehouse, BOUNDARY_PIPELINE_ID, VIEW);
            EndpointAddress warehouse = EndpointAddress.uri(firstWarehouse);
            awaitCustomer(mongo, warehouse, SEEDED, "the first run's own assembly");
            update(firstSource, ONLY_THE_FIRST_RUN);
            awaitCustomer(mongo, warehouse, ONLY_THE_FIRST_RUN, "the first run's own change");
        }

        // The one line that separates the two runs. Without it this test is its sibling above.
        SharedMongo.discardNestState();

        Map<String, Object> secondSource = SharedMySql.settings("nest_boundary_src_two");
        seed(secondSource);
        String secondWarehouse = SharedMongo.replicaSetUrl("nest_boundary_warehouse_two");
        try (ServerHandle server = Tiers.IN_PROCESS.launch(
                        SharedMongo.replicaSetUrl("nest_boundary_state_two"));
                MongoEndpoints mongo = new MongoEndpoints()) {
            // The one line under test is the discard; everything else, the view identity
            // included, matches the first run so nothing but the discard separates them.
            start(server, secondSource, secondWarehouse, BOUNDARY_PIPELINE_ID, VIEW);
            EndpointAddress warehouse = EndpointAddress.uri(secondWarehouse);
            awaitCustomer(mongo, warehouse, SEEDED, "the second run's own assembly");

            assertThat(customer(mongo, warehouse, VIEW))
                    .as("the customer the second run serves once the state has been discarded")
                    .isEqualTo(SEEDED);
        }
    }

    private static void start(ServerHandle server, Map<String, Object> source, String warehouseUri) {
        start(server, source, warehouseUri, PIPELINE_ID, VIEW);
    }

    private static void start(ServerHandle server, Map<String, Object> source, String warehouseUri,
            String pipelineId, String view) {
        ControlPlane control = new ControlPlane(server.baseUrl());
        control.bootstrapAndLogin("e2e", "e2e-password");
        control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
        control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("src_parents.tap.yml", sourceYaml("src_parents", source, PARENTS));
        resources.put("src_children.tap.yml", sourceYaml("src_children", source, CHILDREN));
        resources.put("views.tap.yml", viewsYaml(warehouseUri));
        resources.put("pipeline.tap.yml", pipelineYaml(pipelineId, view));
        control.apply(resources);
        control.discoverSchema("src_parents", "mysql", source);
        control.discoverSchema("src_children", "mysql", source);
        control.lifecycle(pipelineId, LifecycleVerb.START);
    }

    /** One parent with two children, identical in both runs so only the changed value tells them apart. */
    private static void seed(Map<String, Object> settings) throws Exception {
        try (Connection connection = SharedMySql.connect(settings);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + CHILDREN);
            statement.execute("DROP TABLE IF EXISTS " + PARENTS);
            statement.execute("CREATE TABLE " + PARENTS + " (id INT PRIMARY KEY, customer VARCHAR(64))");
            statement.execute("CREATE TABLE " + CHILDREN
                    + " (id INT PRIMARY KEY, order_id INT, sku VARCHAR(64))");
            statement.execute("INSERT INTO " + PARENTS + " (id, customer) VALUES (1, '" + SEEDED + "')");
            statement.execute("INSERT INTO " + CHILDREN
                    + " (id, order_id, sku) VALUES (1, 1, 'sku-1'), (2, 1, 'sku-2')");
        }
    }

    private static void update(Map<String, Object> settings, String customer) throws Exception {
        try (Connection connection = SharedMySql.connect(settings);
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE " + PARENTS + " SET customer = '" + customer + "' WHERE id = 1");
        }
    }

    private static void awaitCustomer(
            MongoEndpoints mongo, EndpointAddress warehouse, String expected, String what) {
        awaitCustomer(mongo, warehouse, VIEW, expected, what);
    }

    private static void awaitCustomer(MongoEndpoints mongo, EndpointAddress warehouse, String view,
            String expected, String what) {
        Await.until(what,
                () -> expected.equals(customer(mongo, warehouse, view)),
                () -> String.valueOf(customer(mongo, warehouse, view)));
    }

    private static String customer(MongoEndpoints mongo, EndpointAddress warehouse, String view) {
        List<Document> documents = mongo.documents(warehouse, view);
        return documents.isEmpty() ? null : documents.getFirst().getString("customer");
    }

    private static Document only(List<Document> documents) {
        assertThat(documents).as("the documents the view holds").hasSize(1);
        return documents.getFirst();
    }

    private static void dropDatabases(String... databases) {
        try (MongoClient client = MongoClients.create(SharedMongo.replicaSetUrl(databases[0]))) {
            for (String database : databases) {
                client.getDatabase(database).drop();
            }
        }
    }

    private static long operatorStateDocuments(String database) {
        try (MongoClient client = MongoClients.create(SharedMongo.replicaSetUrl(database))) {
            return client.getDatabase(database)
                    .getCollection(MongoStorePort.OPERATOR_STATE)
                    .countDocuments();
        }
    }

    private static String sourceYaml(String id, Map<String, Object> config, String table) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ %s ]
                """
                .formatted(id, config.get("host"), config.get("port"), config.get("database"),
                        config.get("username"), config.get("password"), table);
    }

    private static String viewsYaml(String uri) {
        return """
                version: tapstate/v1
                kind: source
                id: views
                connector: mongodb
                config: { uri: "%s" }
                """
                .formatted(uri);
    }

    private static String pipelineYaml(String pipelineId, String view) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: [ src_parents, src_children ]
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
                view:
                  id: %s
                  from: order_doc
                  primary_key: id
                """.formatted(pipelineId, view);
    }
}
