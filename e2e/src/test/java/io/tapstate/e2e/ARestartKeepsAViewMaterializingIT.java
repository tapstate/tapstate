package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A pipeline that materializes a view is still materializing it after the server is restarted.
 *
 * <p>Two companion cases restart a server and the pipeline keeps carrying: one over the file connector,
 * one over MySQL into Mongo. Both send their rows to a declared target. The shape that was reported as
 * broken sends them to a <b>view</b> instead, and that is not the same sink: a target is written and
 * forgotten, while a view is materialized into the managed state store, which is in the very store the
 * restarted process reads back. So a run that comes up onto a view has something of the previous run's to
 * agree with, and the two cases that pass cannot say whether it does.
 *
 * <p>This case is the reported shape with one variable removed. The demo it is modelled on is a single
 * MySQL source, a transform, and a view keyed on the row id; the report reached it through an upgrade,
 * where the build changed and the process restarted together. Here the build is the same at both ends and
 * only the process is replaced, so a failure is the restart's and nothing else's.
 *
 * <p>The filter admits the change operations as well as the snapshot read: a view fed only {@code op ==
 * 'r'} is materialized once and never again, and every assertion after the first would be measuring a
 * pipeline that was never asked to carry anything.
 *
 * <p>Carrying is proven live before the restart; the view is read whole and compared to a value nothing
 * else writes; and it is held to the pre-restart value immediately before the last change is made, so the
 * wait that follows can only be satisfied by a change that crossed after the restart.
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=ARestartKeepsAViewMaterializingIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class ARestartKeepsAViewMaterializingIT {

    private static final String TABLE = "orders";
    private static final String DATABASE = "restart_view_db";
    private static final String PIPELINE_ID = "mysql2view";
    private static final String VIEW_ID = "order_state";

    private static final String SEEDED = "seeded";
    private static final String BEFORE_THE_RESTART = "changed-before-the-restart";
    private static final String AFTER_THE_RESTART = "changed-after-the-restart";

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @Test
    void aChangeMadeAfterTheServerIsRestartedStillReachesTheView() throws Exception {
        Map<String, Object> mysql = SharedMySql.settings(DATABASE);
        seedOneRow(mysql);

        String storeUri = SharedMongo.replicaSetUrl("restart_view_store");
        String viewUri = SharedMongo.replicaSetUrl("restart_view_views");
        EndpointAddress views = EndpointAddress.uri(viewUri);

        try (MongoEndpoints mongo = new MongoEndpoints()) {
            try (ServerHandle first = Tiers.IN_PROCESS.launch(storeUri)) {
                ControlPlane control = new ControlPlane(first.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");
                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

                Map<String, String> resources = new LinkedHashMap<>();
                resources.put("src_mysql.tap.yml", sourceYaml(mysql));
                resources.put("views.tap.yml", stateStoreYaml(viewUri));
                resources.put("pipeline.tap.yml", pipelineYaml());
                control.apply(resources);
                control.discoverSchema("src_mysql", "mysql", mysql);
                control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

                awaitCustomer(mongo, views, SEEDED, "the snapshot to materialize into the view");
                update(mysql, BEFORE_THE_RESTART);
                awaitCustomer(mongo, views, BEFORE_THE_RESTART, "a change made before the restart");
            }

            // The process is gone. The store holding the view's own state, the source database and the
            // materialized view are not, and the jar that comes back up is the one that went down.
            try (ServerHandle second = Tiers.IN_PROCESS.launch(storeUri)) {
                ControlPlane control = new ControlPlane(second.baseUrl());
                control.login("e2e", "e2e-password");

                assertThat(control.state(PIPELINE_ID))
                        .as("the pipeline the restarted server adopted from the store it read")
                        .contains(PipelineState.RUNNING);
                assertThat(customer(mongo, views))
                        .as("the view before the last change is made - the wait below has to be earned")
                        .isEqualTo(BEFORE_THE_RESTART);

                update(mysql, AFTER_THE_RESTART);
                awaitCustomer(mongo, views, AFTER_THE_RESTART, "a change made after the restart");
            }
        }
    }

    private static void seedOneRow(Map<String, Object> settings) throws Exception {
        try (Connection connection = SharedMySql.connect(settings);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + TABLE);
            statement.execute("CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, customer VARCHAR(64))");
            statement.execute("INSERT INTO " + TABLE + " (id, customer) VALUES (1, '" + SEEDED + "')");
        }
    }

    private static void update(Map<String, Object> settings, String customer) throws Exception {
        try (Connection connection = SharedMySql.connect(settings);
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE " + TABLE + " SET customer = '" + customer + "' WHERE id = 1");
        }
    }

    private static void awaitCustomer(
            MongoEndpoints mongo, EndpointAddress views, String expected, String what) {
        Await.until(what, () -> expected.equals(customer(mongo, views)),
                () -> String.valueOf(customer(mongo, views)));
    }

    /** The one view row's customer, or null before it is there. Read from Mongo, not the product. */
    private static String customer(MongoEndpoints mongo, EndpointAddress views) {
        List<Document> documents = mongo.documents(views, VIEW_ID);
        return Optional.ofNullable(documents.isEmpty() ? null : documents.getFirst())
                .map(document -> document.getString("customer"))
                .orElse(null);
    }

    private static String sourceYaml(Map<String, Object> config) {
        return """
                version: tapstate/v1
                kind: source
                id: src_mysql
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ orders ]
                """
                .formatted(config.get("host"), config.get("port"), config.get("database"),
                        config.get("username"), config.get("password"));
    }

    private static String stateStoreYaml(String viewUri) {
        return """
                version: tapstate/v1
                kind: source
                id: views
                connector: mongodb
                config: { uri: "%s" }
                """
                .formatted(viewUri);
    }

    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: mysql2view
                source: src_mysql
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: rows_through, from: [orders], type: filter, expr: "op == 'r' || op == 'i' || op == 'u'" }
                view:
                  id: order_state
                  from: rows_through
                  primary_key: id
                  storage:
                    warm:
                      collection: order_state
                """;
    }
}
