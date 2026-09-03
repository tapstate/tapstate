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
 * The restart question asked again over a real change-capture connector, because the answer over the
 * synthetic one was yes and that is not the shape the failure was reported in.
 *
 * <p>The companion case restarts a server over the file connector and the pipeline keeps carrying. The
 * reported failure is over MySQL into Mongo, and it arrived through an upgrade -- the image changed and
 * the process restarted in one move, so two variables moved together and neither could be blamed. This
 * case moves exactly one of them: same build, same jar, same store, only the process is replaced. What it
 * answers is which half to investigate.
 *
 * <ul>
 *   <li><b>Red here</b> means the restart is enough on its own over a real connector, and the upgrade is
 *       incidental -- the investigation belongs in how a real change-capture source resumes.</li>
 *   <li><b>Green here</b> means the restart alone is not enough, and what is left as the difference is the
 *       change of version across the store -- a different investigation entirely.</li>
 * </ul>
 *
 * <p>Carrying is proven live before the restart, so a target that stops moving after it cannot be confused
 * with a pipeline that never carried. The row is read whole and compared to a value nothing else writes,
 * and the target is held to the pre-restart value immediately before the last change is made, so the wait
 * that follows can only be satisfied by a change that crossed after the restart.
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=ARestartOverARealConnectorKeepsCarryingIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class ARestartOverARealConnectorKeepsCarryingIT {

    private static final String TABLE = "orders";
    private static final String DATABASE = "restart_real_db";
    private static final String PIPELINE_ID = "mysql2mongo";

    private static final String SEEDED = "seeded";
    private static final String BEFORE_THE_RESTART = "changed-before-the-restart";
    private static final String AFTER_THE_RESTART = "changed-after-the-restart";

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @Test
    void aChangeMadeAfterTheServerIsRestartedStillReachesTheTarget() throws Exception {
        Map<String, Object> mysql = SharedMySql.settings(DATABASE);
        seedOneRow(mysql);

        String storeUri = SharedMongo.replicaSetUrl("restart_real_store");
        String targetUri = SharedMongo.replicaSetUrl("restart_real_target");
        EndpointAddress target = EndpointAddress.uri(targetUri);

        try (MongoEndpoints mongo = new MongoEndpoints()) {
            try (ServerHandle first = Tiers.IN_PROCESS.launch(storeUri)) {
                ControlPlane control = new ControlPlane(first.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");
                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

                Map<String, String> resources = new LinkedHashMap<>();
                resources.put("src_mysql.tap.yml", sourceYaml(mysql));
                resources.put("tgt_mongo.tap.yml", targetYaml(targetUri));
                resources.put("pipeline.tap.yml", pipelineYaml());
                control.apply(resources);
                control.discoverSchema("src_mysql", "mysql", mysql);
                control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

                awaitCustomer(mongo, target, SEEDED, "the snapshot to reach the target");
                // Live before the restart: without this the wait after it measures nothing in particular.
                update(mysql, BEFORE_THE_RESTART);
                awaitCustomer(mongo, target, BEFORE_THE_RESTART, "a change made before the restart");
            }

            // The process is gone. The store it wrote, the source database and the target are not, and the
            // jar that comes back up is the same one that went down.
            try (ServerHandle second = Tiers.IN_PROCESS.launch(storeUri)) {
                ControlPlane control = new ControlPlane(second.baseUrl());
                control.login("e2e", "e2e-password");

                assertThat(control.state(PIPELINE_ID))
                        .as("the pipeline the restarted server adopted from the store it read")
                        .contains(PipelineState.RUNNING);
                assertThat(customer(mongo, target))
                        .as("the target before the last change is made - the wait below has to be earned")
                        .isEqualTo(BEFORE_THE_RESTART);

                update(mysql, AFTER_THE_RESTART);
                awaitCustomer(mongo, target, AFTER_THE_RESTART, "a change made after the restart");
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
            MongoEndpoints mongo, EndpointAddress target, String expected, String what) {
        Await.until(what, () -> expected.equals(customer(mongo, target)),
                () -> String.valueOf(customer(mongo, target)));
    }

    /** The one target row's customer, or null before the row is there. Read from Mongo, not the product. */
    private static String customer(MongoEndpoints mongo, EndpointAddress target) {
        List<Document> documents = mongo.documents(target, TABLE);
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

    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: mysql2mongo
                source: src_mysql
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: all_rows, from: [orders], type: filter, expr: "true" }
                serve:
                  from: all_rows
                  sync:
                    - source: tgt_mongo
                """;
    }
}
