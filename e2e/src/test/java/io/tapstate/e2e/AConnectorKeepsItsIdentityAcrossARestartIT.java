package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import java.sql.Connection;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A real change-capture connector mints an identity for itself on its first run and keeps it in the state
 * map the host gives it, reusing the stored one whenever it finds one. That identity is what its recorded
 * position is filed under -- so a run that cannot find it mints another, and then looks its position up
 * under a name nothing ever wrote it under. What comes back is nothing, and the stream refuses to start
 * rather than resume.
 *
 * <p>This is the half of that story a single branch can hold: the identity itself. It asserts the state
 * map is durable in the way the connector needs -- the identity is there after the first run, and the run
 * that comes back after the process is replaced is running under <em>the same one</em> rather than a fresh
 * one. Whether a matching identity is then enough for the stream to resume is a question for a build that
 * has a recorded position to resume from; that machinery is not on this branch, which is also why this
 * case cannot be written as a resume.
 *
 * <p>Read out of the store rather than off any product surface, and compared byte for byte: what is being
 * asserted is that the second run did not mint, and only the value itself says that.
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=AConnectorKeepsItsIdentityAcrossARestartIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class AConnectorKeepsItsIdentityAcrossARestartIT {

    private static final String TABLE = "orders";
    private static final String DATABASE = "identity_restart_db";
    private static final String PIPELINE_ID = "identity_across_restart";
    private static final String SOURCE_ID = "src_mysql";

    /** Where a connector's own notes are filed, derived from the pipeline node that opened it. */
    private static final String NAMESPACE = "pdk.state." + PIPELINE_ID + "." + SOURCE_ID;

    /** The note the MySQL connector mints on a first run and looks for on every later one. */
    private static final String SERVER_NAME = "SERVER_NAME";

    /** The database connector and operator state share, and the collection that holds it. */
    private static final String STATE_DATABASE = "tapstate_nest";
    private static final String STATE_COLLECTION = "operator_state";

    private static final String SEEDED = "seeded";
    private static final String BEFORE_THE_RESTART = "changed-before-the-restart";
    private static final String AFTER_THE_RESTART = "changed-after-the-restart";

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @Test
    void theRunThatComesBackIsTheOneThatMintedTheIdentityNotANewOne() throws Exception {
        Map<String, Object> mysql = SharedMySql.settings(DATABASE);
        seedOneRow(mysql);

        String storeUri = SharedMongo.replicaSetUrl("identity_restart_store");
        String targetUri = SharedMongo.replicaSetUrl("identity_restart_target");
        EndpointAddress target = EndpointAddress.uri(targetUri);

        byte[] minted;
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
                control.discoverSchema(SOURCE_ID, "mysql", mysql);
                control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

                awaitCustomer(mongo, target, SEEDED, "the snapshot to reach the target");
                // Carrying changes, so the connector is past its snapshot and into the drive that mints
                // and stores the identity.
                update(mysql, BEFORE_THE_RESTART);
                awaitCustomer(mongo, target, BEFORE_THE_RESTART, "a change made before the restart");

                Await.until("the connector to have filed the identity it minted",
                        () -> note(storeUri, SERVER_NAME).isPresent(),
                        () -> "nothing under " + NAMESPACE);
                minted = note(storeUri, SERVER_NAME).orElseThrow();
            }

            // The process is gone. The store it wrote, the source database and the target are not.
            try (ServerHandle second = Tiers.IN_PROCESS.launch(storeUri)) {
                ControlPlane control = new ControlPlane(second.baseUrl());
                control.login("e2e", "e2e-password");

                // Liveness first: a run that never started would leave the stored value untouched too, and
                // "unchanged" would then be satisfied by nothing having happened at all.
                update(mysql, AFTER_THE_RESTART);
                awaitCustomer(mongo, target, AFTER_THE_RESTART, "a change made after the restart");

                assertThat(note(storeUri, SERVER_NAME))
                        .as("the identity after a run that came back and is carrying changes")
                        .isPresent();
                assertThat(note(storeUri, SERVER_NAME).orElseThrow())
                        .as("the run that came back is running under the identity the first one minted, "
                                + "not one of its own: a fresh identity is what leaves a recorded position "
                                + "filed under a name nothing looks it up by")
                        .isEqualTo(minted);
            }
        }
    }

    /** One of the connector's own notes, read straight out of the store as the bytes it was written as. */
    private static Optional<byte[]> note(String storeUri, String key) {
        try (MongoClient client = MongoClients.create(storeUri)) {
            Document id = new Document("ns", NAMESPACE).append("k", key);
            Document found = client.getDatabase(STATE_DATABASE)
                    .getCollection(STATE_COLLECTION)
                    .find(new Document("_id", id))
                    .first();
            return Optional.ofNullable(found)
                    .map(document -> document.get("state", Binary.class))
                    .map(Binary::getData);
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
                id: %s
                source: src_mysql
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: all_rows, from: [orders], type: filter, expr: "true" }
                serve:
                  from: all_rows
                  sync:
                    - source: tgt_mongo
                """
                .formatted(PIPELINE_ID);
    }
}
