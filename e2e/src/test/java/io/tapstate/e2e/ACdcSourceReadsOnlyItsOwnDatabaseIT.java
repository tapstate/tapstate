package io.tapstate.e2e;

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
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A source reads changes from the database it names, and from no other database on the same server.
 *
 * <p>A MySQL server keeps one binary log for all of its databases. A change-capture source therefore
 * reads a stream carrying every database's changes and has to decide which of them are its own. Deciding
 * that on the table name alone is enough to look correct on any single-database machine and on every
 * example that runs by itself - two databases only ever collide when both hold a table of the same name,
 * which is exactly what happens when the same specification runs twice against two databases of its own.
 *
 * <p>This is a question about the product, asked directly, because the shape it was found in could not
 * answer it. Two runs of one example leaked a value between them; the store and the source database were
 * measured to be separate for each, which left the shared server as the only path and the binary log as
 * the only mechanism - a narrowing, not a demonstration. So: one server, two databases, the same table
 * name in both, a pipeline told to read exactly one of them.
 *
 * <p>What the assertions have to discriminate:
 * <ul>
 *   <li><b>The negative alone proves nothing.</b> A pipeline that died at start-up, or never reached its
 *       change stream, also fails to deliver the other database's change - and would pass a test that
 *       only checked the target had not moved. So the run first proves change capture is live by changing
 *       the watched database and seeing that arrive.</li>
 *   <li><b>Then the unwatched database is changed</b>, to a value nothing else in the run ever writes, and
 *       the target is held to the watched value for a bounded wait. A value that could arrive from
 *       anywhere would not say where it came from; this one can only have come across the database
 *       boundary.</li>
 *   <li><b>The row is read whole</b> rather than counted. The two databases hold the same ids, so a leak
 *       rewrites a row rather than adding one, and no count would move.</li>
 * </ul>
 *
 * <p>Gated on Docker and on real connector jars, like every real-connector witness. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=ACdcSourceReadsOnlyItsOwnDatabaseIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class ACdcSourceReadsOnlyItsOwnDatabaseIT {


    private static final String TABLE = "orders";
    private static final String WATCHED = "watched_db";
    private static final String UNWATCHED = "unwatched_db";
    private static final String PIPELINE_ID = "mysql2mongo";

    private static final String SEEDED = "seeded";
    private static final String WATCHED_CHANGE = "changed-in-the-watched-database";
    private static final String LEAK = "changed-in-the-unwatched-database-before-the-start";
    private static final String LEAK_WHILE_LIVE = "changed-in-the-unwatched-database-while-live";
    private static final String WATCHED_CHANGE_AGAIN = "changed-in-the-watched-database-again";

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @Test
    void aChangeInAnotherDatabaseOnTheSameServerDoesNotReachTheTarget() throws Exception {
        // One server, two databases on it - provisioned through the shared helper so each gets the grants
        // and the replication privileges a change-capture read needs, exactly as any other witness does.
        Map<String, Object> watched = SharedMySql.settings(WATCHED);
        Map<String, Object> unwatched = SharedMySql.settings(UNWATCHED);
        seedOneRow(watched);
        seedOneRow(unwatched);
        // Before the pipeline exists, so the change is already history by the time the source is
        // positioned. This is the shape the leak was found in: one run changes its own database and a
        // later run, reading the same server from an earlier position, replays it. A change made only
        // after the source is live tests the narrower half and would pass either way.
        update(unwatched, LEAK);
        {
            String targetUri = SharedMongo.replicaSetUrl("cdc_scope_target");

            try (PreparedNativeStore storage = PreparedNativeStore.create();
                    ServerHandle server = Tiers.IN_PROCESS.launch(storage);
                    MongoEndpoints mongo = new MongoEndpoints()) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                storage.loginAndAwaitBusiness(control);
                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

                Map<String, Object> watchedConfig = watched;
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put("src_mysql.tap.yml", sourceYaml(watchedConfig));
                resources.put("tgt_mongo.tap.yml", targetYaml(targetUri));
                resources.put("pipeline.tap.yml", pipelineYaml());
                control.apply(resources);
                control.discoverSchema("src_mysql", "mysql", watchedConfig);
                control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

                EndpointAddress target = EndpointAddress.uri(targetUri);
                // The watched database still says seeded; the unwatched one said LEAK before this
                // pipeline was created. Reaching seeded proves the snapshot landed, not that nothing leaked.
                // The watched database still says seeded; the unwatched one said LEAK before this
                // pipeline was created. Reaching seeded proves the snapshot landed, not that nothing leaked.
                awaitCustomer(mongo, target, SEEDED, "the snapshot of the watched database");

                // The barrier is the log's own order, not a fixed wait. The unwatched change is already
                // behind this point in the binary log, so a source replaying other databases would have
                // applied it before anything written later could arrive. Waiting for a watched change
                // made now therefore proves the stream has passed the unwatched one - and the reading
                // taken immediately after says whether it was applied. A timed quiet window would assert
                // the same thing while only guessing how long to hold its breath.
                update(watched, WATCHED_CHANGE);
                awaitCustomer(mongo, target, WATCHED_CHANGE, "a change made in the watched database");
                assertThat(customer(mongo, target))
                        .as("the target once the stream has passed a change made in another database "
                                + "before this pipeline started - %s means the source matched it by table "
                                + "name across databases", LEAK)
                        .isEqualTo(WATCHED_CHANGE);

                // And again for the live half: a change to the unwatched database while the source is
                // positioned and reading, overtaken by a watched change that acts as the same barrier.
                update(unwatched, LEAK_WHILE_LIVE);
                update(watched, WATCHED_CHANGE_AGAIN);
                awaitCustomer(mongo, target, WATCHED_CHANGE_AGAIN, "a second change in the watched database");
                assertThat(customer(mongo, target))
                        .as("the target once the stream has passed a change made in another database "
                                + "while this pipeline was live - %s means the same leak", LEAK_WHILE_LIVE)
                        .isEqualTo(WATCHED_CHANGE_AGAIN);
            }
        }
    }

    /** One row, keyed the same in both databases so a leak overwrites a row rather than adding one. */
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
        Await.until(what, () -> expected.equals(customer(mongo, target)), () -> String.valueOf(customer(mongo, target)));
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
