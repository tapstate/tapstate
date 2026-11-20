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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A filter that compares two DATETIME values of one row keeps the newer version of that row when an
 * update carrying an older timestamp arrives after it.
 *
 * <p>The source is a MySQL table whose {@code updated} column is a DATETIME, so the expression compares
 * the values the connector really delivers, not text. The filter is the form a late-data rule has to take
 * on a membership filter: {@code op != 'u' || (after != before && after.updated >= before.updated)}. The
 * plain {@code after.updated >= before.updated} would retract the row on a stale update, because a filter
 * emits a delete when an update's after image stops matching while its before image matched.
 *
 * <p>What the assertions have to discriminate:
 * <ul>
 *   <li><b>The stale update is dropped, not merely late.</b> After it, a fresh update to another row acts as
 *       a barrier in the binary log's own order; once that arrives, the stale one has been processed, and
 *       the first row still holds its seeded value.</li>
 *   <li><b>The filter is not dropping every update.</b> A fresh update to the first row afterwards
 *       arrives.</li>
 *   <li><b>The row is not deleted.</b> The target still holds both rows.</li>
 * </ul>
 *
 * <p>Mutation evidence: restore RowExpressions and RowExpressionProgram to their state before same-type
 * temporal ordering was declared and bound. This case then fails at its apply with HTTP 400
 * {@code dsl.row-expression-type-unsupported} naming column {@code updated} of type DATETIME; with the
 * change in place it passes. No other file was changed for the mutation.
 *
 * <p>Gated on Docker and on real connector jars, like every real-connector witness. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=AnOlderUpdateIsFilteredOutByItsOwnTimestampIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class AnOlderUpdateIsFilteredOutByItsOwnTimestampIT {

    private static final String TABLE = "players";
    private static final String DATABASE = "late_update_db";
    private static final String PIPELINE_ID = "newest_players";

    private static final String SEEDED = "seeded";
    private static final String STALE = "stale-older-timestamp";
    private static final String BARRIER = "barrier-on-another-row";
    private static final String FRESH = "fresh-newer-timestamp";

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @Test
    void aStaleUpdateLeavesTheNewerVersionInPlace() throws Exception {
        Map<String, Object> mysql = SharedMySql.settings(DATABASE);
        execute(mysql,
                "DROP TABLE IF EXISTS " + TABLE,
                "CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, status VARCHAR(64), updated DATETIME NOT NULL)",
                "INSERT INTO " + TABLE + " (id, status, updated) VALUES "
                        + "(1, '" + SEEDED + "', '2026-11-01 12:00:00'), (2, '" + SEEDED + "', '2026-11-01 12:00:00')");
        String targetUri = SharedMongo.replicaSetUrl("late_update_target");

        try (PreparedNativeStore storage = PreparedNativeStore.create();
                ServerHandle server = Tiers.IN_PROCESS.launch(storage);
                MongoEndpoints mongo = new MongoEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

            // Discovered before the apply, so the expression is judged against the DATETIME column.
            control.discoverSchema("src_mysql", "mysql", mysql);
            Map<String, String> resources = new LinkedHashMap<>();
            resources.put("src_mysql.tap.yml", sourceYaml(mysql));
            resources.put("tgt_mongo.tap.yml", targetYaml(targetUri));
            resources.put("pipeline.tap.yml", pipelineYaml());
            control.apply(resources);
            control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

            EndpointAddress target = EndpointAddress.uri(targetUri);
            awaitStatus(mongo, target, 1, SEEDED, "the snapshot of row 1");
            awaitStatus(mongo, target, 2, SEEDED, "the snapshot of row 2");

            execute(mysql,
                    "UPDATE " + TABLE + " SET status = '" + STALE + "', updated = '2020-01-01 00:00:00' WHERE id = 1",
                    "UPDATE " + TABLE + " SET status = '" + BARRIER + "', updated = '2026-11-02 12:00:00' WHERE id = 2");
            awaitStatus(mongo, target, 2, BARRIER, "a fresh update to row 2, written after the stale one");
            assertThat(status(mongo, target, 1))
                    .as("row 1 once the stream has passed an update stamped older than the row it replaces - "
                            + "%s means the older version overwrote the newer one, and null that the filter "
                            + "retracted the row", STALE)
                    .isEqualTo(SEEDED);

            execute(mysql,
                    "UPDATE " + TABLE + " SET status = '" + FRESH + "', updated = '2026-11-03 12:00:00' WHERE id = 1");
            awaitStatus(mongo, target, 1, FRESH, "a fresh update to row 1 after the stale one");
            assertThat(mongo.documents(target, TABLE)).as("the target's rows").hasSize(2);
        }
    }

    private static void execute(Map<String, Object> settings, String... statements) throws Exception {
        try (Connection connection = SharedMySql.connect(settings);
                Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }

    private static void awaitStatus(MongoEndpoints mongo, EndpointAddress target, int id, String expected, String what) {
        Await.until(what, () -> expected.equals(status(mongo, target, id)),
                () -> String.valueOf(mongo.documents(target, TABLE)));
    }

    /** The status of one target row, or null while the row is absent. Read from Mongo, not the product. */
    private static String status(MongoEndpoints mongo, EndpointAddress target, int id) {
        List<Document> documents = mongo.documents(target, TABLE);
        return documents.stream()
                .filter(document -> document.get("id") instanceof Number number && number.intValue() == id)
                .findFirst()
                .map(document -> document.getString("status"))
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
                tables: [ players ]
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
                id: newest_players
                source: src_mysql
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: newest
                    from: [players]
                    type: filter
                    expr: "op != 'u' || (after != before && after.updated >= before.updated)"
                serve:
                  from: newest
                  sync:
                    - source: tgt_mongo
                """;
    }
}
