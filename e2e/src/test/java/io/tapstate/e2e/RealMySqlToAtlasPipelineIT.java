package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Real MySQL and Atlas through both runtime modes, including a shipped-JVM restart. */
@RequiresDocker
class RealMySqlToAtlasPipelineIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(120);
    private static final String COLLECTION = "orders";
    private static final String PIPELINE_ID = "mysql_to_atlas";
    private static final int SEEDED_ROWS = 32;

    @BeforeAll
    static void requireRealArtifactsAndAtlas() {
        assumeTrue(System.getenv("TAPSTATE_ATLAS_TEST_URI") != null,
                "a controlled Atlas URI is required for this live witness");
        RealConnectorGate.require("mysql", "mongodb-atlas");
    }

    @ParameterizedTest(name = "{0}/REAL_PROCESS")
    @EnumSource(AtlasRuntime.Mode.class)
    void mysqlSnapshotAndChangesReachAtlasAcrossAWholeJvmRestart(AtlasRuntime.Mode mode) throws Exception {
        String suffix = (mode == AtlasRuntime.Mode.CLOUD ? "cl" : "op") + "_"
                + UUID.randomUUID().toString().substring(0, 8);
        String sourceDatabase = "ts_plan_ms_" + suffix;
        Map<String, Object> mysqlSettings = SharedMySql.settings(sourceDatabase);
        try (AutoCloseable mysqlCleanup = () -> dropMySql(mysqlSettings, sourceDatabase)) {
            verify(mode, suffix, mysqlSettings);
        }
    }

    private static void verify(AtlasRuntime.Mode mode, String suffix, Map<String, Object> mysqlSettings)
            throws Exception {
        try (Connection mysql = SharedMySql.connect(mysqlSettings); Statement statement = mysql.createStatement()) {
            statement.execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, name VARCHAR(64))");
            statement.execute("INSERT INTO orders (id, name) VALUES (1, 'seeded')");
            for (int id = 2; id <= SEEDED_ROWS; id++) {
                statement.execute("INSERT INTO orders (id, name) VALUES (" + id + ", 'seed-" + id + "')");
            }
        }

        String targetDatabase = "ts_plan_mt_" + suffix;
        String targetUri = inDatabase(System.getenv("TAPSTATE_ATLAS_TEST_URI"), targetDatabase);
        String storeUri = SharedMongo.replicaSetUrl("ts_plan_mm_" + suffix);
        try (MongoClient atlas = MongoClients.create(targetUri);
             AtlasRuntime runtime = new AtlasRuntime(mode, storeUri)) {
            try (AutoCloseable targetCleanup = () -> atlas.getDatabase(targetDatabase).drop()) {
                MongoCollection<Document> targetRows = atlas.getDatabase(targetDatabase).getCollection(COLLECTION);
                String acknowledged;
                Map<Long, String> beforeShutdown;
                try (ServerHandle server = runtime.launch(Tiers.REAL_PROCESS)) {
                    ControlPlane control = runtime.control(server, true);
                    runtime.register(control, "mysql", "mongodb-atlas");

                    Map<String, String> resources = new LinkedHashMap<>();
                    resources.put("mysql.tap.yml", sourceYaml(mysqlSettings));
                    resources.put("atlas.tap.yml", targetYaml(targetUri));
                    resources.put("pipeline.tap.yml", pipelineYaml());
                    control.apply(resources);
                    control.discoverSchema("mysql_source", "mysql", mysqlSettings);
                    control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

                    awaitRows(targetRows, mysqlRows(mysqlSettings), "MySQL snapshot to reach Atlas");
                    String snapshotAck = Await.answered("Atlas snapshot ACK", TIMEOUT,
                            () -> control.durablePosition(PIPELINE_ID, COLLECTION));
                    try (Connection mysql = SharedMySql.connect(mysqlSettings);
                         Statement statement = mysql.createStatement()) {
                        statement.execute("UPDATE orders SET name = 'updated' WHERE id = 1");
                    }
                    awaitRows(targetRows, mysqlRows(mysqlSettings), "MySQL update to reach Atlas");
                    String updatedAck = Await.answered("Atlas update ACK", TIMEOUT,
                            () -> control.durablePosition(PIPELINE_ID, COLLECTION)
                                    .filter(position -> !position.equals(snapshotAck)));
                    try (Connection mysql = SharedMySql.connect(mysqlSettings);
                         Statement statement = mysql.createStatement()) {
                        statement.execute("INSERT INTO orders (id, name) VALUES (33, 'new')");
                    }
                    awaitRows(targetRows, mysqlRows(mysqlSettings), "MySQL insert to reach Atlas");
                    String insertedAck = Await.answered("Atlas insert ACK", TIMEOUT,
                            () -> control.durablePosition(PIPELINE_ID, COLLECTION)
                                    .filter(position -> !position.equals(updatedAck)));
                    try (Connection mysql = SharedMySql.connect(mysqlSettings);
                         Statement statement = mysql.createStatement()) {
                        statement.execute("DELETE FROM orders WHERE id = 33");
                    }
                    awaitRows(targetRows, mysqlRows(mysqlSettings), "MySQL delete to reach Atlas");
                    acknowledged = Await.answered("Atlas target ACK after the final MySQL delete", TIMEOUT,
                            () -> control.durablePosition(PIPELINE_ID, COLLECTION)
                                    .filter(position -> !position.equals(insertedAck)));
                    beforeShutdown = atlasRows(targetRows);
                    assertThat(control.snapshotRowsRead(PIPELINE_ID)).containsEntry(COLLECTION, (long) SEEDED_ROWS);
                }

                Instant stoppedAt = Instant.now();
                try (Connection mysql = SharedMySql.connect(mysqlSettings);
                     Statement statement = mysql.createStatement()) {
                    statement.execute("INSERT INTO orders (id, name) VALUES (34, 'downtime-inserted')");
                    statement.execute("UPDATE orders SET name = 'downtime-updated' WHERE id = 2");
                    statement.execute("DELETE FROM orders WHERE id = 3");
                }
                assertThat(atlasRows(targetRows)).as("the stopped JVM cannot carry downtime changes")
                        .isEqualTo(beforeShutdown);

                try (ServerHandle server = runtime.launch(Tiers.REAL_PROCESS)) {
                    ControlPlane control = runtime.control(server, false);
                    awaitRows(targetRows, mysqlRows(mysqlSettings), "MySQL downtime changes to reach Atlas");
                    String replayAck = Await.answered("Atlas downtime target ACK", TIMEOUT,
                            () -> control.durablePosition(PIPELINE_ID, COLLECTION)
                                    .filter(position -> !position.equals(acknowledged)));
                    try (Connection mysql = SharedMySql.connect(mysqlSettings);
                         Statement statement = mysql.createStatement()) {
                        statement.execute("INSERT INTO orders (id, name) VALUES (35, 'resumed')");
                    }
                    awaitRows(targetRows, mysqlRows(mysqlSettings), "fresh MySQL CDC after the JVM restart");
                    Await.until("Atlas target ACK to advance after restart", TIMEOUT,
                            () -> control.durablePosition(PIPELINE_ID, COLLECTION)
                                    .filter(position -> !position.equals(replayAck)).isPresent()
                                    && control.observedAt(PIPELINE_ID).filter(time -> time.isAfter(stoppedAt)).isPresent(),
                            () -> "target ACK did not advance after the resumed MySQL write");
                    AtlasRuntime.assertResumedRun(control, PIPELINE_ID, COLLECTION, SEEDED_ROWS, TIMEOUT);
                    assertThat(atlasRows(targetRows)).isEqualTo(mysqlRows(mysqlSettings));
                    assertThat(atlasRows(targetRows)).doesNotContainKeys(3L, 33L);
                    runtime.assertAuthenticationBoundary();
                }
            }
        }
    }

    private static void dropMySql(Map<String, Object> settings, String database) throws Exception {
        try (Connection mysql = SharedMySql.connect(settings); Statement statement = mysql.createStatement()) {
            statement.execute("DROP DATABASE `" + database + "`");
        }
    }

    private static void awaitRows(MongoCollection<Document> rows, Map<Long, String> expected, String phase) {
        Await.until(phase, TIMEOUT, () -> atlasRows(rows).equals(expected), () -> atlasRows(rows).toString());
    }

    private static Map<Long, String> atlasRows(MongoCollection<Document> rows) {
        Map<Long, String> result = new TreeMap<>();
        for (Document row : rows.find()) {
            assertThat(result.put(((Number) row.get("id")).longValue(), row.getString("name")))
                    .as("each source primary key has exactly one target row").isNull();
        }
        return result;
    }

    private static Map<Long, String> mysqlRows(Map<String, Object> settings) throws Exception {
        Map<Long, String> rows = new TreeMap<>();
        try (Connection mysql = SharedMySql.connect(settings); Statement statement = mysql.createStatement();
             var result = statement.executeQuery("SELECT id, name FROM orders ORDER BY id")) {
            while (result.next()) rows.put(result.getLong("id"), result.getString("name"));
        }
        return rows;
    }

    private static String sourceYaml(Map<String, Object> settings) {
        return """
                version: tapstate/v1
                kind: source
                id: mysql_source
                connector: mysql
                config: %s
                mode: cdc
                tables: [ orders ]
                """.formatted(JsonWriter.write(settings));
    }

    private static String targetYaml(String uri) {
        return """
                version: tapstate/v1
                kind: source
                id: atlas_target
                connector: mongodb-atlas
                config: %s
                """.formatted(JsonWriter.write(Map.of("isUri", true, "uri", uri)));
    }

    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: mysql_source
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: all_rows, from: [orders], type: filter, expr: "true" }
                serve:
                  from: all_rows
                  sync:
                    - source: atlas_target
                """.formatted(PIPELINE_ID);
    }

    private static String inDatabase(String uri, String database) {
        int schemeEnd = uri.indexOf("://");
        int slash = schemeEnd < 0 ? -1 : uri.indexOf('/', schemeEnd + 3);
        if (slash < 0) {
            throw new IllegalArgumentException("Atlas test URI must include a database path");
        }
        int options = uri.indexOf('?', slash);
        return uri.substring(0, slash + 1) + database
                + (options < 0 ? "" : uri.substring(options));
    }
}
