package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Real MySQL source and Atlas target through the on-prem Pipeline runtime. */
@RequiresDocker
class RealMySqlToAtlasPipelineIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(120);
    private static final String COLLECTION = "orders";
    private static final String PIPELINE_ID = "mysql_to_atlas";

    @BeforeAll
    static void requireRealArtifactsAndAtlas() {
        assumeTrue(System.getenv("TAPSTATE_ATLAS_TEST_URI") != null,
                "a controlled Atlas URI is required for this live witness");
        RealConnectorGate.require("mysql", "mongodb-atlas");
    }

    @Test
    void mysqlSnapshotAndChangesReachAtlasWithDurableAckAcrossRestart() throws Exception {
        String suffix = UUID.randomUUID().toString().substring(0, 12);
        Map<String, Object> mysqlSettings = SharedMySql.settings("ts_plan_mysql_atlas_src_" + suffix);
        try (Connection mysql = SharedMySql.connect(mysqlSettings); Statement statement = mysql.createStatement()) {
            statement.execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, name VARCHAR(64))");
            statement.execute("INSERT INTO orders (id, name) VALUES (1, 'seeded')");
        }

        String targetDatabase = "ts_plan_mysql_atlas_tgt_" + suffix;
        String targetUri = inDatabase(System.getenv("TAPSTATE_ATLAS_TEST_URI"), targetDatabase);
        String storeUri = SharedMongo.replicaSetUrl("ts_plan_mysql_atlas_store_" + suffix);
        try (MongoClient atlas = MongoClients.create(targetUri)) {
            try {
                MongoCollection<Document> targetRows = atlas.getDatabase(targetDatabase).getCollection(COLLECTION);
                try (ServerHandle server = Tiers.IN_PROCESS.launch(storeUri)) {
                    ControlPlane control = new ControlPlane(server.baseUrl());
                    control.bootstrapAndLogin("e2e", "e2e-password");
                    control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                    control.registerConnector("mongodb-atlas", ConnectorJars.bytesFor("mongodb-atlas"));

                    Map<String, String> resources = new LinkedHashMap<>();
                    resources.put("mysql.tap.yml", sourceYaml(mysqlSettings));
                    resources.put("atlas.tap.yml", targetYaml(targetUri));
                    resources.put("pipeline.tap.yml", pipelineYaml());
                    control.apply(resources);
                    control.discoverSchema("mysql_source", "mysql", mysqlSettings);
                    control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

                    awaitNames(targetRows, List.of("seeded"), "MySQL snapshot to reach Atlas");
                    try (Connection mysql = SharedMySql.connect(mysqlSettings);
                         Statement statement = mysql.createStatement()) {
                        statement.execute("UPDATE orders SET name = 'updated' WHERE id = 1");
                        statement.execute("INSERT INTO orders (id, name) VALUES (2, 'new')");
                    }
                    awaitNames(targetRows, List.of("new", "updated"), "MySQL update and insert to reach Atlas");
                    try (Connection mysql = SharedMySql.connect(mysqlSettings);
                         Statement statement = mysql.createStatement()) {
                        statement.execute("DELETE FROM orders WHERE id = 2");
                    }
                    awaitNames(targetRows, List.of("updated"), "MySQL delete to reach Atlas");
                    String ackedBeforeRestart = Await.answered("Atlas target ACK after MySQL CDC", TIMEOUT,
                            () -> control.durablePosition(PIPELINE_ID, COLLECTION));

                    control.stop(PIPELINE_ID, false);
                    Await.until("MySQL-to-Atlas Pipeline to stop", TIMEOUT,
                            () -> control.state(PIPELINE_ID).filter(PipelineState.STOPPED::equals).isPresent(),
                            () -> String.valueOf(control.state(PIPELINE_ID)));
                    control.lifecycle(PIPELINE_ID, LifecycleVerb.START);
                    try (Connection mysql = SharedMySql.connect(mysqlSettings);
                         Statement statement = mysql.createStatement()) {
                        statement.execute("INSERT INTO orders (id, name) VALUES (3, 'resumed')");
                    }
                    awaitNames(targetRows, List.of("resumed", "updated"), "MySQL CDC resume to reach Atlas");
                    Await.until("Atlas target ACK to advance after restart", TIMEOUT,
                            () -> control.durablePosition(PIPELINE_ID, COLLECTION)
                                    .filter(position -> !position.equals(ackedBeforeRestart)).isPresent(),
                            () -> "target ACK did not advance after the resumed MySQL write");
                    assertThat(names(targetRows)).containsExactly("resumed", "updated");
                }
            } finally {
                atlas.getDatabase(targetDatabase).drop();
            }
        }
    }

    private static void awaitNames(MongoCollection<Document> rows, List<String> expected, String phase) {
        Await.until(phase, TIMEOUT, () -> names(rows).equals(expected), () -> names(rows).toString());
    }

    private static List<String> names(MongoCollection<Document> rows) {
        return rows.find().into(new java.util.ArrayList<>()).stream()
                .map(row -> row.getString("name"))
                .sorted().toList();
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
                config: { isUri: true, uri: "%s" }
                """.formatted(uri);
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
