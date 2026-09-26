package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static org.assertj.core.api.Assertions.assertThat;

/** A real-process host witness: a large MySQL load must leave another CDC pipeline moving. */
@RequiresDocker
@EnabledIfSystemProperty(named = "tapstate.e2e.large-snapshot.jar", matches = ".+")
class RealLargeSnapshotParallelCdcIT {

    private static final String BOOT_JAR_PROPERTY = "tapstate.e2e.large-snapshot.jar";
    private static final String BULK_PIPELINE = "large_snapshot";
    private static final String FAST_PIPELINE = "parallel_cdc";
    private static final long BULK_ROWS = 524_288L;
    private static final Duration WAIT = Duration.ofMinutes(12);

    @BeforeAll
    static void requireConnectorsAndJar() {
        RealConnectorGate.require("mysql", "mongodb");
        assertThat(Files.isRegularFile(Path.of(System.getProperty(BOOT_JAR_PROPERTY))))
                .as("the explicitly requested boot JAR exists").isTrue();
    }

    @Test
    void aLargeSnapshotLeavesParallelCdcAndObservationsMoving() throws Exception {
        Map<String, Object> bulkMysql = SharedMySql.settings("large_snapshot_source");
        Map<String, Object> fastMysql = SharedMySql.settings("parallel_cdc_source");
        seedBulk(bulkMysql);
        seedFast(fastMysql);
        String storeUri = SharedMongo.replicaSetUrl("large_snapshot_store");
        String targetUri = SharedMongo.replicaSetUrl("large_snapshot_target");
        EndpointAddress target = EndpointAddress.uri(targetUri);
        Path jar = Path.of(System.getProperty(BOOT_JAR_PROPERTY));

        try (MongoEndpoints mongo = new MongoEndpoints();
                MongoClient targetClient = MongoClients.create(targetUri);
                RealProcessServer server = RealProcessServer.start(storeUri, jar)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("large-snapshot", "large-snapshot-password");
            control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            control.apply(resources(bulkMysql, fastMysql, targetUri));
            control.discoverSchema("bulk_source", "mysql", bulkMysql);
            control.discoverSchema("fast_source", "mysql", fastMysql);

            control.lifecycle(FAST_PIPELINE, LifecycleVerb.START);
            Await.until("parallel CDC baseline", Duration.ofMinutes(2),
                    () -> "before".equals(fastStatus(mongo, target)),
                    () -> "state=" + control.state(FAST_PIPELINE) + ", target=" + fastStatus(mongo, target));
            assertThat(control.state(FAST_PIPELINE)).contains(PipelineState.RUNNING);

            control.lifecycle(BULK_PIPELINE, LifecycleVerb.START);
            Await.until("bulk snapshot to be visibly in flight", Duration.ofMinutes(3),
                    () -> inFlightRows(control) > 1_000 && inFlightRows(control) < BULK_ROWS,
                    () -> "state=" + control.state(BULK_PIPELINE)
                            + ", rows=" + control.snapshotRowsRead(BULK_PIPELINE));
            long firstRows = inFlightRows(control);
            Instant firstObserved = control.statusObservedAt(BULK_PIPELINE);

            long issued = System.nanoTime();
            updateFast(fastMysql);
            Await.until("parallel CDC while the other snapshot is still running", Duration.ofMinutes(2),
                    () -> "after".equals(fastStatus(mongo, target)),
                    () -> "fast=" + control.state(FAST_PIPELINE)
                            + ", target=" + fastStatus(mongo, target)
                            + ", bulkRows=" + inFlightRows(control));
            long deliveredNanos = System.nanoTime() - issued;
            long rowsAtFastDelivery = inFlightRows(control);
            assertThat(rowsAtFastDelivery)
                    .as("the other pipeline delivered CDC before the bulk load finished")
                    .isBetween(1L, BULK_ROWS - 1L);
            Await.until("snapshot rows and freshness to progress while CDC stays live", Duration.ofSeconds(20),
                    () -> inFlightRows(control) > firstRows && inFlightRows(control) < BULK_ROWS
                            && control.statusObservedAt(BULK_PIPELINE).isAfter(firstObserved),
                    () -> "firstRows=" + firstRows + ", now=" + inFlightRows(control)
                            + ", firstObserved=" + firstObserved);

            Await.until("bulk target terminal coverage", WAIT,
                    () -> control.recordCount(BULK_PIPELINE).orElse(0L) >= BULK_ROWS,
                    () -> "recordsOut=" + control.recordCount(BULK_PIPELINE)
                            + ", snapshot=" + control.snapshotRowsRead(BULK_PIPELINE));
            String targetDatabase = new ConnectionString(targetUri).getDatabase();
            long physicallyDelivered = targetClient.getDatabase(targetDatabase)
                    .getCollection("bulk_orders").countDocuments();
            assertThat(physicallyDelivered).isEqualTo(BULK_ROWS);
            assertThat(control.errorCount(BULK_PIPELINE)).contains(0L);
            assertThat(control.errorCount(FAST_PIPELINE)).contains(0L);
            System.out.printf("large-snapshot-live jar=%s bulkRows=%d firstObservedRows=%d"
                            + " rowsWhenParallelCdcDelivered=%d parallelCdcMs=%.3f"
                            + " physicallyDelivered=%d%n",
                    jar, BULK_ROWS, firstRows, rowsAtFastDelivery,
                    deliveredNanos / 1_000_000.0, physicallyDelivered);
        }
    }

    private static long inFlightRows(ControlPlane control) {
        return control.snapshotRowsRead(BULK_PIPELINE).getOrDefault("bulk_orders", 0L);
    }

    private static String fastStatus(MongoEndpoints mongo, EndpointAddress target) {
        List<Document> rows = mongo.documents(target, "fast_orders");
        return rows.isEmpty() ? null : rows.getFirst().getString("status");
    }

    private static void seedBulk(Map<String, Object> mysql) throws Exception {
        try (Connection connection = SharedMySql.connect(mysql);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE bulk_orders (id BIGINT PRIMARY KEY, payload VARCHAR(64))");
            statement.execute("INSERT INTO bulk_orders VALUES (1, REPEAT('x', 64))");
            for (long count = 1; count < BULK_ROWS; count *= 2) {
                statement.execute("INSERT INTO bulk_orders (id, payload)"
                        + " SELECT id + " + count + ", payload FROM bulk_orders");
            }
        }
    }

    private static void seedFast(Map<String, Object> mysql) throws Exception {
        try (Connection connection = SharedMySql.connect(mysql);
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE fast_orders (id BIGINT PRIMARY KEY, status VARCHAR(16))");
            statement.execute("INSERT INTO fast_orders VALUES (1, 'before')");
        }
    }

    private static void updateFast(Map<String, Object> mysql) throws Exception {
        try (Connection connection = SharedMySql.connect(mysql);
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE fast_orders SET status='after' WHERE id=1");
        }
    }

    private static Map<String, String> resources(
            Map<String, Object> bulkMysql, Map<String, Object> fastMysql, String targetUri) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("bulk-source.tap.yml", source("bulk_source", "bulk_orders", bulkMysql));
        resources.put("fast-source.tap.yml", source("fast_source", "fast_orders", fastMysql));
        resources.put("target.tap.yml", """
                version: tapstate/v1
                kind: source
                id: parallel_target
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(targetUri));
        resources.put("bulk-pipeline.tap.yml",
                pipeline(BULK_PIPELINE, "bulk_source", "bulk_orders"));
        resources.put("fast-pipeline.tap.yml",
                pipeline(FAST_PIPELINE, "fast_source", "fast_orders"));
        return resources;
    }

    private static String source(String id, String table, Map<String, Object> mysql) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ %s ]
                """.formatted(id, mysql.get("host"), mysql.get("port"), mysql.get("database"),
                mysql.get("username"), mysql.get("password"), table);
    }

    private static String pipeline(String id, String source, String table) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: all_rows, from: [%s], type: filter, expr: "true" }
                serve:
                  from: all_rows
                  sync:
                    - source: parallel_target
                """.formatted(id, source, table);
    }
}
