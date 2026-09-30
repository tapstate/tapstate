package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/** The public pipeline stop releases the real source cursor held by its last local capture user. */
class StoppingAMongoCaptureKillsItsSourceCursorIT {
    private static final String DATABASE = "e2e_tail_close_source";
    private static final String TARGET_DATABASE = "e2e_tail_close_target";
    private static final String PIPELINE = "tail_close";

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @Test
    void stoppingTheLastPipelineKillsItsChangeStreamCursor() throws Exception {
        String sourceUri = SharedMongo.replicaSetUrl(DATABASE);
        String targetUri = SharedMongo.replicaSetUrl(TARGET_DATABASE);
        String storeUri = SharedMongo.replicaSetUrl("e2e_tail_close_store");
        // Surefire also carries the MCP sidecar's non-web configuration; this witness hosts the REST server.
        String previousWebType = System.getProperty("spring.main.web-application-type");
        System.setProperty("spring.main.web-application-type", "servlet");
        try (MongoClient observer = MongoClients.create(sourceUri);
                ServerHandle server = InProcessServer.start(storeUri)) {
            var orders = observer.getDatabase(DATABASE).getCollection("orders");
            orders.insertOne(new Document("_id", "seed").append("value", 1));
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            control.apply(Map.of(
                    "source.tap.yml", sourceYaml("src_mongo", sourceUri, DATABASE) + "mode: cdc\n",
                    "target.tap.yml", sourceYaml("tgt_mongo", targetUri, TARGET_DATABASE),
                    "pipeline.tap.yml", """
                            version: tapstate/v1
                            kind: pipeline
                            id: tail_close
                            source: src_mongo
                            settings: { read_mode: cdc_only }
                            transforms:
                              - { id: changes, from: [orders], type: filter, expr: "op == 'i'" }
                            serve:
                              from: changes
                              sync:
                                - source: tgt_mongo
                                  rename:
                                    map: { orders: copied_orders }
                            """));
            control.discoverSchema("src_mongo", "mongodb",
                    Map.of("uri", sourceUri, "database", DATABASE));
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            await(() -> !cursors(observer).isEmpty(), "the pipeline opened its source change stream");
            List<Document> opened = cursors(observer);
            assertThat(opened).as("one capture reads the source").hasSize(1);
            long cursorId = cursorId(opened.getFirst());
            orders.insertOne(new Document("_id", "after_start").append("value", 2));
            await(() -> observer.getDatabase(TARGET_DATABASE).getCollection("copied_orders")
                            .countDocuments(new Document("_id", "after_start")) == 1,
                    "a change reached the target before stopping");

            control.stop(PIPELINE, false);
            await(() -> control.state(PIPELINE).orElse(null) == PipelineState.STOPPED,
                    "the last pipeline stopped");
            long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
            List<Document> remaining;
            do {
                remaining = cursors(observer).stream().filter(op -> cursorId(op) == cursorId).toList();
                if (remaining.isEmpty()) {
                    break;
                }
                Thread.sleep(50);
            } while (System.nanoTime() < deadline);
            assertThat(remaining)
                    .as("stopping the last pipeline must kill source cursor %s; $currentOp: %s",
                            cursorId, remaining)
                    .isEmpty();
        } finally {
            if (previousWebType == null) {
                System.clearProperty("spring.main.web-application-type");
            } else {
                System.setProperty("spring.main.web-application-type", previousWebType);
            }
        }
    }

    private static long cursorId(Document operation) {
        return ((Number) operation.get("cursor", Document.class).get("cursorId")).longValue();
    }

    private static List<Document> cursors(MongoClient observer) {
        return observer.getDatabase("admin").aggregate(List.of(
                new Document("$currentOp", new Document("allUsers", true).append("idleCursors", true)),
                new Document("$match", new Document("cursor.originatingCommand.$db", DATABASE)
                        .append("cursor.originatingCommand.pipeline.0.$changeStream",
                                new Document("$exists", true)))))
                .into(new ArrayList<>());
    }

    private static String sourceYaml(String id, String uri, String database) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s", database: %s }
                tables: [ orders ]
                """.formatted(id, uri, database);
    }

    private static void await(BooleanSupplier ready, String description) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            if (ready.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        assertThat(ready.getAsBoolean()).as(description).isTrue();
    }
}
