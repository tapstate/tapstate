package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** A quiet CDC pipeline's frontier agrees with independently reconciled target contents. */
class SettledMongoCdcFrontierIT {

    private static final String PIPELINE = "settled_mongo_frontier";
    private static final String SOURCE_DB = "settled_frontier_source";
    private static final String TARGET_DB = "settled_frontier_target";
    private static final Duration TIMEOUT = Duration.ofSeconds(90);

    @BeforeAll
    static void requireRuntime() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @Test
    void reconciledSnapshotAndChangesDoNotLeaveAQuietFrontierPinned() {
        String sourceUri = SharedMongo.replicaSetUrl(SOURCE_DB);
        String targetUri = SharedMongo.replicaSetUrl(TARGET_DB);
        String storeUri = SharedMongo.replicaSetUrl("settled_frontier_store");
        try (MongoClient sourceClient = MongoClients.create(sourceUri);
                MongoClient targetClient = MongoClients.create(targetUri);
                ServerHandle server = Tiers.IN_PROCESS.launch(storeUri, "settled_frontier_state")) {
            MongoCollection<Document> source = sourceClient.getDatabase(SOURCE_DB).getCollection("orders");
            MongoCollection<Document> target = targetClient.getDatabase(TARGET_DB).getCollection("orders");
            source.insertMany(List.of(row("one", "first"), row("two", "second")));

            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            Map<String, String> resources = new LinkedHashMap<>();
            resources.put("source.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: src_mongo
                    connector: mongodb
                    config: { uri: "%s", database: %s }
                    mode: cdc
                    tables: [ orders ]
                    """.formatted(sourceUri, SOURCE_DB));
            resources.put("target.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: tgt_mongo
                    connector: mongodb
                    config: { uri: "%s", database: %s }
                    """.formatted(targetUri, TARGET_DB));
            resources.put("pipeline.tap.yml", """
                    version: tapstate/v1
                    kind: pipeline
                    id: %s
                    source: src_mongo
                    settings: { read_mode: snapshot_and_cdc }
                    serve:
                      from: orders
                      sync: [ { source: tgt_mongo, write_mode: upsert } ]
                    """.formatted(PIPELINE));
            control.apply(resources);
            control.discoverSchema("src_mongo", "mongodb", Map.of("uri", sourceUri, "database", SOURCE_DB));
            control.lifecycle(PIPELINE, LifecycleVerb.START);

            reconcile(source, target);
            Await.until("both snapshot rows to be observed", TIMEOUT,
                    () -> control.snapshotRowsRead(PIPELINE).getOrDefault("orders", -1L) == 2L,
                    () -> control.snapshotRowsRead(PIPELINE).toString());
            Await.until("the snapshot to be durably landed", TIMEOUT,
                    () -> control.snapshotTable(PIPELINE, "orders").filter(progress -> progress.landed()).isPresent(),
                    () -> control.snapshotTable(PIPELINE, "orders").toString());
            assertThat(control.metricsNamed(PIPELINE, "frontierGap").getOrDefault("frontierGap.orders", 0L))
                    .as("the settled snapshot starts without a frontier gap")
                    .isZero();
            source.insertOne(row("three", "third"));
            reconcile(source, target);
            source.updateOne(new Document("_id", "one"), new Document("$set", new Document("name", "updated")));
            reconcile(source, target);
            source.deleteOne(new Document("_id", "two"));
            reconcile(source, target);
            Await.until("all five writes to be observed", TIMEOUT,
                    () -> control.recordsOut(PIPELINE).filter(count -> count == 5L).isPresent(),
                    () -> control.metricsNamed(PIPELINE, "").toString());

            // Allow metrics collection and publication to catch up with the independent database read.
            // Every sample must be newly published, so retained observations cannot witness a stall.
            Instant after = Instant.now().plusSeconds(10);
            Await.until("an observation after the target settled", TIMEOUT,
                    () -> control.observedAt(PIPELINE).filter(time -> time.isAfter(after)).isPresent(),
                    () -> control.observedAt(PIPELINE).toString());
            List<Map<String, Long>> samples = new ArrayList<>();
            for (int sample = 0; sample < 3; sample++) {
                Instant observed = control.observedAt(PIPELINE).orElseThrow();
                samples.add(control.metricsNamed(PIPELINE, "frontier"));
                assertThat(control.state(PIPELINE)).contains(PipelineState.RUNNING);
                assertThat(control.errorCount(PIPELINE)).contains(0L);
                assertThat(rows(target)).containsExactlyInAnyOrderElementsOf(rows(source));
                if (sample < 2) {
                    // A new observation can still contain the previous five-second metrics collection.
                    Instant nextCollection = observed.plusSeconds(6);
                    Await.until("the next fresh observation", TIMEOUT,
                            () -> control.observedAt(PIPELINE).filter(time -> time.isAfter(nextCollection)).isPresent(),
                            () -> control.observedAt(PIPELINE).toString());
                }
            }
            assertThat(samples)
                    .as("fresh frontier readings after all five writes landed: %s", samples)
                    .allSatisfy(metrics -> {
                        assertThat(metrics.getOrDefault("frontierGap.orders", 0L))
                                .as("a reconciled quiet frontier has no unexplained bound distance")
                                .isZero();
                        assertThat(metrics.getOrDefault("frontierStalledMillis.orders", 0L))
                                .as("a fully reconciled quiet pipeline is not pinned")
                                .isZero();
                    });
        }
    }

    private static Document row(String id, String name) {
        return new Document("_id", id).append("name", name);
    }

    private static List<Document> rows(MongoCollection<Document> collection) {
        return collection.find().sort(new Document("_id", 1)).into(new ArrayList<>());
    }

    private static void reconcile(MongoCollection<Document> source, MongoCollection<Document> target) {
        Await.until("source and target contents to reconcile", TIMEOUT,
                () -> rows(source).equals(rows(target)),
                () -> "source=" + rows(source) + ", target=" + rows(target));
    }
}
