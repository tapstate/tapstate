package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** A server restart over a MongoDB NEST pipeline whose quiet table saved an old epoch boundary. */
class AQuietNestTableResumesAfterRestartIT {
    private static final String PIPELINE = "quiet_nest";
    private static final String SOURCE = "quiet_nest_source";
    private static final String VIEW = "quiet_nest_orders";
    private static final String QUIET = "items";
    private static final long OLD_BOUNDARY_SEQUENCE = 140737488355326L;
    private static final Duration WAIT = Duration.ofSeconds(60);

    @BeforeAll
    static void prerequisites() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @Test
    void aQuietTablesFirstChangeReachesTheNestAfterServerRestart(@TempDir Path plugins) throws Exception {
        String uri = SharedMongo.replicaSetUrl("quiet_nest_input");
        var input = EndpointAddress.uri(uri);
        try (var storage = PreparedNativeStore.create(); var mongo = new MongoEndpoints();
                var documents = new NativeSrsDocuments(storage)) {
            var target = EndpointAddress.uri(storage.masterUri());
            String chain;
            String consumer;
            try (var server = Tiers.IN_PROCESS.launch(storage, plugins)) {
                var control = new ControlPlane(server.baseUrl());
                storage.loginAndAwaitBusiness(control);
                mongo.seed(input, "orders", List.of(Map.of("id", 1, "customer", "seed")));
                mongo.seed(input, QUIET, List.of(Map.of("id", 1, "order_id", 1, "sku", "snapshot")));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                var resources = workspace(uri);
                control.apply(Map.of(SOURCE + ".tap.yml", resources.get(SOURCE + ".tap.yml")));
                control.discoverSchema(SOURCE, "mongodb", Map.of("isUri", true, "uri", uri));
                control.apply(resources);
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                Await.until("both NEST snapshots to land", WAIT, () -> {
                    var snapshots = control.snapshotTables(PIPELINE);
                    return snapshots.size() == 2 && snapshots.values().stream().allMatch(TableSnapshot::landed)
                            && items(mongo, target).equals(List.of("snapshot"));
                }, () -> control.failure(PIPELINE) + " " + mongo.documents(target, VIEW));
                var key = documents.pipelineMembership(PIPELINE, SOURCE).get("_id", Document.class);
                chain = key.getString("chain");
                consumer = key.getString("consumer");
            }

            assertThat(documents.largestSequence(chain, QUIET)).isEqualTo(-1L);
            assertThat(documents.consumerReadSeq(chain, consumer, QUIET)).isEqualTo(-1L);
            restoreOldEpochBoundary(storage, documents.consumerOffset(chain, consumer));
            try (var server = Tiers.IN_PROCESS.launch(storage, plugins)) {
                var control = new ControlPlane(server.baseUrl());
                storage.loginAndAwaitBusiness(control);
                Await.until("the restarted NEST to resume or report its failure", WAIT,
                        () -> control.state(PIPELINE).filter(state -> state == PipelineState.RUNNING
                                || state == PipelineState.FAILED).isPresent(), () -> control.failure(PIPELINE).toString());
                assertThat(control.state(PIPELINE)).as("recovery outcome: %s", control.failure(PIPELINE))
                        .contains(PipelineState.RUNNING);
                assertThat(items(mongo, target)).containsExactly("snapshot");
                mongo.insert(input, QUIET, List.of(Map.of("id", 2, "order_id", 1, "sku", "first-cdc")));
                Await.until("the quiet table's first CDC change to reach the existing document", WAIT,
                        () -> items(mongo, target).contains("first-cdc"),
                        () -> control.failure(PIPELINE) + " " + mongo.documents(target, VIEW));
                assertThat(items(mongo, target)).containsExactlyInAnyOrder("snapshot", "first-cdc");
                assertThat(control.state(PIPELINE)).contains(PipelineState.RUNNING);
            }
        }
    }

    private static void restoreOldEpochBoundary(PreparedNativeStore storage, Document row) {
        // Replay the previous writer's version-one BSON output after the original server has stopped.
        var progress = row.get("progress", Document.class);
        long epoch = progress.getLong("snapshotEpoch");
        var boundary = new Document("epoch", epoch).append("sequence", OLD_BOUNDARY_SEQUENCE);
        tableEntry(progress, "sinkAckedByTable").put("position", new Document(boundary).append("token", null));
        tableEntry(row, "ringDoneThrough").put("sequence", OLD_BOUNDARY_SEQUENCE);
        var writers = tableEntry(row.get("writerRun", Document.class), "progress").getList("writers", Document.class);
        for (var writer : writers) writer.put("durableThrough", boundary);
        row.remove("contentHash");
        row.put("contentHash", CanonicalHash.ofText(row.toJson()));
        try (var client = MongoClients.create(storage.cacheUri())) {
            assertThat(SystemCollections.CACHE_SRS_CONSUMER_OFFSETS.on(client.getDatabase(storage.cacheDatabase()))
                    .replaceOne(new Document("_id", row.get("_id")), row).getMatchedCount()).isOne();
        }
    }

    private static Document tableEntry(Document row, String field) {
        return row.getList(field, Document.class).stream().filter(entry -> QUIET.equals(entry.getString("table")))
                .findFirst().orElseThrow();
    }

    private static List<String> items(MongoEndpoints mongo, EndpointAddress target) {
        var rows = mongo.documents(target, VIEW);
        if (rows.isEmpty()) return List.of();
        assertThat(rows).hasSize(1);
        return rows.getFirst().getList("items", Document.class, List.of()).stream().map(item -> item.getString("sku")).toList();
    }

    private static Map<String, String> workspace(String uri) {
        return Map.of(SOURCE + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: quiet_nest_source
                connector: mongodb
                config: { isUri: true, uri: "%s" }
                mode: cdc
                tables: [ orders, items ]
                """.formatted(uri), PIPELINE + ".tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: quiet_nest
                source: [ { id: quiet_nest_source, srs: true } ]
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: order_doc
                    type: nest
                    from: { o: orders, i: items }
                    root:
                      from: o
                      key: [ id ]
                      embed:
                        - { from: i, on: { order_id: id }, as: array, path: items, arrayKey: [ id ] }
                view:
                  id: quiet_nest_orders
                  from: order_doc
                  primary_key: id
                """);
    }
}
