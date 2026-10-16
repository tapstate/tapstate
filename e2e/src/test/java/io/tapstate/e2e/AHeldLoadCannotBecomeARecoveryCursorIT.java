package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.srs.SrsRingbuffer;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Snapshot progress through a nest cannot invent a ring cursor that a later run cannot recover from. */
class AHeldLoadCannotBecomeARecoveryCursorIT {

    private static final String PIPELINE = "held_load_cursor";
    private static final Duration CONVERGE = Duration.ofMinutes(3);
    // One-row input batches make these separate updates of the same document during its load.
    private static final int CHILDREN = 2;

    @BeforeAll
    static void requireDockerAndMongoConnector() {
        DockerGate.require();
        RealConnectorGate.require("mongodb");
    }

    @Test
    void aLoadedDocumentCanKeepReceivingChildrenAfterItsRunIsReplaced(@TempDir Path temporary) throws Exception {
        Path source = Files.createDirectory(temporary.resolve("source"));
        EndpointAddress address = EndpointAddress.uri(source.toString());
        FileEndpoints files = new FileEndpoints();
        files.seed(address, "orders", List.of(Map.of("id", 1L, "seq", 1L)));
        files.seed(address, "items", LongStream.rangeClosed(1, CHILDREN)
                .mapToObj(id -> Map.<String, Object>of("id", id, "seq", 1L)).toList());
        Holds holds = new Holds(temporary.resolve("holds"));
        holds.read("items");
        String store = SharedMongo.replicaSetUrl("held_load_cursor_store");
        String target = SharedMongo.replicaSetUrl("held_load_cursor_target");
        try (ServerHandle server = Tiers.REAL_PROCESS.launch(store);
                MongoEndpoints mongo = new MongoEndpoints();
                StoreDocuments documents = StoreDocuments.at(store)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID,
                    Files.readAllBytes(E2eConnectorJar.buildInto(temporary)));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            control.apply(workspace(source, holds.directory(), target));
            for (String table : List.of("orders", "items")) {
                control.discoverSchema("src_" + table, E2eConnectorJar.CONNECTOR_ID,
                        Map.of("uri", source.toString(), "hold", holds.directory().toString()));
            }
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            awaitChildren(control, mongo, target, 0);
            holds.releaseRead("items");
            awaitChildren(control, mongo, target, CHILDREN);
            Await.until("both source loads to land", CONVERGE,
                    () -> control.snapshotTables(PIPELINE).size() == 2
                            && control.snapshotTables(PIPELINE).values().stream().allMatch(table -> table.landed()),
                    () -> "snapshot=" + control.snapshotTables(PIPELINE) + ", failure=" + control.failure(PIPELINE));

            assertCursorsBelongToTheirLogs(documents);

            control.stop(PIPELINE, false);
            Await.until("the run to stop while keeping its state", CONVERGE,
                    () -> control.state(PIPELINE).filter(PipelineState.STOPPED::equals).isPresent(),
                    () -> "state=" + control.state(PIPELINE) + ", failure=" + control.failure(PIPELINE));
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            Await.until("the replacement run to start", CONVERGE,
                    () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                    () -> "state=" + control.state(PIPELINE) + ", failure=" + control.failure(PIPELINE));
            files.insert(address, "items", List.of(Map.of("id", CHILDREN + 1L, "seq", 1L)));
            awaitChildren(control, mongo, target, CHILDREN + 1);
            String consumer = SrsConsumerId.of(PIPELINE, "src_items").value();
            String chain = documents.miningChainIds().stream()
                    .filter(id -> documents.consumerOffset(id, consumer) != null).findFirst().orElseThrow();
            Document log = documents.logBounds(SrsRingbuffer.ringName(chain, "items"));
            assertThat(log).as("the new child was captured in the retained log").isNotNull();
            long producedThrough = log.get("largestSequence", Number.class).longValue();
            Await.until("the new child's ring sequence to be confirmed", CONVERGE,
                    () -> {
                        Document done = documents.consumerOffset(chain, consumer).get("perTableRingDone", Document.class);
                        return done != null && done.get("items") instanceof Number cursor
                                && cursor.longValue() >= producedThrough;
                    },
                    () -> "consumer=" + documents.consumerOffset(chain, consumer)
                            + ", log=" + documents.logBounds(SrsRingbuffer.ringName(chain, "items"))
                            + ", state=" + control.state(PIPELINE) + ", failure=" + control.failure(PIPELINE));
            assertCursorsBelongToTheirLogs(documents);
        } finally {
            holds.releaseRead("items");
        }
    }

    private static void assertCursorsBelongToTheirLogs(StoreDocuments documents) {
        for (String table : List.of("orders", "items")) {
            String consumer = SrsConsumerId.of(PIPELINE, "src_" + table).value();
            List<String> chains = documents.miningChainIds().stream()
                    .filter(chain -> documents.consumerOffset(chain, consumer) != null).toList();
            assertThat(chains).as("the source has durable consumer accounting").hasSize(1);
            String chain = chains.getFirst();
            Document offset = documents.consumerOffset(chain, consumer);
            Document done = offset.get("perTableRingDone", Document.class);
            Document log = documents.logBounds(SrsRingbuffer.ringName(chain, table));
            long tail = log == null ? -1 : log.get("largestSequence", Number.class).longValue();
            if (done != null && done.get(table) instanceof Number cursor) {
                assertThat(cursor.longValue())
                        .as("the recovery cursor belongs to retained history: consumer=%s, log=%s", offset, log)
                        .isLessThanOrEqualTo(tail);
            }
        }
    }

    private static void awaitChildren(ControlPlane control, MongoEndpoints mongo, String target, int count) {
        Await.until("the document to carry " + count + " children", CONVERGE, () -> {
            List<Document> rows = mongo.documents(EndpointAddress.uri(target), "orders");
            return rows.size() == 1 && rows.getFirst().getList("items", Document.class, List.of()).size() == count;
        }, () -> "state=" + control.state(PIPELINE) + ", failure=" + control.failure(PIPELINE));
    }

    private static Map<String, String> workspace(Path source, Path holds, String target) {
        Map<String, String> resources = new LinkedHashMap<>();
        for (String table : List.of("orders", "items")) {
            resources.put("src_" + table + ".tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: src_%s
                    connector: %s
                    config: { uri: "%s", hold: "%s" }
                    mode: cdc
                    tables: [ %s ]
                    """.formatted(table, E2eConnectorJar.CONNECTOR_ID, source, holds, table));
        }
        resources.put("target.tap.yml", """
                version: tapstate/v1
                kind: source
                id: held_load_target
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(target));
        resources.put("pipeline.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: [ src_orders, src_items ]
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: order_doc
                    type: nest
                    execution: { batch: { max_records: 1 } }
                    from: { o: orders, i: items }
                    root:
                      from: o
                      key: [ id ]
                      embed:
                        - { from: i, on: { seq: id }, as: array, path: items, arrayKey: [ id ] }
                serve:
                  from: order_doc
                  sync:
                    - { source: held_load_target, execution: { parallelism: 3 } }
                """.formatted(PIPELINE));
        return resources;
    }
}
