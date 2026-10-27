package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A real clustered pipeline binds durable sink progress to its submitted run, then rebinds it on restart.
 * The specification vocabulary cannot inspect the run identity stored beside a consumer checkpoint.
 * The separate store race witness covers a late acknowledgement; this case covers the production wiring
 * from the HTTP start verb through the distributed sink to that durable fence, without calling app seams.
 */
class ASinkCheckpointBelongsToTheRunThatWroteItIT {

    private static final String PIPELINE = "fenced_sink_pipe";
    private static final String SOURCE = "fenced_sink_src";
    private static final String TARGET = "fenced_sink_tgt";
    private static final String TABLE = "orders";
    private static final Duration DELIVERY = Duration.ofMinutes(2);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void restartingAPipelineRebindsItsDurableCheckpointToTheReplacementRun(@TempDir Path directory)
            throws Exception {
        Path source = Files.createDirectories(directory.resolve("src"));
        Path target = Files.createDirectories(directory.resolve("tgt"));
        byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(directory));

        try (FileEndpoints files = new FileEndpoints();
                PreparedNativeStore storage = PreparedNativeStore.create();
                NativeSrsDocuments documents = new NativeSrsDocuments(storage);
                MongoClient reader = MongoClients.create(storage.cacheUri());
                TwoMemberCluster cluster = TwoMemberCluster.startPrepared(storage)) {
            MongoDatabase database = reader.getDatabase(storage.cacheDatabase());
            EndpointAddress sourceAddress = EndpointAddress.uri(source.toString());
            EndpointAddress targetAddress = EndpointAddress.uri(target.toString());
            files.seed(sourceAddress, TABLE, SeedRows.generated(3));
            cluster.awaitBothMembers();
            ControlPlane control = cluster.first();
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
            control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            Map<String, String> resources = new LinkedHashMap<>();
            resources.put(SOURCE + ".tap.yml", Workspaces.cdcSourceYaml(SOURCE, source));
            resources.put(TARGET + ".tap.yml", Workspaces.targetYaml(TARGET, target));
            resources.put(PIPELINE + ".tap.yml", Workspaces.pipelineYaml(PIPELINE, SOURCE, TARGET, TABLE));
            control.apply(resources);
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            awaitState(control, PipelineState.RUNNING);
            Document firstCursor = awaitCheckpoint(documents, files, targetAddress, 3, null, -1, null);
            Document firstFence = assertFenceMatchesCurrentRun(documents, database, control.clusterId(), firstCursor);
            String chain = firstCursor.get("_id", Document.class).getString("chain");
            control.stop(PIPELINE, false);
            awaitState(control, PipelineState.STOPPED);
            long retainedEnd = documents.largestSequence(chain, TABLE);
            files.insert(sourceAddress, TABLE, SeedRows.generated(4).subList(3, 4));
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            awaitState(control, PipelineState.RUNNING);
            Document replacementCursor = awaitCheckpoint(documents, files, targetAddress, 4, chain, retainedEnd, firstFence);
            Document replacementFence = assertFenceMatchesCurrentRun(documents, database, control.clusterId(), replacementCursor);
            assertThat(replacementFence.getLong("executionGeneration"))
                    .as("a replacement execution cannot retain the old run's durable sink fence")
                    .isGreaterThan(firstFence.getLong("executionGeneration"));
        }
    }

    private static void awaitState(ControlPlane control, PipelineState state) {
        Await.until(PIPELINE + " to reach " + state, DELIVERY,
                () -> control.state(PIPELINE).filter(state::equals).isPresent(),
                () -> String.valueOf(control.state(PIPELINE)));
    }

    private static Document awaitCheckpoint(NativeSrsDocuments documents, FileEndpoints files,
            EndpointAddress target, long expectedRows, String originalChain, long priorRingEnd, Document priorFence) {
        var boundary = new AtomicReference<AckBoundary>();
        var proof = new AtomicReference<Document>();
        Await.until("the rows and current writers' durable checkpoint to land", DELIVERY, () -> {
            if (files.count(target, TABLE) != expectedRows) return false;
            Document cursor = consumer(documents);
            if (cursor == null || writerFence(cursor) == null
                    || cursor.get("progress", Document.class).get("sinkAcked") == null
                    || priorFence != null && priorFence.equals(writerFence(cursor))) return false;
            String chain = cursor.get("_id", Document.class).getString("chain");
            if (originalChain != null && !originalChain.equals(chain)) return false;
            if (boundary.get() == null) {
                long epoch = documents.chain(chain).getLong("epoch");
                long sequence = documents.largestSequence(chain, TABLE);
                if (sequence <= priorRingEnd || documents.chain(chain).getLong("epoch") != epoch) return false;
                boundary.set(new AckBoundary(chain, epoch, sequence));
            }
            if (!currentWritersConfirmed(cursor, boundary.get())) return false;
            proof.set(cursor);
            return true;
        }, () -> "target rows=" + files.count(target, TABLE) + ", boundary=" + boundary.get()
                + ", consumer=" + consumer(documents));
        return proof.get();
    }

    private static Document consumer(NativeSrsDocuments documents) {
        Document member = documents.pipelineMembership(PIPELINE, SOURCE);
        if (member == null) return null;
        String chain = member.get("_id", Document.class).getString("chain");
        Document cursor = documents.consumerOffset(chain, SrsConsumerId.of(PIPELINE, SOURCE).value());
        if (cursor == null || !member.get("stateGeneration").equals(cursor.get("stateGeneration"))
                || !member.get("revision").equals(cursor.get("membershipRevision"))) return null;
        return cursor;
    }

    private static Document writerFence(Document consumer) {
        Document run = consumer.get("writerRun", Document.class);
        return run == null ? null : run.get("commandFence", Document.class);
    }

    private record AckBoundary(String chain, long epoch, long sequence) {}

    /** A new roster is insufficient: every current writer must durably cover the fixed new row boundary. */
    private static boolean currentWritersConfirmed(Document cursor, AckBoundary boundary) {
        if (!boundary.chain().equals(cursor.get("_id", Document.class).getString("chain"))) return false;
        Document run = cursor.get("writerRun", Document.class);
        List<String> expected = run.getList("expected", Document.class).stream()
                .filter(table -> TABLE.equals(table.getString("table")))
                .flatMap(table -> table.getList("writers", String.class).stream()).toList();
        List<Document> progress = run.getList("progress", Document.class).stream()
                .filter(table -> TABLE.equals(table.getString("table")))
                .flatMap(table -> table.getList("writers", Document.class).stream()).toList();
        if (expected.isEmpty() || progress.size() != expected.size()
                || !new java.util.HashSet<>(expected).equals(progress.stream()
                        .map(writer -> writer.getString("writer")).collect(java.util.stream.Collectors.toSet()))) return false;
        for (Document writer : progress) {
            Document through = writer.get("durableThrough", Document.class);
            if (through == null || through.getLong("epoch") != boundary.epoch()
                    || through.getLong("sequence") < boundary.sequence()) return false;
        }
        return cursor.getList("ringDoneThrough", Document.class).stream()
                .anyMatch(table -> TABLE.equals(table.getString("table"))
                        && table.getLong("sequence") >= boundary.sequence());
    }

    private static Document assertFenceMatchesCurrentRun(NativeSrsDocuments documents, MongoDatabase database, String clusterId, Document consumer) {
        assertThat(consumer).isNotNull();
        Document fence = writerFence(consumer);
        assertThat(fence).as("the sink's actual durable checkpoint carries its admitted run identity").isNotNull();
        Document claim = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database)
                .find(new Document("_id.clusterId", clusterId)
                        .append("_id.resourceType", WorkloadClaimType.PIPELINE_ACTUATION.name())
                        .append("_id.resourceId", PIPELINE)).first();
        assertThat(claim).isNotNull();
        var scope = documents.scope();
        assertThat(claim.get("pipelineScope", Document.class))
                .isEqualTo(new Document("clusterId", scope.clusterId()).append("bindingVersion", scope.bindingVersion())
                        .append("cacheIncarnation", scope.cacheIncarnation()).append("pipelineId", PIPELINE)
                        .append("stateGeneration", consumer.getLong("stateGeneration")));
        assertThat(fence).containsEntry("clusterId", clusterId)
                .containsEntry("resourceType", WorkloadClaimType.PIPELINE_ACTUATION.name())
                .containsEntry("resourceId", PIPELINE);
        for (String field : new String[] {"ownerNodeId", "ownerBootId", "claimGeneration",
                "executionGeneration", "topologyRevision"}) {
            assertThat(fence).containsEntry(field, claim.get(field));
        }
        return fence;
    }
}
