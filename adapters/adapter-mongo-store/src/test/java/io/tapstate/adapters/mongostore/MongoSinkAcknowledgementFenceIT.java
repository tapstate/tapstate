package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.spi.store.WriterProgress;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Every durable sink effect obeys both the live claim and the run the pipeline's cursor is bound to. */
@RequiresDocker
class MongoSinkAcknowledgementFenceIT {

    private static final String CHAIN = "orders@source";
    private static final String PIPELINE = "orders_pipeline";
    private static final String TABLE = "orders";
    private static final String WRITER = "writer-1";
    private static final Map<String, List<String>> PLAN = Map.of(TABLE, List.of(WRITER));

    @Container
    private static final MongoDBContainer MONGO =
            new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void theCurrentRunCanAdvanceItsWritersAndThePipelinesRecord() {
        withStore(fixture -> {
            var store = fixture.store();
            var fence = fixture.fence();
            assertThat(store.beginWriterRun(CHAIN, PIPELINE, fixture.runId(), PLAN, fence)).isTrue();

            assertThat(store.advanceWriter(CHAIN, PIPELINE, fixture.runId(), WRITER, TABLE, progress(3), fence))
                    .hasValueSatisfying(run -> assertThat(run.progressFor(TABLE)).containsEntry(WRITER, progress(3)));
            assertThat(store.advanceSinkAcked(CHAIN, PIPELINE, TABLE, position(4), fence)).isTrue();
            assertThat(store.advanceSinkAcked(CHAIN, PIPELINE, TABLE, position(2), fence)).isTrue();
            assertThat(ackedBy(store)).isEqualTo(position(4));
            assertThat(store.ringDoneThrough(CHAIN, PIPELINE)).containsEntry(TABLE, 4L);
            assertThat(store.advanceRingDone(CHAIN, PIPELINE, TABLE, 6, fence)).isTrue();
            assertThat(store.ringDoneThrough(CHAIN, PIPELINE)).containsEntry(TABLE, 6L);
            assertThat(store.markSnapshotComplete(CHAIN, PIPELINE, TABLE, fence)).isTrue();
            assertThat(store.markSnapshotComplete(CHAIN, PIPELINE, TABLE, fence)).isTrue();
            assertThat(store.read(CHAIN).orElseThrow().snapshotCompletedTables(PIPELINE)).containsExactly(TABLE);
        });
    }

    /**
     * A member joining has the holder take the same claim again, with the same generations, under the new
     * topology. The run it carries goes on, so its fenced effects land, and the binding records the claim as it
     * now stands.
     */
    @Test
    void theSameRunGoesOnAfterItsHolderTakesTheClaimAgainUnderANewTopology() {
        withStore(fixture -> {
            var store = fixture.store();
            assertThat(store.beginWriterRun(CHAIN, PIPELINE, fixture.runId(), PLAN, fixture.fence())).isTrue();
            WorkloadClaim again = fixture.claims().acquire(fixture.fence().key(), fixture.fence().owner(), 2,
                    Duration.ofMinutes(1)).claim();
            WorkloadClaimFence retaken = WorkloadClaimFence.from(again);
            assertThat(retaken.executionGeneration()).isEqualTo(fixture.fence().executionGeneration());

            assertThat(store.advanceSinkAcked(CHAIN, PIPELINE, TABLE, position(5), retaken)).isTrue();

            assertThat(ackedBy(store)).isEqualTo(position(5));
            assertThat(fixture.consumer().get(MongoSrsMetaStore.SINK_ACK_FENCE, Document.class))
                    .containsEntry("topologyRevision", 2L);
            assertThat(store.advanceSinkAcked(CHAIN, PIPELINE, TABLE, position(6), fixture.fence()))
                    .as("the claim as it stood before is no longer the live one")
                    .isFalse();
        });
    }

    @Test
    void aLiveClaimWithoutAMatchingConsumerBindingCannotChangeAnySinkProgress() {
        withStore(fixture -> {
            fixture.store().beginWriterRun(CHAIN, PIPELINE, fixture.runId(), PLAN);
            assertEffectsLeaveConsumerUnchanged(fixture);
            assertThat(fixture.store().beginWriterRun(CHAIN, PIPELINE, fixture.runId(), PLAN, fixture.fence()))
                    .isTrue();
            Document otherRun = WorkloadClaimDocuments.stored(fixture.fence());
            otherRun.put("executionGeneration", fixture.fence().executionGeneration() + 1);
            fixture.consumers().updateOne(new Document("pipelineId", PIPELINE),
                    new Document("$set", new Document(MongoSrsMetaStore.SINK_ACK_FENCE, otherRun)));
            assertEffectsLeaveConsumerUnchanged(fixture);
        });
    }

    @Test
    void aSupersededClaimCannotRebindOrAdvanceTheConsumer() {
        withStore(fixture -> {
            assertThat(fixture.store().beginWriterRun(CHAIN, PIPELINE, fixture.runId(), PLAN, fixture.fence()))
                    .isTrue();
            var current = fixture.claims().read(fixture.fence().key()).orElseThrow().claim();
            var next = fixture.claims().advanceExecution(current, 1, Set.of("node-a")).orElseThrow();
            assertThat(fixture.store().beginWriterRun(CHAIN, PIPELINE, "g" + next.executionGeneration(), PLAN,
                    WorkloadClaimFence.from(next))).isTrue();
            Document before = fixture.consumer();

            assertThat(fixture.store().beginWriterRun(CHAIN, PIPELINE, fixture.runId(), PLAN, fixture.fence()))
                    .as("a superseded run cannot take its binding back")
                    .isFalse();
            assertThat(fixture.consumer()).isEqualTo(before);
            assertEffectsLeaveConsumerUnchanged(fixture);
        });
    }

    @Test
    void aDamagedConsumerFenceIsReportedAndNeverReplacedByAnAcknowledgement() {
        withStore(fixture -> {
            assertThat(fixture.store().beginWriterRun(CHAIN, PIPELINE, fixture.runId(), PLAN, fixture.fence()))
                    .isTrue();
            fixture.consumers().updateOne(new Document("pipelineId", PIPELINE),
                    new Document("$set", new Document(MongoSrsMetaStore.SINK_ACK_FENCE, "damaged")));
            Document before = fixture.consumer();
            for (Runnable effect : effects(fixture)) {
                assertThatThrownBy(effect::run).isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
                assertThat(fixture.consumer()).isEqualTo(before);
            }
        });
    }

    @Test
    void aSinkFenceMustNameThisPipelineAndASubmittedExecution() {
        WorkloadOwner owner = new WorkloadOwner("node-a", "boot-a");
        for (WorkloadClaimFence fence : List.of(
                new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.CAPTURE, PIPELINE),
                        owner, 1, 1, 1),
                new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "other"),
                        owner, 1, 1, 1),
                new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE),
                        owner, 1, 0, 1))) {
            assertThatThrownBy(() -> MongoSrsMetaStore.sinkAckFenceDocument(PIPELINE, fence))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    private static void assertEffectsLeaveConsumerUnchanged(Fixture fixture) {
        Document before = fixture.consumer();
        for (Runnable effect : effects(fixture)) {
            effect.run();
            assertThat(fixture.consumer()).isEqualTo(before);
        }
    }

    private static List<Runnable> effects(Fixture fixture) {
        var store = fixture.store();
        var fence = fixture.fence();
        return List.of(
                () -> store.advanceWriter(CHAIN, PIPELINE, fixture.runId(), WRITER, TABLE, progress(9), fence),
                () -> store.advanceSinkAcked(CHAIN, PIPELINE, TABLE, position(9), fence),
                () -> store.advanceRingDone(CHAIN, PIPELINE, TABLE, 9, fence),
                () -> store.markSnapshotComplete(CHAIN, PIPELINE, TABLE, fence));
    }

    private static ChainPosition position(long seq) {
        return new ChainPosition(new SourceOrder(1, seq), "w" + seq);
    }

    private static WriterProgress progress(long seq) {
        return new WriterProgress(new SourceOrder(1, seq), position(seq));
    }

    private static ChainPosition ackedBy(MongoSrsMetaStore store) {
        return store.read(CHAIN).orElseThrow().consumerOffset(PIPELINE).orElseThrow().sinkAcked();
    }

    private static void withStore(Consumer<Fixture> test) {
        try (MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = client.getDatabase("sink_fence_" + System.nanoTime());
            var consumers = database.getCollection("srs_consumer_offsets");
            var claims = new MongoWorkloadClaimStore(database.getCollection("workload_claims"));
            var store = new MongoSrsMetaStore(client, database.getCollection("srs_meta"), consumers,
                    database.getCollection("workload_claims"));
            store.create(CHAIN, null);
            var key = new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE);
            var claim = claims.acquire(key, new WorkloadOwner("node-a", "boot-a"), 1,
                    Duration.ofMinutes(1)).claim();
            var run = claims.advanceExecution(claim, 1, Set.of("node-a")).orElseThrow();
            try {
                test.accept(new Fixture(store, consumers, claims, WorkloadClaimFence.from(run),
                        "g" + run.executionGeneration()));
            } finally {
                database.drop();
            }
        }
    }

    private record Fixture(MongoSrsMetaStore store, MongoCollection<Document> consumers,
                           MongoWorkloadClaimStore claims, WorkloadClaimFence fence, String runId) {
        Document consumer() {
            return consumers.find(new Document("pipelineId", PIPELINE)).first();
        }
    }
}
