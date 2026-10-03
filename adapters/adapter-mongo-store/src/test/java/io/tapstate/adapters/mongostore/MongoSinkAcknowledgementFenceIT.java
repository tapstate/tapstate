package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
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

/** All durable sink effects obey both the live claim and the consumer's configured run identity. */
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
    void theCurrentRunCanAdvanceDirectAcknowledgementsAndCompleteSnapshots() {
        withStore(fixture -> {
            var store = fixture.store();
            var fence = fixture.fence();
            store.configureSinkWriters(CHAIN, PIPELINE, PLAN, fence);
            store.advanceSinkAcked(CHAIN, PIPELINE, position(1), fence);
            assertThat(store.read(CHAIN).orElseThrow().consumerOffset(PIPELINE).orElseThrow().sinkAcked())
                    .isEqualTo(position(1));
            store.advanceSinkAcked(CHAIN, PIPELINE, TABLE, position(4), fence);
            store.advanceSinkAcked(CHAIN, PIPELINE, TABLE, position(2), fence);
            assertThat(store.ringDoneThrough(CHAIN, PIPELINE)).containsEntry(TABLE, 4L);
            store.advanceSinkAcked(CHAIN, PIPELINE, TABLE, position(-1), fence);
            assertThat(store.ringDoneThrough(CHAIN, PIPELINE)).containsEntry(TABLE, 4L);
            store.markSnapshotComplete(CHAIN, PIPELINE, TABLE, fence);
            store.markSnapshotComplete(CHAIN, PIPELINE, TABLE, fence);
            assertThat(store.read(CHAIN).orElseThrow().snapshotCompletedTables(PIPELINE))
                    .containsExactly(TABLE);
        });
    }

    @Test
    void writerSnapshotCompletionRequiresEveryConfiguredWriter() {
        withStore(fixture -> {
            var store = fixture.store();
            var fence = fixture.fence();
            store.configureSinkWriters(CHAIN, PIPELINE, Map.of(TABLE, List.of(WRITER, "writer-2")), fence);
            store.advanceSinkWriterAcked(CHAIN, PIPELINE, WRITER, TABLE, position(2), fence);
            store.markSinkWriterSnapshotComplete(CHAIN, PIPELINE, WRITER, TABLE, fence);
            assertThat(store.read(CHAIN).orElseThrow().snapshotCompletedTables(PIPELINE)).isEmpty();
            store.advanceSinkWriterAcked(CHAIN, PIPELINE, "writer-2", TABLE, position(1), fence);
            store.markSinkWriterSnapshotComplete(CHAIN, PIPELINE, "writer-2", TABLE, fence);
            assertThat(store.read(CHAIN).orElseThrow().snapshotCompletedTables(PIPELINE)).containsExactly(TABLE);
            var consumer = store.read(CHAIN).orElseThrow().consumerOffset(PIPELINE).orElseThrow();
            assertThat(consumer.sinkAckedByTable()).containsEntry(TABLE, position(1));
            assertThat(consumer.sinkAcked()).as("a writer's word never moves the chain-level position").isNull();
            assertThat(store.ringDoneThrough(CHAIN, PIPELINE)).containsEntry(TABLE, 1L);
        });
    }

    @Test
    void aLiveClaimWithoutAMatchingConsumerBindingCannotChangeAnySinkProgress() {
        withStore(fixture -> {
            fixture.store().configureSinkWriters(CHAIN, PIPELINE, PLAN);
            assertEffectsLeaveConsumerUnchanged(fixture);
            fixture.store().configureSinkWriters(CHAIN, PIPELINE, PLAN, fixture.fence());
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
            fixture.store().configureSinkWriters(CHAIN, PIPELINE, PLAN, fixture.fence());
            var current = fixture.claims().read(fixture.fence().key()).orElseThrow().claim();
            var next = fixture.claims().advanceExecution(current, 1, Set.of("node-a")).orElseThrow();
            fixture.store().configureSinkWriters(CHAIN, PIPELINE, PLAN, WorkloadClaimFence.from(next));
            Document before = fixture.consumer();
            fixture.store().configureSinkWriters(CHAIN, PIPELINE, PLAN, fixture.fence());
            assertThat(fixture.consumer()).isEqualTo(before);
            assertEffectsLeaveConsumerUnchanged(fixture);
        });
    }

    @Test
    void aDamagedConsumerFenceIsReportedAndNeverReplacedByAnAcknowledgement() {
        withStore(fixture -> {
            fixture.store().configureSinkWriters(CHAIN, PIPELINE, PLAN, fixture.fence());
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
                () -> store.advanceSinkAcked(CHAIN, PIPELINE, position(9), fence),
                () -> store.advanceSinkAcked(CHAIN, PIPELINE, TABLE, position(9), fence),
                () -> store.advanceSinkWriterAcked(CHAIN, PIPELINE, WRITER, TABLE, position(9), fence),
                () -> store.markSnapshotComplete(CHAIN, PIPELINE, TABLE, fence),
                () -> store.markSinkWriterSnapshotComplete(CHAIN, PIPELINE, WRITER, TABLE, fence));
    }

    private static ChainPosition position(long seq) {
        return new ChainPosition(new SourceOrder(1, seq), "w" + seq);
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
                test.accept(new Fixture(store, consumers, claims, WorkloadClaimFence.from(run)));
            } finally {
                database.drop();
            }
        }
    }

    private record Fixture(MongoSrsMetaStore store, MongoCollection<Document> consumers,
                           MongoWorkloadClaimStore claims, WorkloadClaimFence fence) {
        Document consumer() {
            return consumers.find(new Document("pipelineId", PIPELINE)).first();
        }
    }
}
