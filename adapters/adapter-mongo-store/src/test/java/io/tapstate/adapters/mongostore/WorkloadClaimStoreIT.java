package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Atomic Mongo-time workload ownership: one winner, monotonic generations, and no TTL deletion. */
@RequiresDocker
class WorkloadClaimStoreIT {

    private static final WorkloadClaimKey KEY =
            new WorkloadClaimKey("cluster-a", WorkloadClaimType.NODE_SESSION, "node-a");
    private static final Duration TTL = Duration.ofSeconds(30);

    @Container
    private static final MongoDBContainer REPLICA_SET =
            new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test
    void concurrentBootsForOneStableNodeProduceOneClaimHolder() throws Exception {
        withStore((store, collection) -> {
            CountDownLatch start = new CountDownLatch(1);
            try (var workers = Executors.newFixedThreadPool(2)) {
                Future<WorkloadClaimAttempt> one = workers.submit(() -> {
                    start.await();
                    return store.acquire(KEY, new WorkloadOwner("node-a", "boot-1"), 0, TTL);
                });
                Future<WorkloadClaimAttempt> two = workers.submit(() -> {
                    start.await();
                    return store.acquire(KEY, new WorkloadOwner("node-a", "boot-2"), 0, TTL);
                });
                start.countDown();

                List<WorkloadClaimAttempt> attempts = List.of(one.get(), two.get());
                assertThat(attempts).filteredOn(WorkloadClaimAttempt::acquired).hasSize(1);
                assertThat(attempts).filteredOn(attempt -> !attempt.acquired()).hasSize(1);
                assertThat(attempts.get(0).claim()).isEqualTo(attempts.get(1).claim());
                assertThat(attempts.get(0).claim().claimGeneration()).isEqualTo(1);
                assertThat(collection.countDocuments()).isEqualTo(1);
            }
        });
    }

    @Test
    void releaseAndReacquireAdvanceGenerationWithoutDeletingTheDocument() {
        withStore((store, collection) -> {
            WorkloadClaim first = store.acquire(KEY, new WorkloadOwner("node-a", "boot-1"), 0, TTL).claim();
            assertThat(store.release(first)).isTrue();

            WorkloadClaim second = store.acquire(KEY, new WorkloadOwner("node-a", "boot-2"), 0, TTL).claim();

            assertThat(second.claimGeneration()).isEqualTo(2);
            assertThat(second.owner().bootId()).isEqualTo("boot-2");
            assertThat(collection.countDocuments()).isEqualTo(1);
        });
    }

    @Test
    void oneExpectedExecutionGenerationCanAdvanceOnlyOnce() {
        withStore((store, collection) -> {
            WorkloadClaim claim = store.acquire(
                    new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders"),
                    new WorkloadOwner("node-a", "boot-1"), 7, TTL).claim();

            WorkloadClaim advanced = store.advanceExecution(claim, 7, Set.of("node-a", "node-b")).orElseThrow();

            assertThat(advanced.claimGeneration()).isEqualTo(claim.claimGeneration());
            assertThat(advanced.executionGeneration()).isEqualTo(1);
            assertThat(advanced.contextExecutionGeneration()).isEqualTo(advanced.executionGeneration());
            assertThat(advanced.executionClaimGeneration()).isEqualTo(claim.claimGeneration());
            assertThat(advanced.executionNodeIds()).containsExactlyInAnyOrder("node-a", "node-b");
            assertThat(store.advanceExecution(claim, 7, Set.of("node-a", "node-b"))).isEmpty();
        });
    }

    @Test
    void standaloneAndClusterAdvanceTheSameDocumentAcrossStoreInstancesAndOwnershipChanges() {
        withStore((store, collection) -> {
            WorkloadClaimKey pipeline =
                    new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
            assertThat(store.advanceStandalone("cluster-a", "orders")).hasValue(1);
            assertThat(store.advanceStandalone("cluster-a", "orders")).hasValue(2);
            assertThat(store.currentGeneration("cluster-a", "orders")).hasValue(2);
            assertThat(store.read(pipeline)).as("standalone allocation creates no claim or lease").isEmpty();
            assertThat(store.readAll(List.of(pipeline))).isEmpty();
            assertThat(collection.countDocuments()).isEqualTo(1);

            MongoWorkloadClaimStore reopened = new MongoWorkloadClaimStore(collection);
            assertThat(reopened.advanceStandalone("cluster-a", "orders"))
                    .as("reopening the adapter must not reset the sequence").hasValue(3);

            WorkloadClaim first = reopened.acquire(pipeline, new WorkloadOwner("node-a", "boot-1"), 7, TTL)
                    .claim();
            assertThat(first.executionGeneration()).isEqualTo(3);
            WorkloadClaim fourth = reopened.advanceUnderClaim(first, 7).orElseThrow();
            assertThat(fourth.executionGeneration()).isEqualTo(4);
            assertThat(fourth.contextExecutionGeneration())
                    .as("a legacy advance supplied no factual planned members").isLessThan(fourth.executionGeneration());
            assertThat(reopened.recordExecutionFailure(fourth, false)).isEmpty();
            assertThat(reopened.currentGeneration("cluster-a", "orders")).hasValue(4);
            assertThat(reopened.advanceUnderClaim(first, 7)).as("stale expected generation is refused").isEmpty();
            assertThat(reopened.advanceStandalone("cluster-a", "orders"))
                    .as("a live cluster claim blocks a standalone start").isEmpty();
            assertThat(reopened.read(pipeline).orElseThrow().claim().executionGeneration()).isEqualTo(4);

            assertThat(reopened.release(fourth)).isTrue();
            assertThat(reopened.advanceStandalone("cluster-a", "orders"))
                    .as("standalone may continue after the previous lease has ended").hasValue(5);
            assertThat(reopened.currentGeneration("cluster-a", "orders")).hasValue(5);
            WorkloadClaim successor = reopened.acquire(
                    pipeline, new WorkloadOwner("node-b", "boot-2"), 8, TTL).claim();
            assertThat(successor.claimGeneration()).isEqualTo(2);
            assertThat(successor.executionGeneration()).isEqualTo(5);
            assertThat(reopened.advanceUnderClaim(fourth, 7)).as("the old owner cannot allocate").isEmpty();
            assertThat(reopened.advanceUnderClaim(successor, 8).orElseThrow().executionGeneration())
                    .isEqualTo(6);
            assertThat(collection.countDocuments()).isEqualTo(1);
        });
    }

    @Test
    void aClaimWithNoValidLeaseDateCannotBeTreatedAsExpiredByStandalone() {
        withStore((store, collection) -> {
            Document id = new Document("clusterId", "cluster-a")
                    .append("resourceType", WorkloadClaimType.PIPELINE_ACTUATION.name())
                    .append("resourceId", "orders");
            collection.insertOne(new Document("_id", id)
                    .append("clusterId", "cluster-a")
                    .append("resourceType", WorkloadClaimType.PIPELINE_ACTUATION.name())
                    .append("resourceId", "orders")
                    .append("ownerNodeId", "node-a")
                    .append("ownerBootId", "boot-1")
                    .append("claimGeneration", 1L)
                    .append("executionGeneration", 9L)
                    .append("topologyRevision", 7L));

            assertThat(store.advanceStandalone("cluster-a", "orders")).isEmpty();
            assertThat(collection.find(new Document("_id", id)).first()
                    .getLong("executionGeneration")).isEqualTo(9L);
        });
    }

    @Test
    void executionAndFirstFailureContextSurviveTakeoverAndResetForTheNextRun() {
        withStore((store, collection) -> {
            WorkloadClaimKey pipeline =
                    new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
            WorkloadClaim first = store.acquire(
                    pipeline, new WorkloadOwner("node-a", "boot-1"), 7, TTL).claim();
            WorkloadClaim run = store.advanceExecution(first, 7, Set.of("node-b", "node-a")).orElseThrow();
            WorkloadClaim failed = store.recordExecutionFailure(run, false).orElseThrow();

            var stored = collection.find().first();
            assertThat(stored).isNotNull();
            assertThat(stored.getLong("executionClaimGeneration")).isEqualTo(1L);
            assertThat(stored.getLong("contextExecutionGeneration")).isEqualTo(1L);
            assertThat(stored.getList("executionNodeIds", String.class)).containsExactly("node-a", "node-b");
            assertThat(stored.getLong("failureClaimGeneration")).isEqualTo(1L);
            assertThat(stored.getBoolean("failureAfterMemberLoss")).isFalse();
            assertThat(store.recordExecutionFailure(failed, true).orElseThrow().failureAfterMemberLoss())
                    .as("the first failure observation fixes the order of failure and member loss")
                    .isFalse();

            assertThat(store.release(failed)).isTrue();
            WorkloadClaim inherited = store.acquire(
                    pipeline, new WorkloadOwner("node-b", "boot-2"), 7, TTL).claim();
            assertThat(inherited.claimGeneration()).isEqualTo(2);
            assertThat(inherited.executionGeneration()).isEqualTo(run.executionGeneration());
            assertThat(inherited.contextExecutionGeneration()).isEqualTo(run.executionGeneration());
            assertThat(inherited.executionClaimGeneration()).isEqualTo(1);
            assertThat(inherited.executionNodeIds()).containsExactlyInAnyOrder("node-a", "node-b");
            assertThat(inherited.failureClaimGeneration()).isEqualTo(1);
            assertThat(store.recordExecutionFailure(failed, true)).isEmpty();

            WorkloadClaim nextRun = store.advanceExecution(inherited, 7, Set.of("node-b")).orElseThrow();
            assertThat(nextRun.executionClaimGeneration()).isEqualTo(2);
            assertThat(nextRun.contextExecutionGeneration()).isEqualTo(nextRun.executionGeneration());
            assertThat(nextRun.executionNodeIds()).containsExactly("node-b");
            assertThat(nextRun.failureClaimGeneration()).isZero();
            assertThat(nextRun.failureAfterMemberLoss()).isFalse();

            // An older member can advance the generation without writing the newer context fields.
            // The mismatch must be visible, so a new holder does not reuse the prior run's verdict.
            collection.updateOne(new org.bson.Document("resourceId", "orders"),
                    new org.bson.Document("$inc", new org.bson.Document("executionGeneration", 1L)));
            WorkloadClaim olderWriter = store.read(pipeline).orElseThrow().claim();
            assertThat(olderWriter.contextExecutionGeneration())
                    .isLessThan(olderWriter.executionGeneration());
            assertThat(store.recordExecutionFailure(olderWriter, false)).isEmpty();
        });
    }

    @Test
    void legacyClaimedAdvanceCannotReuseThePriorExecutionsFailureVerdict() {
        withStore((store, collection) -> {
            WorkloadClaimKey pipeline = new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
            WorkloadClaim first = store.acquire(pipeline, new WorkloadOwner("node-a", "boot-1"), 7, TTL).claim();
            WorkloadClaim run = store.advanceExecution(first, 7, Set.of("node-a", "node-b")).orElseThrow();
            WorkloadClaim failed = store.recordExecutionFailure(run, false).orElseThrow();

            WorkloadClaim legacy = store.advanceUnderClaim(failed, 7).orElseThrow();

            assertThat(legacy.executionGeneration()).isEqualTo(2);
            assertThat(legacy.contextExecutionGeneration()).isEqualTo(run.executionGeneration());
            assertThat(legacy.contextExecutionGeneration()).isLessThan(legacy.executionGeneration());
            assertThat(store.recordExecutionFailure(legacy, true)).isEmpty();
            WorkloadClaim next = store.advanceExecution(legacy, 7, Set.of("node-a")).orElseThrow();
            assertThat(next.contextExecutionGeneration()).isEqualTo(next.executionGeneration());
            assertThat(next.executionNodeIds()).containsExactly("node-a");
            assertThat(next.failureClaimGeneration()).isZero();
            assertThat(next.failureAfterMemberLoss()).isFalse();
            assertThat(collection.countDocuments()).isEqualTo(1);
        });
    }

    @Test
    void phasedAdmissionRecordsTheSameContextAndRollsBackEveryFieldOnRefusal() {
        try (PhasedClaimFixture f = new PhasedClaimFixture()) {
            Document prior = f.database.getCollection("workload_claims").find().first();
            assertThatThrownBy(() -> f.state.admitSuccessor(f.pending, "inc-a", "submit-b", Set.of(), f.at))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> f.state.admitSuccessor(f.pending, "inc-a", "b".repeat(5000),
                    Set.of("node-c", "node-a"), f.at))
                    .isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_TOO_LARGE));
            assertThat(f.database.getCollection("workload_claims").find().first()).isEqualTo(prior);
            assertThat(f.state.readStopReservation("orders")).contains(f.pending);

            var admitted = f.state.admitSuccessor(f.pending, "inc-a", "submit-b",
                    Set.of("node-c", "node-a"), f.at).orElseThrow();
            WorkloadClaim next = admitted.advancedClaim().orElseThrow();

            assertThat(next.executionGeneration()).isEqualTo(2);
            assertThat(next.contextExecutionGeneration()).isEqualTo(next.executionGeneration());
            assertThat(next.executionClaimGeneration()).isEqualTo(f.priorRun.claimGeneration());
            assertThat(next.executionNodeIds()).containsExactlyInAnyOrder("node-a", "node-c");
            assertThat(next.failureClaimGeneration()).isZero();
            assertThat(next.failureAfterMemberLoss()).isFalse();
            assertThat(next.leaseUntil()).isEqualTo(f.priorRun.leaseUntil());
            assertThat(f.claims.read(next.key()).orElseThrow().claim()).isEqualTo(next);
            assertThat(admitted.reservation().writerAuthority()).isEqualTo(StopAuthority.claimed(WorkloadClaimFence.from(next)));
            assertThat(admitted.reservation().source()).isEqualTo(f.pending.source());
            assertThat(admitted.reservation().originalDesired()).isEqualTo(f.pending.originalDesired());
            assertThat(f.state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(PipelineState.STOPPED));
        }
    }

    @Test
    void legacyPhasedAdmissionDoesNotInventCurrentExecutionMembers() {
        try (PhasedClaimFixture f = new PhasedClaimFixture()) {
            var admitted = f.state.admitSuccessor(f.pending, "inc-a", "submit-b", f.at).orElseThrow();
            WorkloadClaim legacy = admitted.advancedClaim().orElseThrow();

            assertThat(legacy.executionGeneration()).isEqualTo(2);
            assertThat(legacy.contextExecutionGeneration()).isEqualTo(f.priorRun.executionGeneration());
            assertThat(legacy.contextExecutionGeneration()).isLessThan(legacy.executionGeneration());
            assertThat(legacy.executionNodeIds()).isEqualTo(f.priorRun.executionNodeIds());
            assertThat(f.claims.recordExecutionFailure(legacy, true)).isEmpty();
            assertThat(legacy.leaseUntil()).isEqualTo(f.priorRun.leaseUntil());
            assertThat(admitted.reservation().source()).isEqualTo(f.pending.source());
        }
    }

    @Test
    void aReadAnswersHowMuchOfTheLeaseTheServerItselfSaysIsLeft() {
        withStore((store, collection) -> {
            WorkloadClaim claim = store.acquire(KEY, new WorkloadOwner("node-a", "boot-1"), 0, TTL).claim();

            WorkloadClaimReading fresh = store.read(KEY).orElseThrow();

            assertThat(fresh.claim()).isEqualTo(claim);
            assertThat(fresh.leased()).isTrue();
            assertThat(fresh.leaseRemaining())
                    .as("a lease just handed out has nearly all of itself left, and never more than all")
                    .isLessThanOrEqualTo(TTL)
                    .isGreaterThan(TTL.minusSeconds(10));

            assertThat(store.release(claim)).isTrue();

            WorkloadClaimReading lapsed = store.read(KEY).orElseThrow();

            assertThat(lapsed.claim().claimGeneration()).isEqualTo(claim.claimGeneration());
            assertThat(lapsed.claim().executionGeneration()).isEqualTo(claim.executionGeneration());
            assertThat(lapsed.leased())
                    .as("the record and both generations outlive the lease; only this says nobody owns it")
                    .isFalse();
            assertThat(lapsed.leaseRemaining()).isLessThanOrEqualTo(Duration.ZERO);
        });
    }

    @Test
    void aBatchedReadAnswersEachClaimAsItsOwnReadDoesAndLeavesOutWhatNobodyClaimed() {
        withStore((store, collection) -> {
            WorkloadOwner owner = new WorkloadOwner("node-a", "boot-1");
            WorkloadClaimKey pipeline =
                    new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
            WorkloadClaimKey released = new WorkloadClaimKey("cluster-a", WorkloadClaimType.CAPTURE, "capture-1");
            WorkloadClaimKey unclaimed = new WorkloadClaimKey("cluster-a", WorkloadClaimType.CAPTURE, "capture-2");
            // The same resource in another cluster: a match on part of the id would answer it with this one.
            WorkloadClaimKey elsewhere =
                    new WorkloadClaimKey("cluster-b", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
            store.acquire(KEY, owner, 0, TTL);
            store.acquire(pipeline, owner, 7, TTL);
            assertThat(store.release(store.acquire(released, owner, 7, TTL).claim())).isTrue();

            Map<WorkloadClaimKey, WorkloadClaimReading> batch =
                    store.readAll(List.of(KEY, pipeline, released, unclaimed, elsewhere));

            assertThat(batch)
                    .as("a key nobody claimed is absent, as its own read answers nothing")
                    .containsOnlyKeys(KEY, pipeline, released);
            for (WorkloadClaimKey key : List.of(KEY, pipeline, released)) {
                WorkloadClaimReading alone = store.read(key).orElseThrow();
                assertThat(batch.get(key).claim()).as("%s", key).isEqualTo(alone.claim());
                assertThat(batch.get(key).leased()).as("%s", key).isEqualTo(alone.leased());
            }
            for (WorkloadClaimKey live : List.of(KEY, pipeline)) {
                assertThat(batch.get(live).leaseRemaining())
                        .as("worked out by the server, as a single read is: nearly all of a lease just "
                                + "handed out, and never more than all")
                        .isLessThanOrEqualTo(TTL)
                        .isGreaterThan(TTL.minusSeconds(10));
            }
            assertThat(batch.get(released).leased()).isFalse();
            assertThat(batch.get(released).leaseRemaining()).isLessThanOrEqualTo(Duration.ZERO);
        });
    }

    /**
     * The one that catches a batch arrived at a claim at a time. Nothing about the answer would differ, so
     * this counts what the driver was actually asked to send. A cursor delivering the rest of one answer
     * in further batches is that same aggregation, not a query per claim, so it is not counted as another.
     */
    @Test
    void aBatchedReadIsOneAggregationHoweverManyClaimsItAnswers() {
        List<String> commands = new CopyOnWriteArrayList<>();
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(REPLICA_SET.getReplicaSetUrl()))
                .addCommandListener(new CommandListener() {
                    @Override
                    public void commandStarted(CommandStartedEvent event) {
                        commands.add(event.getCommandName());
                    }
                })
                .build();
        try (MongoClient client = MongoClients.create(settings)) {
            var collection = client.getDatabase("tapstate").getCollection(MongoStorePort.WORKLOAD_CLAIMS);
            collection.drop();
            MongoWorkloadClaimStore store = new MongoWorkloadClaimStore(collection);
            List<WorkloadClaimKey> keys = new ArrayList<>();
            for (int i = 0; i < 200; i++) {
                WorkloadClaimKey key = new WorkloadClaimKey("cluster-a", WorkloadClaimType.CAPTURE, "capture-" + i);
                keys.add(key);
                store.acquire(key, new WorkloadOwner("node-a", "boot-1"), 0, TTL);
            }
            commands.clear();

            assertThat(store.readAll(keys)).hasSize(200);

            assertThat(commands).filteredOn(command -> !command.equals("getMore")).containsExactly("aggregate");

            commands.clear();
            assertThat(store.readAll(List.of())).isEmpty();
            assertThat(commands).as("asking for nothing sends nothing").isEmpty();
        }
    }

    private static void withStore(CheckedBody body) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            var collection = client.getDatabase("tapstate").getCollection(MongoStorePort.WORKLOAD_CLAIMS);
            collection.drop();
            body.run(new MongoWorkloadClaimStore(collection), collection);
        } catch (RuntimeException | Error failure) {
            throw failure;
        } catch (Exception failure) {
            throw new AssertionError(failure);
        }
    }

    private static final class PhasedClaimFixture implements AutoCloseable {
        private final MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl());
        private final com.mongodb.client.MongoDatabase database = client.getDatabase(
                "phased_claim_" + UUID.randomUUID().toString().replace("-", ""));
        private final MongoWorkloadClaimStore claims = new MongoWorkloadClaimStore(database.getCollection("workload_claims"));
        private final MongoDesiredStore desired = new MongoDesiredStore(database.getCollection("desired"));
        private final MongoStateStore state = new MongoStateStore(client, database.getCollection("states"),
                database.getCollection("desired"), database.getCollection("workload_claims"), database.getCollection("artifacts"));
        private final Instant at = Instant.parse("2026-10-02T00:00:00Z");
        private final WorkloadClaim priorRun;
        private final StopReservation pending;

        private PhasedClaimFixture() {
            WorkloadClaimKey key = new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
            WorkloadClaim claimed = claims.acquire(key, new WorkloadOwner("node-a", "boot-1"), 7, TTL).claim();
            priorRun = claims.recordExecutionFailure(
                    claims.advanceExecution(claimed, 7, Set.of("node-a", "node-b")).orElseThrow(), false).orElseThrow();
            database.getCollection("artifacts").insertOne(new Document("_id", "orders").append("kind", "pipeline")
                    .append("pipelineIncarnationId", "inc-a"));
            state.create("orders", StateJson.of(PipelineState.FAILED), at);
            DesiredState intent = new DesiredState("orders", PipelineState.RUNNING, "rev-a", false,
                    "assembly-a", true, 0L);
            desired.save(intent);
            CheckpointDoc actual = state.read("orders").orElseThrow();
            StopReservation proposal = StopReservation.stopping("orders", "phased-claim", actual.epoch(), intent,
                    new StopReservation.Source("cluster-a", new ObservationStore.Scope("inc-a", 1),
                            new StopReservation.JobIdentity("cluster-a", 11, "old-boot")),
                    StopReservation.CounterPolicy.freeze(actual, intent), StopAuthority.claimed(WorkloadClaimFence.from(priorRun)));
            pending = state.markReplacementPending(state.reserveStop(actual, proposal, at).orElseThrow(), at).orElseThrow();
        }

        @Override public void close() { database.drop(); client.close(); }
    }

    @FunctionalInterface
    private interface CheckedBody {
        void run(MongoWorkloadClaimStore store, com.mongodb.client.MongoCollection<org.bson.Document> collection)
                throws Exception;
    }
}
