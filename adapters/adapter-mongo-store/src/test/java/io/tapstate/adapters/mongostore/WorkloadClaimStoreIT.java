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
import io.tapstate.spi.store.ExecutionProfile;
import io.tapstate.spi.store.PendingPipelineResume;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

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
    void theSameBootCannotReuseAReleasedClaimGeneration() {
        withStore((store, collection) -> {
            WorkloadClaimKey pipeline = new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
            WorkloadOwner owner = new WorkloadOwner("node-a", "boot-1");
            WorkloadClaim first = store.acquire(pipeline, owner, 7, TTL).claim();
            WorkloadClaim original = store.advanceExecution(first, 7, Set.of("node-a", "node-b")).orElseThrow();
            assertThat(store.release(original)).isTrue();

            WorkloadClaim current = store.acquire(pipeline, owner, 7, TTL).claim();

            assertThat(current.claimGeneration()).isEqualTo(original.claimGeneration() + 1);
            assertThat(PendingPipelineResume.sameExecutionContext(original, current)).isTrue();
            assertOldGenerationIsRejected(store, collection, original, current);
            assertThat(collection.countDocuments()).isEqualTo(1);
        });
    }

    @Test
    void theSameBootCannotReuseAnExpiredClaimGeneration() {
        withStore((store, collection) -> {
            WorkloadClaimKey pipeline = new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
            WorkloadOwner owner = new WorkloadOwner("node-a", "boot-1");
            WorkloadClaim first = store.acquire(pipeline, owner, 7, Duration.ofSeconds(2)).claim();
            WorkloadClaim original = store.advanceExecution(first, 7, Set.of("node-a", "node-b")).orElseThrow();
            awaitServerExpiry(store, pipeline);

            WorkloadClaim current = store.acquire(pipeline, owner, 7, TTL).claim();

            assertThat(current.claimGeneration()).isEqualTo(original.claimGeneration() + 1);
            assertThat(PendingPipelineResume.sameExecutionContext(original, current)).isTrue();
            assertOldGenerationIsRejected(store, collection, original, current);
            assertThat(collection.countDocuments()).isEqualTo(1);
        });
    }

    @Test
    void aLiveSameBootCanRefreshTopologyWithoutChangingItsGenerationOrExecutionContext() {
        withStore((store, collection) -> {
            WorkloadClaimKey pipeline = new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
            WorkloadOwner owner = new WorkloadOwner("node-a", "boot-1");
            WorkloadClaim first = store.acquire(pipeline, owner, 7, TTL).claim();
            WorkloadClaim original = store.advanceExecution(first, 7, Set.of("node-a", "node-b")).orElseThrow();

            WorkloadClaim refreshed = store.acquire(pipeline, owner, 8, TTL).claim();

            assertThat(refreshed.claimGeneration()).isEqualTo(original.claimGeneration());
            assertThat(refreshed.topologyRevision()).isEqualTo(8);
            assertThat(PendingPipelineResume.sameExecutionContext(original, refreshed)).isTrue();
            assertThat(store.renew(original, TTL)).isEmpty();
            assertThat(store.renew(refreshed, TTL)).isPresent();
            assertThat(store.read(pipeline).orElseThrow().claim().executionGeneration()).isEqualTo(original.executionGeneration());
        });
    }

    @Test
    void profiledSameBootReacquisitionSeparatesLeaseContinuityFromRetiredAuthority() {
        for (boolean release : List.of(false, true)) {
            withProfiledStore((store, collection) -> {
                WorkloadClaimKey pipeline = new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
                WorkloadOwner owner = new WorkloadOwner("node-a", "boot-1");
                WorkloadClaim first = store.acquire(pipeline, owner, 7, Duration.ofSeconds(2)).claim();
                WorkloadClaim original = store.advanceExecution(first, 7, Set.of("node-a", "node-b")).orElseThrow();
                WorkloadClaim refreshed = store.acquire(pipeline, owner, 8, Duration.ofSeconds(2)).claim();
                assertThat(refreshed.profileGeneration()).isPositive();
                assertThat(refreshed.executionProfile().generation()).isEqualTo(refreshed.profileGeneration());
                assertThat(refreshed.claimGeneration()).isEqualTo(original.claimGeneration());
                assertThat(refreshed.topologyRevision()).isEqualTo(8);
                assertThat(refreshed.executionTopologyRevision()).isEqualTo(7);
                assertThat(PendingPipelineResume.sameExecutionContext(original, refreshed)).isTrue();
                if (release) {
                    assertThat(store.release(refreshed)).isTrue();
                } else {
                    awaitServerExpiry(store, pipeline);
                }

                WorkloadClaim current = store.acquire(pipeline, owner, 8, TTL).claim();

                assertThat(current.claimGeneration()).isEqualTo(refreshed.claimGeneration() + 1);
                assertThat(PendingPipelineResume.sameExecutionContext(refreshed, current)).isTrue();
                assertOldGenerationIsRejected(store, collection, refreshed, current);
                assertThat(collection.countDocuments(new org.bson.Document("resourceType", WorkloadClaimType.PIPELINE_ACTUATION.name())))
                        .isEqualTo(1);
            });
        }
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

    private static void assertOldGenerationIsRejected(MongoWorkloadClaimStore store,
            com.mongodb.client.MongoCollection<org.bson.Document> collection, WorkloadClaim original, WorkloadClaim current) {
        org.bson.Document before = collection.find(new org.bson.Document("resourceId", current.key().resourceId())).first();
        assertThat(store.renew(original, TTL)).isEmpty();
        assertThat(store.advanceExecution(original, original.topologyRevision(), original.executionNodeIds())).isEmpty();
        assertThat(store.recordExecutionFailure(original, false)).isEmpty();
        assertThat(store.release(original)).isFalse();
        assertThat(collection.find(new org.bson.Document("resourceId", current.key().resourceId())).first()).isEqualTo(before);
        assertThat(store.read(current.key()).orElseThrow().claim()).isEqualTo(current);
    }

    private static void awaitServerExpiry(MongoWorkloadClaimStore store, WorkloadClaimKey key) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(8).toNanos();
        while (store.read(key).orElseThrow().leased()) {
            if (System.nanoTime() >= deadline) {
                throw new AssertionError("the claim did not expire on the Mongo server clock");
            }
            Thread.sleep(10);
        }
    }

    private static void withProfiledStore(CheckedBody body) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            var database = client.getDatabase("tapstate");
            var collection = database.getCollection(MongoStorePort.WORKLOAD_CLAIMS);
            var profiles = database.getCollection("workload_test_profiles");
            var registry = database.getCollection("workload_test_nodes");
            collection.drop();
            profiles.drop();
            registry.drop();
            MongoClusterProfileStore profileStore = new MongoClusterProfileStore(client, profiles, collection, registry);
            profileStore.reserve("cluster-a", new WorkloadOwner("node-a", "boot-1"), URI.create("http://node-a:8080"),
                    new ExecutionProfile(1, Map.of("build", "one", "threads", "4")), Duration.ofMinutes(1));
            body.run(new MongoWorkloadClaimStore(collection, profileStore), collection);
        } catch (RuntimeException | Error failure) {
            throw failure;
        } catch (Exception failure) {
            throw new AssertionError(failure);
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

    @FunctionalInterface
    private interface CheckedBody {
        void run(MongoWorkloadClaimStore store, com.mongodb.client.MongoCollection<org.bson.Document> collection)
                throws Exception;
    }
}
