package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
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
    void oneExpectedExecutionGenerationCanAdvanceOnlyOnce() {
        withStore((store, collection) -> {
            WorkloadClaim claim = store.acquire(
                    new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders"),
                    new WorkloadOwner("node-a", "boot-1"), 7, TTL).claim();

            WorkloadClaim advanced = store.advanceExecution(claim, 7).orElseThrow();

            assertThat(advanced.claimGeneration()).isEqualTo(claim.claimGeneration());
            assertThat(advanced.executionGeneration()).isEqualTo(1);
            assertThat(store.advanceExecution(claim, 7)).isEmpty();
        });
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
