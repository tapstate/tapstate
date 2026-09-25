package io.tapstate.app;

import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class PipelineActuationOwnershipConcurrencyTest {

    private static final Duration TTL = Duration.ofSeconds(30);

    @Test
    void allocationAndRemovalSerializePerPipelineWithoutBlockingAnotherPermit() throws Exception {
        BlockingClaims store = new BlockingClaims();
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        ClusterMembershipGate membership = new ClusterMembershipGate(properties);
        membership.install(new ClusterMembership("cluster-a", 7, Set.of("node-a", "node-b", "node-c")));
        membership.canCommit(Set.of("node-a", "node-b"));
        PipelineActuationOwnership ownership = new PipelineActuationOwnership(
                "cluster-a", new WorkloadOwner("node-a", "boot-a"), membership,
                new ClusterWorkloadClaims(store, membership), TTL, Duration.ofSeconds(10), () -> 0L);
        assertThat(ownership.permit("orders").granted()).isTrue();
        assertThat(ownership.permit("customers").granted()).isTrue();

        try (var workers = Executors.newFixedThreadPool(3)) {
            Future<PipelineActuationOwnership.Execution> starting =
                    workers.submit(() -> ownership.beginExecution("orders"));
            assertThat(store.enteredAdvance.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(ownership.permit("orders").retry())
                    .as("a busy pipeline asks the scheduler to retry without cancelling its worker").isTrue();

            Future<PipelineActuationOwnership.Permit> unrelated =
                    workers.submit(() -> ownership.permit("customers"));
            assertThat(unrelated.get(2, TimeUnit.SECONDS).granted())
                    .as("one pipeline's store round trip must not block another permit").isTrue();

            CountDownLatch retainStarted = new CountDownLatch(1);
            Future<?> removing = workers.submit(() -> {
                retainStarted.countDown();
                ownership.retain(List.of("customers"));
            });
            assertThat(retainStarted.await(2, TimeUnit.SECONDS)).isTrue();
            removing.get(2, TimeUnit.SECONDS);
            assertThat(store.releasedOrders.getCount())
                    .as("retain defers this claim while its execution advance is in flight").isEqualTo(1);

            store.finishAdvance.countDown();
            assertThat(starting.get(2, TimeUnit.SECONDS).fence().executionGeneration()).isEqualTo(1);
            ownership.retain(List.of("customers"));
            assertThat(store.releasedOrders.getCount()).isZero();
            assertThat(ownership.beginExecution("orders").allowed())
                    .as("a removed claim cannot authorize a late worker").isFalse();
        } finally {
            store.finishAdvance.countDown();
        }
    }

    private static final class BlockingClaims implements WorkloadClaimStore {
        private final InMemoryWorkloadClaimStore delegate = new InMemoryWorkloadClaimStore();
        private final CountDownLatch enteredAdvance = new CountDownLatch(1);
        private final CountDownLatch finishAdvance = new CountDownLatch(1);
        private final CountDownLatch releasedOrders = new CountDownLatch(1);

        @Override
        public WorkloadClaimAttempt acquire(
                WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
            return delegate.acquire(key, owner, topologyRevision, ttl);
        }

        @Override
        public Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
            return delegate.renew(expected, ttl);
        }

        @Override
        public boolean release(WorkloadClaim expected) {
            if (expected.key().resourceId().equals("orders")) {
                releasedOrders.countDown();
            }
            return delegate.release(expected);
        }

        @Override
        public Optional<WorkloadClaim> advanceUnderClaim(WorkloadClaim expected, long topologyRevision) {
            if (expected.key().resourceId().equals("orders")) {
                enteredAdvance.countDown();
                try {
                    if (!finishAdvance.await(2, TimeUnit.SECONDS)) {
                        throw new AssertionError("execution advance was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(interrupted);
                }
            }
            return delegate.advanceUnderClaim(expected, topologyRevision);
        }

        @Override
        public Optional<WorkloadClaimReading> read(WorkloadClaimKey key) {
            return delegate.read(key);
        }
    }
}
