package io.tapstate.app;

import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class NodeSessionLeaseTest {

    private static final WorkloadClaim CLAIM = new WorkloadClaim(
            new WorkloadClaimKey("cluster-a", WorkloadClaimType.NODE_SESSION, "node-a"),
            new WorkloadOwner("node-a", "boot-1"), 1, 0, 0, Instant.now().plusSeconds(30));

    @Test
    void aLiveNodeSessionIsRenewedAndReleasedBeforeTheMemberIsDestroyed() throws Exception {
        RecordingStore store = new RecordingStore(true);
        NodeSessionLease lease = new NodeSessionLease(
                store, CLAIM, System.nanoTime(), Duration.ofSeconds(1), Duration.ofMillis(10), () -> { });

        assertThat(store.renewed.await(1, TimeUnit.SECONDS)).isTrue();
        lease.close();

        assertThat(store.released.get()).isTrue();
    }

    @Test
    void losingTheExactClaimStopsTheMemberRatherThanRenewingAsTheNewGeneration() throws Exception {
        RecordingStore store = new RecordingStore(false);
        CountDownLatch memberStopped = new CountDownLatch(1);
        NodeSessionLease lease = new NodeSessionLease(
                store, CLAIM, System.nanoTime(), Duration.ofSeconds(1), Duration.ofMillis(10),
                memberStopped::countDown);

        assertThat(memberStopped.await(1, TimeUnit.SECONDS)).isTrue();
        lease.close();

        assertThat(store.released.get()).isFalse();
    }

    /**
     * The member is stopped from a renewer thread, and stopping it gracefully waits -- on the partitions it
     * hands over, on its own services. Shutting the renewer down before running the stop interrupted the
     * very thread about to run it, so the first of those waits returned at once.
     */
    @Test
    void theMemberIsStoppedFromARenewerThreadThatCanStillWait() throws Exception {
        RecordingStore store = new RecordingStore(false);
        CountDownLatch memberStopped = new CountDownLatch(1);
        AtomicBoolean waitedThroughTheStop = new AtomicBoolean();
        NodeSessionLease lease = new NodeSessionLease(
                store, CLAIM, System.nanoTime(), Duration.ofSeconds(1), Duration.ofMillis(10), () -> {
                    try {
                        Thread.sleep(20);
                        waitedThroughTheStop.set(true);
                    } catch (InterruptedException cutShort) {
                        Thread.currentThread().interrupt();
                    }
                    memberStopped.countDown();
                });
        try {
            assertThat(memberStopped.await(5, TimeUnit.SECONDS)).as("the refused renewal stops the member").isTrue();
            assertThat(waitedThroughTheStop)
                    .as("a wait on the way out of the stop runs to its end rather than being interrupted")
                    .isTrue();
        } finally {
            lease.close();
        }
    }

    /**
     * A renewal the store never answers loses the session once the lease the last accepted renewal bought
     * has run out -- not before, and not only when the store finally answers.
     *
     * <p>Measured as a store frozen under three members: the renewal hung for the whole outage, every
     * member stayed in the cluster throughout, and once the store answered again each of them answered for
     * the cluster for a moment before its renewal came back refused.
     */
    @Test
    void aRenewalTheStoreNeverAnswersLosesTheSessionOnceTheLeaseItLastBoughtRunsOut() throws Exception {
        SilentStore store = new SilentStore();
        CountDownLatch memberStopped = new CountDownLatch(1);
        Duration lease = Duration.ofMillis(500);
        long askedAt = System.nanoTime();
        NodeSessionLease session = new NodeSessionLease(
                store, CLAIM, askedAt, lease, Duration.ofMillis(20), memberStopped::countDown);
        try {
            assertThat(store.asked.await(1, TimeUnit.SECONDS))
                    .as("a renewal was asked for, and the store is not answering it")
                    .isTrue();
            assertThat(memberStopped.await(5, TimeUnit.SECONDS))
                    .as("the member leaves once the lease it last proved has run out")
                    .isTrue();
            assertThat(Duration.ofNanos(System.nanoTime() - askedAt))
                    .as("and not before it has")
                    .isGreaterThanOrEqualTo(lease);
        } finally {
            session.close();
            store.answer();
        }
    }

    /** A session the store keeps renewing is never lost to the clock, however long it runs. */
    @Test
    void aSessionTheStoreKeepsRenewingIsNotLostToTheClock() throws Exception {
        RecordingStore store = new RecordingStore(true);
        CountDownLatch memberStopped = new CountDownLatch(1);
        NodeSessionLease session = new NodeSessionLease(
                store, CLAIM, System.nanoTime(), Duration.ofMillis(200), Duration.ofMillis(20),
                memberStopped::countDown);
        try {
            assertThat(memberStopped.await(1, TimeUnit.SECONDS))
                    .as("five leases' worth of renewals, every one of them accepted")
                    .isFalse();
        } finally {
            session.close();
        }
    }

    /** A store whose renewals hang until {@link #answer} lets them go, as a frozen store's do. */
    private static final class SilentStore implements WorkloadClaimStore {

        private final CountDownLatch asked = new CountDownLatch(1);
        private final CountDownLatch answered = new CountDownLatch(1);

        void answer() {
            answered.countDown();
        }

        @Override
        public Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
            asked.countDown();
            try {
                answered.await();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
            return Optional.empty();
        }

        @Override
        public WorkloadClaimAttempt acquire(
                WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean release(WorkloadClaim expected) {
            return true;
        }

        @Override
        public Optional<WorkloadClaim> advanceExecution(
                WorkloadClaim expected, long topologyRevision, java.util.Set<String> executionNodeIds) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<WorkloadClaim> recordExecutionFailure(
                WorkloadClaim expected, boolean afterMemberLoss) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<WorkloadClaimReading> read(WorkloadClaimKey key) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class RecordingStore implements WorkloadClaimStore {

        /** Nothing here reads the lease; it answers with a live one so a read is not a lapsed claim. */
        private static final Duration LIVE_LEASE = Duration.ofSeconds(30);
        private final boolean renews;
        private final CountDownLatch renewed = new CountDownLatch(1);
        private final AtomicBoolean released = new AtomicBoolean();

        private RecordingStore(boolean renews) {
            this.renews = renews;
        }

        @Override
        public WorkloadClaimAttempt acquire(
                WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
            renewed.countDown();
            return renews ? Optional.of(expected) : Optional.empty();
        }

        @Override
        public boolean release(WorkloadClaim expected) {
            released.set(true);
            return true;
        }

        @Override
        public Optional<WorkloadClaim> advanceExecution(
                WorkloadClaim expected, long topologyRevision, java.util.Set<String> executionNodeIds) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<WorkloadClaim> recordExecutionFailure(
                WorkloadClaim expected, boolean afterMemberLoss) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<WorkloadClaimReading> read(WorkloadClaimKey key) {
            return Optional.of(new WorkloadClaimReading(CLAIM, LIVE_LEASE));
        }
    }
}
