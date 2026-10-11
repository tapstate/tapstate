package io.tapstate.app;

import io.tapstate.adapters.mongostore.SourceConfigKeyringSession;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
    void aLiveNodeSessionRenewsAndReleasesItsKeyringAcknowledgement() throws Exception {
        RecordingStore store = new RecordingStore(true);
        RecordingKeyring keyring = new RecordingKeyring(false);
        NodeSessionLease lease = new NodeSessionLease(
                store, CLAIM, System.nanoTime(), Duration.ofSeconds(1), Duration.ofMillis(10), keyring,
                () -> { });

        assertThat(keyring.acknowledged.await(1, TimeUnit.SECONDS)).isTrue();
        lease.close();

        assertThat(keyring.acknowledgements).hasValueGreaterThanOrEqualTo(1);
        assertThat(keyring.released.get()).isTrue();
    }

    @Test
    void aKeyringEpochRefusalStopsTheMemberEvenWhenTheNodeClaimRenewed() throws Exception {
        RecordingStore store = new RecordingStore(true);
        RecordingKeyring keyring = new RecordingKeyring(true);
        CountDownLatch memberStopped = new CountDownLatch(1);
        NodeSessionLease lease = new NodeSessionLease(
                store, CLAIM, System.nanoTime(), Duration.ofSeconds(1), Duration.ofMillis(10), keyring,
                memberStopped::countDown);
        try {
            assertThat(memberStopped.await(1, TimeUnit.SECONDS)).isTrue();
        } finally {
            lease.close();
        }
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

    @Test
    void anExpiredLocalProofDoesNotAskTheStoreToCreateANewProof() throws Exception {
        RecordingStore store = new RecordingStore(true);
        RecordingKeyring keyring = new RecordingKeyring(false);
        CountDownLatch stopped = new CountDownLatch(1);
        try (NodeSessionLease lease = new NodeSessionLease(store, CLAIM,
                System.nanoTime() - Duration.ofSeconds(1).toNanos(), Duration.ofMillis(200),
                Duration.ofMillis(50), keyring, stopped::countDown)) {
            assertThat(stopped.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(store.renewed.getCount()).as("expired proof cannot be extended by a late store success").isEqualTo(1);
            assertThat(keyring.acknowledgements).hasValue(0);
        }
    }

    @ParameterizedTest
    @EnumSource(FirstFailure.class)
    void anImmediateFirstRefusalRunsOnlyAfterBothTasksAreRegistered(FirstFailure failure) throws Exception {
        for (int race = 0; race < 20; race++) {
            RecordingStore store = new RecordingStore(failure != FirstFailure.OWNER,
                    failure == FirstFailure.STORE ? new TapstateException(IoError.STORE_UNAVAILABLE,
                            Map.of("detail", "owned fixture store unavailable"), null) : null);
            RecordingKeyring keyring = new RecordingKeyring(failure == FirstFailure.ACK);
            CountDownLatch stopped = new CountDownLatch(1);
            AtomicInteger stops = new AtomicInteger();
            try (NodeSessionLease lease = new NodeSessionLease(store, CLAIM, System.nanoTime(),
                    Duration.ofSeconds(2), Duration.ofSeconds(1), keyring, () -> {
                        stops.incrementAndGet();
                        stopped.countDown();
                    })) {
                assertThat(stopped.await(500, TimeUnit.MILLISECONDS))
                        .as("the first refusal must not wait for the periodic renewal interval").isTrue();
                assertThat(stops).hasValue(1);
            }
            assertThat(stops).hasValue(1);
        }
    }

    @ParameterizedTest
    @EnumSource(HeldSuccess.class)
    void aLateSuccessfulResponseCannotExtendAnAlreadyLapsedProof(HeldSuccess stage) throws Exception {
        Delay delay = new Delay();
        RecordingStore store = new RecordingStore(true, null, stage == HeldSuccess.STORE ? delay : null);
        RecordingKeyring keyring = new RecordingKeyring(false, stage == HeldSuccess.ACK ? delay : null);
        CountDownLatch stopped = new CountDownLatch(1);
        AtomicInteger stops = new AtomicInteger();
        Duration ttl = Duration.ofSeconds(2);
        long askedAt = System.nanoTime() - Duration.ofMillis(1500).toNanos();
        long originalDeadline = askedAt + ttl.toNanos();
        try (NodeSessionLease lease = new NodeSessionLease(store, CLAIM, askedAt, ttl,
                Duration.ofMillis(1800), keyring, () -> {
                    stops.incrementAndGet();
                    stopped.countDown();
                })) {
            assertThat(delay.entered.await(300, TimeUnit.MILLISECONDS)).as("the immediate renewal reached its held response").isTrue();
            while (System.nanoTime() - originalDeadline < 0) Thread.sleep(5);
            assertThat(stopped.getCount()).as("the coarse lapse checker has not run before the controlled return").isEqualTo(1);
            delay.resume.countDown();
            assertThat(stopped.await(300, TimeUnit.MILLISECONDS))
                    .as("a response after the prior proof expired must fail closed before publishing a new proof").isTrue();
            assertThat(stops).hasValue(1);
            if (stage == HeldSuccess.STORE) {
                assertThat(keyring.acknowledgements).as("an already-lapsed proof cannot publish an ACK").hasValue(0);
            }
        } finally {
            delay.resume.countDown();
        }
        assertThat(stops).hasValue(1);
    }

    private enum HeldSuccess { STORE, ACK }

    private static final class Delay {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch resume = new CountDownLatch(1);

        private void block() {
            entered.countDown();
            try {
                if (!resume.await(2, TimeUnit.SECONDS)) throw new AssertionError("owned successful response was not released");
            } catch (InterruptedException stopped) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private enum FirstFailure { OWNER, ACK, STORE }

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
        private final RuntimeException renewalFailure;
        private final Delay delay;
        private final CountDownLatch renewed = new CountDownLatch(1);
        private final AtomicBoolean released = new AtomicBoolean();

        private RecordingStore(boolean renews) {
            this(renews, null);
        }

        private RecordingStore(boolean renews, RuntimeException renewalFailure) {
            this(renews, renewalFailure, null);
        }

        private RecordingStore(boolean renews, RuntimeException renewalFailure, Delay delay) {
            this.renews = renews;
            this.renewalFailure = renewalFailure;
            this.delay = delay;
        }

        @Override
        public WorkloadClaimAttempt acquire(
                WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
            renewed.countDown();
            if (delay != null) delay.block();
            if (renewalFailure != null) throw renewalFailure;
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

    private static final class RecordingKeyring implements SourceConfigKeyringSession {
        private final boolean refuse;
        private final Delay delay;
        private final CountDownLatch acknowledged = new CountDownLatch(1);
        private final AtomicInteger acknowledgements = new AtomicInteger();
        private final AtomicBoolean released = new AtomicBoolean();

        private RecordingKeyring(boolean refuse) {
            this(refuse, null);
        }

        private RecordingKeyring(boolean refuse, Delay delay) {
            this.refuse = refuse;
            this.delay = delay;
        }

        @Override
        public void acknowledge(WorkloadClaim session, Duration ttl) {
            acknowledgements.incrementAndGet();
            if (delay != null) delay.block();
            acknowledged.countDown();
            if (refuse) {
                throw new TapstateException(
                        io.tapstate.adapters.mongostore.StoreError.SOURCE_CONFIG_KEYRING_NOT_READY,
                        java.util.Map.of(), null);
            }
        }

        @Override
        public void release(WorkloadClaim session) {
            released.set(true);
        }
    }
}
