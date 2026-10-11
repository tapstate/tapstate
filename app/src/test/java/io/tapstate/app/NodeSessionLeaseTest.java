package io.tapstate.app;

import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.ClusterProfileStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.core.common.TapstateException;
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
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

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
    void anAcquiredSessionNearItsOriginalDeadlineRequestsRenewalBeforeThePeriodicInterval() throws Exception {
        RecordingStore store = new RecordingStore(true);
        AtomicBoolean memberStopped = new AtomicBoolean();
        Duration ttl = Duration.ofSeconds(9);
        Duration remaining = Duration.ofSeconds(2);
        long askedAt = System.nanoTime() - ttl.minus(remaining).toNanos();
        long originalDeadline = askedAt + ttl.toNanos();
        NodeSessionLease session = new NodeSessionLease(
                store, CLAIM, askedAt, ttl, Duration.ofSeconds(3), () -> memberStopped.set(true));
        try {
            assertThat(store.renewed.await(1, TimeUnit.SECONDS))
                    .as("the acquired session has two seconds left, so its first renewal cannot wait three")
                    .isTrue();
            assertThat(store.firstRenewalAskedAt.get()).isLessThan(originalDeadline);
            assertThat(memberStopped).as("an available renewal preserves this exact boot's authority").isFalse();
        } finally {
            session.close();
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
        long askedAt = System.nanoTime() - Duration.ofMillis(250).toNanos();
        NodeSessionLease session = new NodeSessionLease(
                store, CLAIM, askedAt, lease, Duration.ofMillis(20), memberStopped::countDown);
        try {
            assertThat(store.asked.await(1, TimeUnit.SECONDS))
                    .as("a renewal was asked for, and the store is not answering it")
                    .isTrue();
            assertThat(session.proof().deadlineNanos())
                    .as("a stalled first renewal cannot reset the already acquired session's deadline")
                    .isEqualTo(askedAt + lease.toNanos());
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

    private static final WorkloadClaim PROFILE_CLAIM = new WorkloadClaim(
            new WorkloadClaimKey("cluster-a", WorkloadClaimType.NODE_SESSION, "node-c"),
            new WorkloadOwner("node-c", "80cdfe5d-eabf-4ca3-9091-7b7d68685b14"),
            1, 0, 0, Instant.now().plusSeconds(30), 0, 0, Set.of(), 0, false, 1);
    private static final NodeSessionLease.JoinedIdentity ORIGINAL_JOIN = new NodeSessionLease.JoinedIdentity(
            "0a8bfdb5-270c-40b4-8a9f-9febd668c1dc", "[127.0.0.1]:61763", PROFILE_CLAIM.owner().nodeId(),
            PROFILE_CLAIM.owner().bootId(), "1", "7a66995c4d6e1da8d5458f166420aee0da32906360337991c4af21b2365e500f");
    private static final NodeSessionLease.JoinedIdentity MERGED_JOIN = new NodeSessionLease.JoinedIdentity(
            "41ecddca-dcb8-4a70-a418-06f21c854d84", "[127.0.0.1]:61763", ORIGINAL_JOIN.nodeId(),
            ORIGINAL_JOIN.bootId(), ORIGINAL_JOIN.profileGeneration(), ORIGINAL_JOIN.profileHash());

    @Test
    void aChangedNativeUuidPublishesOnlyTheExactAcceptedNodeSession() throws Exception {
        WorkloadClaim accepted = acceptedProfileClaim(1);
        WorkloadClaimStore store = mock(WorkloadClaimStore.class);
        CountDownLatch renewed = new CountDownLatch(1);
        when(store.renew(any(), any())).thenAnswer(call -> { renewed.countDown(); return Optional.of(accepted); });
        ClusterProfileStore profiles = mock(ClusterProfileStore.class);
        CountDownLatch refreshed = new CountDownLatch(1);
        AtomicReference<WorkloadClaim> published = new AtomicReference<>();
        when(profiles.markJoined(any(), anyString(), anyString())).thenAnswer(call -> {
            assertThat(call.getArgument(1, String.class)).isEqualTo(MERGED_JOIN.memberUuid());
            assertThat(call.getArgument(2, String.class)).isEqualTo(MERGED_JOIN.memberAddress());
            published.set(call.getArgument(0, WorkloadClaim.class));
            refreshed.countDown();
            return true;
        });
        AtomicReference<NodeSessionLease.JoinedIdentity> actual = new AtomicReference<>(ORIGINAL_JOIN);
        AtomicBoolean stopped = new AtomicBoolean();
        try (NodeSessionLease session = profileSession(store, profiles, actual, () -> stopped.set(true))) {
            assertThat(renewed.await(1, TimeUnit.SECONDS)).isTrue();
            actual.set(MERGED_JOIN);
            assertThat(refreshed.await(1, TimeUnit.SECONDS)).as("the real new UUID must replace the old joined proof").isTrue();
            assertThat(published.get()).isSameAs(accepted);
            assertThat(published.get().owner()).isEqualTo(PROFILE_CLAIM.owner());
            assertThat(published.get().claimGeneration()).isEqualTo(PROFILE_CLAIM.claimGeneration());
            assertThat(published.get().profileGeneration()).isEqualTo(PROFILE_CLAIM.profileGeneration());
            assertThat(stopped).isFalse();
            assertThat(session.proof().live()).isTrue();
            verify(store, never()).acquire(any(), any(), anyLong(), any());
            verify(store, never()).advanceExecution(any(), anyLong(), anySet());
        }
    }

    @Test
    void anUnchangedNativeIdentityDoesNotRewriteJoinTimeDuringRenewals() throws Exception {
        WorkloadClaimStore store = mock(WorkloadClaimStore.class);
        CountDownLatch renewals = new CountDownLatch(3);
        when(store.renew(any(), any())).thenAnswer(call -> { renewals.countDown(); return Optional.of(acceptedProfileClaim(1)); });
        ClusterProfileStore profiles = mock(ClusterProfileStore.class);
        AtomicReference<NodeSessionLease.JoinedIdentity> actual = new AtomicReference<>(ORIGINAL_JOIN);
        try (NodeSessionLease session = profileSession(store, profiles, actual, () -> { })) {
            assertThat(renewals.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(session.proof().live()).isTrue();
            verify(profiles, never()).markJoined(any(), anyString(), anyString());
        }
    }

    @Test
    void aRefusedChangedJoinProofStopsTheMemberAfterItsLeaseWasRenewed() throws Exception {
        WorkloadClaim accepted = acceptedProfileClaim(1);
        WorkloadClaimStore store = mock(WorkloadClaimStore.class);
        when(store.renew(any(), any())).thenReturn(Optional.of(accepted));
        ClusterProfileStore profiles = mock(ClusterProfileStore.class);
        when(profiles.markJoined(any(), anyString(), anyString())).thenReturn(false);
        CountDownLatch stopped = new CountDownLatch(1);
        try (NodeSessionLease session = profileSession(store, profiles, new AtomicReference<>(MERGED_JOIN), stopped::countDown)) {
            assertThat(stopped.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(session.proof().live()).isFalse();
            verify(profiles).markJoined(same(accepted), eq(MERGED_JOIN.memberUuid()), eq(MERGED_JOIN.memberAddress()));
            verify(store, never()).acquire(any(), any(), anyLong(), any());
        }
    }

    @Test
    void anUnavailableChangedJoinProofCannotLeaveTheMemberAuthorized() throws Exception {
        WorkloadClaimStore store = mock(WorkloadClaimStore.class);
        when(store.renew(any(), any())).thenReturn(Optional.of(acceptedProfileClaim(1)));
        ClusterProfileStore profiles = mock(ClusterProfileStore.class);
        when(profiles.markJoined(any(), anyString(), anyString())).thenThrow(
                new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "join proof unavailable"), null));
        CountDownLatch stopped = new CountDownLatch(1);
        try (NodeSessionLease session = profileSession(store, profiles, new AtomicReference<>(MERGED_JOIN), stopped::countDown)) {
            assertThat(stopped.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(session.proof().live()).isFalse();
        }
    }

    @Test
    void aJoinRefreshTheStoreNeverAnswersIsBoundedByTheIndependentLapseCheck() throws Exception {
        WorkloadClaimStore store = mock(WorkloadClaimStore.class);
        when(store.renew(any(), any())).thenReturn(Optional.of(acceptedProfileClaim(1)));
        ClusterProfileStore profiles = mock(ClusterProfileStore.class);
        CountDownLatch asked = new CountDownLatch(1);
        CountDownLatch answer = new CountDownLatch(1);
        when(profiles.markJoined(any(), anyString(), anyString())).thenAnswer(call -> {
            asked.countDown();
            try { answer.await(); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
            return false;
        });
        CountDownLatch stopped = new CountDownLatch(1);
        Duration ttl = Duration.ofMillis(500);
        long askedAt = System.nanoTime();
        try (NodeSessionLease session = new NodeSessionLease(store, PROFILE_CLAIM, askedAt, ttl, Duration.ofMillis(20),
                stopped::countDown, profiles, () -> MERGED_JOIN, ORIGINAL_JOIN)) {
            assertThat(asked.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(session.proof().deadlineNanos()).isEqualTo(askedAt + ttl.toNanos());
            assertThat(stopped.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(session.proof().live()).isFalse();
        } finally { answer.countDown(); }
    }

    @Test
    void anotherClaimGenerationCannotBorrowThisBootsJoinedIdentity() throws Exception {
        WorkloadClaimStore store = mock(WorkloadClaimStore.class);
        when(store.renew(any(), any())).thenReturn(Optional.of(acceptedProfileClaim(2)));
        ClusterProfileStore profiles = mock(ClusterProfileStore.class);
        CountDownLatch stopped = new CountDownLatch(1);
        try (NodeSessionLease session = profileSession(store, profiles, new AtomicReference<>(MERGED_JOIN), stopped::countDown)) {
            assertThat(stopped.await(1, TimeUnit.SECONDS)).isTrue();
            assertThat(session.proof().live()).isFalse();
            assertThat(session.proof().claim().claimGeneration()).isEqualTo(PROFILE_CLAIM.claimGeneration());
            verify(profiles, never()).markJoined(any(), anyString(), anyString());
        }
    }

    @Test
    void anotherNativeBootOrProfileCannotBorrowTheCurrentNodeSession() throws Exception {
        List<NodeSessionLease.JoinedIdentity> unrelated = List.of(
                new NodeSessionLease.JoinedIdentity(MERGED_JOIN.memberUuid(), MERGED_JOIN.memberAddress(),
                        ORIGINAL_JOIN.nodeId(), "63d1db0e-911f-4ef2-80b4-8a9f79a8b242", "1", ORIGINAL_JOIN.profileHash()),
                new NodeSessionLease.JoinedIdentity(MERGED_JOIN.memberUuid(), MERGED_JOIN.memberAddress(),
                        ORIGINAL_JOIN.nodeId(), ORIGINAL_JOIN.bootId(), "1", "8a66995c4d6e1da8d5458f166420aee0da32906360337991c4af21b2365e500f"));
        for (NodeSessionLease.JoinedIdentity nativeIdentity : unrelated) {
            WorkloadClaimStore store = mock(WorkloadClaimStore.class);
            when(store.renew(any(), any())).thenReturn(Optional.of(acceptedProfileClaim(1)));
            ClusterProfileStore profiles = mock(ClusterProfileStore.class);
            CountDownLatch stopped = new CountDownLatch(1);
            try (NodeSessionLease session = profileSession(store, profiles, new AtomicReference<>(nativeIdentity), stopped::countDown)) {
                assertThat(stopped.await(1, TimeUnit.SECONDS)).isTrue();
                assertThat(session.proof().live()).isFalse();
                verify(profiles, never()).markJoined(any(), anyString(), anyString());
            }
        }
    }

    @Test
    void aProfileAwareSessionRequiresItsProfileStore() {
        WorkloadClaimStore store = mock(WorkloadClaimStore.class);
        when(store.renew(any(), any())).thenReturn(Optional.of(acceptedProfileClaim(1)));
        AtomicReference<NodeSessionLease> created = new AtomicReference<>();
        try {
            assertThatThrownBy(() -> created.set(profileSession(store, null, new AtomicReference<>(ORIGINAL_JOIN), () -> { })))
                    .isInstanceOfSatisfying(TapstateException.class,
                            coded -> assertThat(coded.code()).isEqualTo(BootError.COORDINATION_STORE_REQUIRED));
        } finally {
            if (created.get() != null) created.get().close();
        }
    }

    private static WorkloadClaim acceptedProfileClaim(long generation) {
        return new WorkloadClaim(PROFILE_CLAIM.key(), PROFILE_CLAIM.owner(), generation, 0, 0,
                Instant.now().plusSeconds(60), 0, 0, Set.of(), 0, false, PROFILE_CLAIM.profileGeneration());
    }

    private static NodeSessionLease profileSession(WorkloadClaimStore store, ClusterProfileStore profiles,
            AtomicReference<NodeSessionLease.JoinedIdentity> actual, Runnable stopped) {
        return new NodeSessionLease(store, PROFILE_CLAIM, System.nanoTime(), Duration.ofSeconds(1), Duration.ofMillis(10),
                stopped, profiles, actual::get, ORIGINAL_JOIN);
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
        private final AtomicLong firstRenewalAskedAt = new AtomicLong();
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
            firstRenewalAskedAt.compareAndSet(0L, System.nanoTime());
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
