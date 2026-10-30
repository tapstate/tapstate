package io.tapstate.app;

import io.tapstate.runtime.srs.CaptureId;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CaptureClaimLeaseTest {

    @Test
    void aDelayedLossCallbackCannotEndTheTenureStartedAfterTheOriginalPhysicalExit() throws Exception {
        var raw=spy(new InMemoryWorkloadClaimStore());var gate=eligibleGate();
        var ownership=spy(new CaptureOwnership("cluster-a",new WorkloadOwner("node-a","boot-a"),gate,
                new ClusterWorkloadClaims(raw,gate),Duration.ofSeconds(30)));
        var original=ownership.acquire(CAPTURE);
        doReturn(Optional.empty()).when(ownership).renew(any());
        var entered=new CountDownLatch(1);var resume=new CountDownLatch(1);
        var lease=new CaptureClaimLease(ownership,original.claim(),Duration.ofHours(1));
        lease.onLost(() -> {
            entered.countDown();
            try {assertThat(resume.await(3,TimeUnit.SECONDS)).isTrue();}
            catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new AssertionError(interrupted);}
            lease.closeAfterPhysicalExit();
        });
        try(var callers=Executors.newSingleThreadExecutor()) {
            var loss=callers.submit(lease::renew);
            try {
                assertThat(entered.await(3,TimeUnit.SECONDS)).isTrue();
                lease.closeAfterPhysicalExit();
                assertThat(raw.read(original.claim().key()).orElseThrow().leased()).isFalse();
                var replacement=ownership.acquire(CAPTURE);
                assertThat(replacement.acquired()).isTrue();
                assertThat(replacement.fence().claimGeneration()).isGreaterThan(original.fence().claimGeneration());
                resume.countDown();loss.get(3,TimeUnit.SECONDS);
                var live=raw.read(original.claim().key()).orElseThrow();assertThat(live.leased()).isTrue();
                assertThat(io.tapstate.spi.store.WorkloadClaimFence.from(live.claim())).isEqualTo(replacement.fence());
                verify(raw,times(1)).release(any());
            } finally {resume.countDown();}
        } finally {lease.close();}
    }

    @ParameterizedTest @ValueSource(strings={"not-committed","committed","replaced"})
    void anUnknownReleaseAfterPhysicalExitRetriesOnlyTheOriginalTuple(String outcome) {
        var raw=new InMemoryWorkloadClaimStore();var gate=eligibleGate();
        var ownership=spy(new CaptureOwnership("cluster-a",new WorkloadOwner("node-a","boot-a"),gate,
                new ClusterWorkloadClaims(raw,gate),Duration.ofSeconds(30)));
        var original=ownership.acquire(CAPTURE);var attempts=new AtomicInteger();
        doReturn(Optional.empty()).when(ownership).renew(any());
        doAnswer(call -> {
            if(attempts.incrementAndGet()==1) {
                if(outcome.equals("committed"))call.callRealMethod();
                throw new io.tapstate.core.common.TapstateException(io.tapstate.spi.store.IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);
            }
            return call.callRealMethod();
        }).when(ownership).release(any());
        try(var lease=new CaptureClaimLease(ownership,original.claim(),Duration.ofHours(1))) {
            lease.onLost(lease::closeAfterPhysicalExit);lease.renew();
            assertThat(raw.read(original.claim().key()).orElseThrow().leased()).isEqualTo(!outcome.equals("committed"));
            var expected=original.fence();
            if(outcome.equals("replaced")) {
                assertThat(raw.release(original.claim())).isTrue();
                expected=io.tapstate.spi.store.WorkloadClaimFence.from(raw.acquire(original.claim().key(),
                        new WorkloadOwner("node-b","boot-b"),3,Duration.ofSeconds(30)).claim());
            }
            lease.closeAfterPhysicalExit();lease.closeAfterPhysicalExit();
            var after=raw.read(original.claim().key()).orElseThrow();
            assertThat(after.leased()).isEqualTo(outcome.equals("replaced"));
            assertThat(io.tapstate.spi.store.WorkloadClaimFence.from(after.claim())).isEqualTo(expected);
            assertThat(attempts).hasValue(2);
        }
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void anExplicitCaptureEndRechecksTheOriginalTupleAfterAnUnknownRelease(boolean committed) {
        var raw=spy(new InMemoryWorkloadClaimStore());var gate=eligibleGate();
        var ownership=new CaptureOwnership("cluster-a",new WorkloadOwner("node-a","boot-a"),gate,
                new ClusterWorkloadClaims(raw,gate),Duration.ofSeconds(30));
        var permit=ownership.acquire(CAPTURE);var attempts=new AtomicInteger();
        var unavailable=new io.tapstate.core.common.TapstateException(io.tapstate.spi.store.IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);
        doAnswer(call -> {
            if(attempts.incrementAndGet()==1) {
                if(committed)call.callRealMethod();
                throw unavailable;
            }
            return call.callRealMethod();
        }).when(raw).release(any());
        assertThatThrownBy(() -> ownership.endTenure(permit.fence())).isSameAs(unavailable);
        ownership.endTenure(permit.fence());int completed=attempts.get();ownership.endTenure(permit.fence());
        assertThat(completed).isEqualTo(committed?1:2);assertThat(attempts).hasValue(completed);
        var ended=raw.read(permit.claim().key()).orElseThrow();assertThat(ended.leased()).isFalse();
        assertThat(io.tapstate.spi.store.WorkloadClaimFence.from(ended.claim())).isEqualTo(permit.fence());
    }

    @ParameterizedTest @ValueSource(strings={"later-owner","foreign-owner","wrong-kind","missing","business-lost","release-refused","read-failed","unfenced"})
    void endingACaptureCannotAdoptUnknownOrForeignAuthority(String fault) {
        var raw=spy(new InMemoryWorkloadClaimStore());var gate=eligibleGate();var local=new WorkloadOwner("node-a","boot-a");
        var ownership=new CaptureOwnership("cluster-a",local,gate,new ClusterWorkloadClaims(raw,gate),Duration.ofSeconds(30));
        var permit=ownership.acquire(CAPTURE);var selected=permit.fence();
        switch(fault) {
            case "later-owner" -> {
                assertThat(raw.release(permit.claim())).isTrue();raw.acquire(permit.claim().key(),new WorkloadOwner("node-b","boot-b"),3,Duration.ofSeconds(30));
            }
            case "foreign-owner" -> selected=new io.tapstate.spi.store.WorkloadClaimFence(permit.claim().key(),new WorkloadOwner("node-b","boot-b"),1,0,3);
            case "wrong-kind" -> selected=new io.tapstate.spi.store.WorkloadClaimFence(new WorkloadClaimKey("cluster-a",io.tapstate.spi.store.WorkloadClaimType.PIPELINE_ACTUATION,"p"),local,1,0,3);
            case "missing" -> doReturn(Optional.empty()).when(raw).read(permit.claim().key());
            case "business-lost" -> gate.canCommit(Set.of("node-a"));
            case "release-refused" -> doReturn(false).when(raw).release(any());
            case "read-failed" -> doThrow(new io.tapstate.core.common.TapstateException(io.tapstate.spi.store.IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null)).when(raw).read(permit.claim().key());
            case "unfenced" -> ownership=CaptureOwnership.single();
            default -> throw new AssertionError(fault);
        }
        clearInvocations(raw);var checked=ownership;var expected=selected;
        assertThatThrownBy(() -> checked.endTenure(expected)).isInstanceOf(io.tapstate.core.common.TapstateException.class);
        if(!fault.equals("release-refused"))verify(raw,never()).release(any());
        verify(raw,never()).acquire(any(),any(),anyLong(),any());verify(raw,never()).renew(any(),any());
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void closingRetriesAnUnacknowledgedOriginalReleaseAfterStoppingRenewals(boolean committed) {
        var raw=new InMemoryWorkloadClaimStore();var gate=eligibleGate();
        var ownership=spy(new CaptureOwnership("cluster-a",new WorkloadOwner("node-a","boot-a"),gate,
                new ClusterWorkloadClaims(raw,gate),Duration.ofSeconds(30)));
        var permit=ownership.acquire(CAPTURE);var stopped=new AtomicInteger();var attempts=new AtomicInteger();
        var unavailable=new io.tapstate.core.common.TapstateException(io.tapstate.spi.store.IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null);
        doAnswer(call -> {
            if(attempts.incrementAndGet()==1) {
                if(committed)call.callRealMethod();
                throw unavailable;
            }
            return call.callRealMethod();
        }).when(ownership).release(any());
        try(var lease=new CaptureClaimLease(ownership,permit.claim(),Duration.ofHours(1),stopped::incrementAndGet)) {
            assertThatThrownBy(lease::close).isSameAs(unavailable);
            assertThat(raw.read(permit.claim().key()).orElseThrow().leased()).isEqualTo(!committed);
            assertThat(lease.retain(() -> {throw new AssertionError("closed capture");})).isFalse();
            lease.renew();verify(ownership,never()).renew(any());
            lease.close();var ended=raw.read(permit.claim().key()).orElseThrow();
            assertThat(ended.leased()).isFalse();
            assertThat(io.tapstate.spi.store.WorkloadClaimFence.from(ended.claim())).isEqualTo(permit.fence());
            assertThat(stopped).hasValue(0);assertThat(attempts).hasValue(2);
            lease.close();assertThat(attempts).hasValue(2);
        }
    }

    @Test
    void aRetiredLeaseRefusesAttachmentsAndCannotRenewItsReleasedClaim() {
        InMemoryWorkloadClaimStore raw = new InMemoryWorkloadClaimStore();
        ClusterMembershipGate gate = eligibleGate();
        CaptureOwnership ownership = new CaptureOwnership("cluster-a", new WorkloadOwner("node-a", "boot-a"),
                gate, new ClusterWorkloadClaims(raw, gate), Duration.ofSeconds(30));
        CaptureOwnership.Permit permit = ownership.acquire(CAPTURE);
        AtomicInteger stopped = new AtomicInteger();
        AtomicBoolean reserved = new AtomicBoolean();
        CaptureClaimLease lease = new CaptureClaimLease(ownership, permit.claim(), Duration.ofHours(1),
                stopped::incrementAndGet);
        try {
            lease.whileNeeded(() -> false);
            lease.renew();

            assertThat(raw.read(permit.claim().key()).orElseThrow().leased()).isFalse();
            assertThat(lease.retain(() -> reserved.set(true))).isFalse();
            assertThat(reserved).as("a retired tail cannot reserve a new attachment").isFalse();
            lease.whileNeeded(() -> true);
            lease.renew();
            assertThat(raw.read(permit.claim().key()).orElseThrow().leased())
                    .as("a later ownership observation cannot revive a retired lease").isFalse();
            assertThat(stopped).hasValue(1);
        } finally {
            lease.close();
        }
    }

    @Test
    void anUnfencedTailCanReserveAttachmentsWithoutAClusterLease() {
        CaptureClaimLease lease = CaptureClaimLease.unfenced();
        AtomicBoolean reserved = new AtomicBoolean();

        assertThat(lease.retain(() -> reserved.set(true))).isTrue();
        assertThat(reserved).isTrue();
        lease.close();
    }

    @Test
    void anAttachmentThatPreventsRetirementStillGetsTheRenewalDueOnThatPass() {
        InMemoryWorkloadClaimStore raw = new InMemoryWorkloadClaimStore();
        ClusterMembershipGate gate = eligibleGate();
        Duration ttl = Duration.ofSeconds(15);
        CaptureOwnership ownership = new CaptureOwnership("cluster-a", new WorkloadOwner("node-a", "boot-a"),
                gate, new ClusterWorkloadClaims(raw, gate), ttl);
        CaptureOwnership.Permit permit = ownership.acquire(CAPTURE);
        AtomicBoolean lost = new AtomicBoolean();
        CaptureClaimLease lease = new CaptureClaimLease(ownership, permit.claim(), Duration.ofHours(1),
                () -> lost.set(true));
        AtomicInteger looks = new AtomicInteger();
        // A driven attachment reserves the tail between the first look and the retirement's locked recheck.
        lease.whileNeeded(() -> looks.incrementAndGet() > 1);
        try {
            raw.elapse(Duration.ofSeconds(10));
            lease.renew();
            raw.elapse(Duration.ofSeconds(10));

            assertThat(raw.read(permit.claim().key()).orElseThrow().leased())
                    .as("declining retirement must still renew before the next interval passes the old TTL").isTrue();
            assertThat(lost).isFalse();
        } finally {
            lease.close();
        }
    }

    @Test
    void aRenewalThatNoLongerMatchesStopsTheCaptureWithoutReleasingTheNewOwner() {
        RefusingRenewals raw = new RefusingRenewals();
        ClusterMembershipGate gate = eligibleGate();
        CaptureOwnership ownership = new CaptureOwnership(
                "cluster-a", new WorkloadOwner("node-a", "boot-a"), gate,
                new ClusterWorkloadClaims(raw, gate), Duration.ofSeconds(30));
        CaptureOwnership.Permit permit = ownership.acquire(CaptureId.of(
                new CaptureConfig("mysql", Map.of("host", "db.internal"), List.of("orders")), null));
        AtomicBoolean lost = new AtomicBoolean();
        CaptureClaimLease lease = new CaptureClaimLease(
                ownership, permit.claim(), Duration.ofHours(1), () -> lost.set(true));

        lease.renew();
        lease.close();

        assertThat(lost).isTrue();
        assertThat(raw.releases).as("a fenced holder must not release a claim it no longer owns").isZero();
    }

    /**
     * A claim is renewed from the moment it is taken, before the capture it guards is open, so it can be lost
     * before there is anything to stop. That loss is not dropped: binding the stop answers it at once, since
     * the capture was opened over a claim somebody else may hold now.
     */
    @Test
    void aClaimLostBeforeItsCaptureIsOpenStopsTheCaptureAsSoonAsItIsBound() {
        RefusingRenewals raw = new RefusingRenewals();
        ClusterMembershipGate gate = eligibleGate();
        CaptureOwnership ownership = new CaptureOwnership(
                "cluster-a", new WorkloadOwner("node-a", "boot-a"), gate,
                new ClusterWorkloadClaims(raw, gate), Duration.ofSeconds(30));
        CaptureOwnership.Permit permit = ownership.acquire(CAPTURE);
        CaptureClaimLease lease = new CaptureClaimLease(ownership, permit.claim(), Duration.ofHours(1));

        lease.renew();
        AtomicBoolean lost = new AtomicBoolean();
        lease.onLost(() -> lost.set(true));
        lease.close();

        assertThat(lost).as("the capture opened meanwhile is stopped once there is one to stop").isTrue();
        assertThat(raw.releases).as("and a claim this member no longer owns is not released").isZero();
    }

    /**
     * A member joining the cluster moves the committed topology on and takes nothing from a capture already
     * being tailed, so its holder goes on holding it -- the lease is carried past the one it had when the
     * member joined, under the same owner and generation. Refusing the renewal instead stopped every tail
     * in the cluster within one renewal of any member joining.
     */
    @Test
    void aMemberJoiningTheClusterLeavesTheCaptureItsHolderTailsRunning() {
        InMemoryWorkloadClaimStore store = new InMemoryWorkloadClaimStore();
        ClusterMembershipGate gate = eligibleGate();
        WorkloadOwner owner = new WorkloadOwner("node-a", "boot-a");
        CaptureOwnership ownership = new CaptureOwnership(
                "cluster-a", owner, gate, new ClusterWorkloadClaims(store, gate), Duration.ofSeconds(30));
        CaptureOwnership.Permit permit = ownership.acquire(CAPTURE);
        AtomicBoolean lost = new AtomicBoolean();
        CaptureClaimLease lease = new CaptureClaimLease(
                ownership, permit.claim(), Duration.ofHours(1), () -> lost.set(true));
        try {
            gate.install(new ClusterMembership(
                    "cluster-a", 4, Set.of("node-a", "node-b", "node-c", "node-d")));
            gate.canCommit(Set.of("node-a", "node-b", "node-c", "node-d"));
            store.elapse(Duration.ofSeconds(20));

            lease.renew();
            // Past the lease the claim had when the member joined: only a renewal that happened keeps it.
            store.elapse(Duration.ofSeconds(20));

            assertThat(lost).as("the capture keeps running across the join").isFalse();
            assertThat(store.read(permit.claim().key())).hasValueSatisfying(reading -> {
                assertThat(reading.claim().owner()).isEqualTo(owner);
                assertThat(reading.claim().claimGeneration()).isEqualTo(permit.claim().claimGeneration());
                assertThat(reading.leased()).as("renewed rather than left to lapse").isTrue();
            });
        } finally {
            lease.close();
        }
    }

    /**
     * The capture is stopped from the renewer's own thread, and the stop has to be able to wait: for the
     * capture's thread, the connector, the store. Shutting the renewer down before running the stop
     * interrupted the very thread about to run it, so the first wait on the way out returned at once and a
     * fenced-out tail was left half stopped.
     */
    @Test
    void theCaptureIsStoppedFromARenewerThreadThatCanStillWait() throws InterruptedException {
        RefusingRenewals raw = new RefusingRenewals();
        ClusterMembershipGate gate = eligibleGate();
        CaptureOwnership ownership = new CaptureOwnership(
                "cluster-a", new WorkloadOwner("node-a", "boot-a"), gate,
                new ClusterWorkloadClaims(raw, gate), Duration.ofSeconds(30));
        CaptureOwnership.Permit permit = ownership.acquire(CAPTURE);
        CountDownLatch stopped = new CountDownLatch(1);
        AtomicBoolean waitedThroughTheStop = new AtomicBoolean();
        CaptureClaimLease lease = new CaptureClaimLease(ownership, permit.claim(), Duration.ofMillis(10), () -> {
            try {
                Thread.sleep(20);
                waitedThroughTheStop.set(true);
            } catch (InterruptedException cutShort) {
                Thread.currentThread().interrupt();
            }
            stopped.countDown();
        });
        try {
            assertThat(stopped.await(5, TimeUnit.SECONDS)).as("the refused renewal stops the capture").isTrue();
            assertThat(waitedThroughTheStop)
                    .as("a wait on the way out of the stop runs to its end rather than being interrupted")
                    .isTrue();
        } finally {
            lease.close();
        }
    }

    private static final CaptureId CAPTURE = CaptureId.of(
            new CaptureConfig("mysql", Map.of("host", "db.internal"), List.of("orders")), null);

    private static ClusterMembershipGate eligibleGate() {
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        ClusterMembershipGate gate = new ClusterMembershipGate(properties);
        gate.install(new ClusterMembership("cluster-a", 3, Set.of("node-a", "node-b", "node-c")));
        gate.canCommit(Set.of("node-a", "node-b"));
        return gate;
    }

    private static final class RefusingRenewals implements WorkloadClaimStore {

        /** Nothing here reads the lease; it answers with a live one so a read is not a lapsed claim. */
        private static final Duration LIVE_LEASE = Duration.ofSeconds(30);
        private WorkloadClaim claim;
        private int releases;

        @Override
        public WorkloadClaimAttempt acquire(
                WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
            claim = new WorkloadClaim(key, owner, 1, 0, topologyRevision, Instant.now().plus(ttl));
            return WorkloadClaimAttempt.acquired(claim);
        }

        @Override
        public Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
            return Optional.empty();
        }

        @Override
        public boolean release(WorkloadClaim expected) {
            releases++;
            return true;
        }

        @Override
        public Optional<WorkloadClaim> advanceExecution(
                WorkloadClaim expected, long topologyRevision, java.util.Set<String> executionNodeIds) {
            return Optional.empty();
        }

        @Override
        public Optional<WorkloadClaim> recordExecutionFailure(
                WorkloadClaim expected, boolean afterMemberLoss) {
            return Optional.empty();
        }

        @Override
        public Optional<WorkloadClaimReading> read(WorkloadClaimKey key) {
            return Optional.ofNullable(claim).map(held -> new WorkloadClaimReading(held, LIVE_LEASE));
        }
    }
}
