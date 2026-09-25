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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class CaptureClaimLeaseTest {

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
        public Optional<WorkloadClaim> advanceUnderClaim(WorkloadClaim expected, long topologyRevision) {
            return Optional.empty();
        }

        @Override
        public Optional<WorkloadClaimReading> read(WorkloadClaimKey key) {
            return Optional.ofNullable(claim).map(held -> new WorkloadClaimReading(held, LIVE_LEASE));
        }
    }
}
