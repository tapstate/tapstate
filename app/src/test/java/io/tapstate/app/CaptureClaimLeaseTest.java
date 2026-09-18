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
        public Optional<WorkloadClaim> advanceExecution(WorkloadClaim expected, long topologyRevision) {
            return Optional.empty();
        }

        @Override
        public Optional<WorkloadClaimReading> read(WorkloadClaimKey key) {
            return Optional.ofNullable(claim).map(held -> new WorkloadClaimReading(held, LIVE_LEASE));
        }
    }
}
