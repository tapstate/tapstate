package io.tapstate.app;

import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterWorkloadClaimsTest {

    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-1");
    private static final Duration TTL = Duration.ofSeconds(30);

    @Test
    void businessAcquireAndRenewNeverReachTheStoreWithoutTheCommittedMajority() {
        ClusterMembershipGate gate = productionGate();
        gate.install(new ClusterMembership("cluster-a", 1, Set.of("a", "b", "c", "d")));
        gate.canCommit(Set.of("a", "b"));
        RecordingClaims raw = new RecordingClaims();
        ClusterWorkloadClaims claims = new ClusterWorkloadClaims(raw, gate);
        WorkloadClaimKey key =
                new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
        WorkloadClaim expected = raw.claim(key);

        assertThat(claims.acquire(key, OWNER, 1, TTL)).isEmpty();
        assertThat(claims.renew(expected, TTL)).isEmpty();
        assertThat(raw.calls).hasValue(0);

        gate.canCommit(Set.of("a", "b", "c"));
        assertThat(claims.acquire(key, OWNER, 1, TTL)).isPresent();
        assertThat(claims.renew(expected, TTL)).isPresent();
        assertThat(raw.calls).hasValue(2);
    }

    @Test
    void preJoinNodeSessionDoesNotDependOnAMembershipThatCannotExistYet() {
        ClusterMembershipGate gate = productionGate();
        RecordingClaims raw = new RecordingClaims();
        ClusterWorkloadClaims claims = new ClusterWorkloadClaims(raw, gate);
        WorkloadClaimKey key =
                new WorkloadClaimKey("cluster-a", WorkloadClaimType.NODE_SESSION, "node-a");

        assertThat(claims.acquire(key, OWNER, 0, TTL)).isPresent();
        assertThat(raw.calls).hasValue(1);
    }

    @Test
    void aTopologyRevisionChangeFencesRenewalUntilTheWorkloadIsReacquired() {
        ClusterMembershipGate gate = productionGate();
        gate.install(new ClusterMembership("cluster-a", 1, Set.of("a", "b", "c")));
        gate.canCommit(Set.of("a", "b"));
        RecordingClaims raw = new RecordingClaims();
        ClusterWorkloadClaims claims = new ClusterWorkloadClaims(raw, gate);
        WorkloadClaimKey key = new WorkloadClaimKey("cluster-a", WorkloadClaimType.CAPTURE, "capture-a");
        WorkloadClaim revisionOne = raw.claim(key);

        gate.install(new ClusterMembership("cluster-a", 2, Set.of("a", "b", "c")));
        gate.canCommit(Set.of("a", "b"));

        assertThat(claims.renew(revisionOne, TTL)).isEmpty();
        assertThat(raw.calls).hasValue(0);
    }

    private static ClusterMembershipGate productionGate() {
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        return new ClusterMembershipGate(properties);
    }

    private static final class RecordingClaims implements WorkloadClaimStore {
        private final AtomicInteger calls = new AtomicInteger();

        private WorkloadClaim claim(WorkloadClaimKey key) {
            return new WorkloadClaim(key, OWNER, 1, 0, 1, Instant.now().plus(TTL));
        }

        @Override
        public WorkloadClaimAttempt acquire(
                WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
            calls.incrementAndGet();
            return WorkloadClaimAttempt.acquired(claim(key));
        }

        @Override
        public Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
            calls.incrementAndGet();
            return Optional.of(expected);
        }

        @Override
        public boolean release(WorkloadClaim expected) {
            calls.incrementAndGet();
            return true;
        }

        @Override
        public Optional<WorkloadClaim> advanceExecution(WorkloadClaim expected, long topologyRevision) {
            calls.incrementAndGet();
            return Optional.of(expected);
        }

        @Override
        public Optional<WorkloadClaim> read(WorkloadClaimKey key) {
            return Optional.of(claim(key));
        }
    }
}
