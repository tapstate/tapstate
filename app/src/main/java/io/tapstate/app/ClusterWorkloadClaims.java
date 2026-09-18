package io.tapstate.app;

import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/** Business-claim facade that refuses acquire/renew before the local committed-membership gate qualifies. */
final class ClusterWorkloadClaims {

    private final WorkloadClaimStore store;
    private final ClusterMembershipGate membership;

    ClusterWorkloadClaims(WorkloadClaimStore store, ClusterMembershipGate membership) {
        this.store = Objects.requireNonNull(store, "store");
        this.membership = Objects.requireNonNull(membership, "membership");
    }

    Optional<WorkloadClaimAttempt> acquire(
            WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
        if (key.type() != WorkloadClaimType.NODE_SESSION && !membership.businessEligible()) {
            return Optional.empty();
        }
        return Optional.of(store.acquire(key, owner, topologyRevision, ttl));
    }

    Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
        if (expected.key().type() != WorkloadClaimType.NODE_SESSION && !membership.businessEligible()) {
            return Optional.empty();
        }
        return store.renew(expected, ttl);
    }

    boolean release(WorkloadClaim expected) {
        return store.release(expected);
    }

    Optional<WorkloadClaim> advanceExecution(WorkloadClaim expected, long topologyRevision) {
        if (!membership.businessEligible()) {
            return Optional.empty();
        }
        return store.advanceExecution(expected, topologyRevision);
    }
}
