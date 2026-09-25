package io.tapstate.app;

import io.tapstate.spi.store.ClusterMembership;
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

    /**
     * Renews a claim this member holds, while the cluster still lets it.
     *
     * <p>Every business claim needs the committed majority to renew. Most also need the topology they were
     * granted under to still be the committed one: their work is planned over the members, so a changed
     * membership has them prove themselves again, and their holder takes the claim once more under the new
     * revision on its next pass -- the same owner, so the same generation.
     *
     * <p>A capture claim is held to the majority alone. It decides which member tails a source, which a
     * member joining does not change -- and only a join moves the revision, since departures are never
     * committed. Its holder also has no next pass to take it again on: losing it stops the tail it guards.
     * Fencing it on the revision stopped every tail in the cluster within one renewal of any member joining,
     * and left the pipelines reading them failed.
     */
    Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
        WorkloadClaimType type = expected.key().type();
        if (type != WorkloadClaimType.NODE_SESSION) {
            if (!membership.businessEligible()) {
                return Optional.empty();
            }
            ClusterMembership current = membership.committed();
            if (type != WorkloadClaimType.CAPTURE
                    && (current == null || current.revision() != expected.topologyRevision())) {
                return Optional.empty();
            }
        }
        return store.renew(expected, ttl);
    }

    boolean release(WorkloadClaim expected) {
        return store.release(expected);
    }

    Optional<WorkloadClaim> advanceUnderClaim(WorkloadClaim expected, long topologyRevision) {
        if (!membership.businessEligible()) {
            return Optional.empty();
        }
        return store.advanceUnderClaim(expected, topologyRevision);
    }
}
