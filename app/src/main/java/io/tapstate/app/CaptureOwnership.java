package io.tapstate.app;

import io.tapstate.runtime.srs.CaptureId;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/** Acquires and maintains CAPTURE claims without confusing them with member or pipeline ownership. */
final class CaptureOwnership {

    record Permit(boolean acquired, WorkloadClaim claim) {

        static Permit unfenced() {
            return new Permit(true, null);
        }

        static Permit denied() {
            return new Permit(false, null);
        }

        WorkloadClaimFence fence() {
            return claim == null ? null : WorkloadClaimFence.from(claim);
        }
    }

    private final String clusterId;
    private final WorkloadOwner owner;
    private final ClusterMembershipGate membership;
    private final ClusterWorkloadClaims claims;
    private final Duration ttl;
    private final boolean fenced;

    private CaptureOwnership() {
        this.clusterId = "single";
        this.owner = null;
        this.membership = null;
        this.claims = null;
        this.ttl = Duration.ZERO;
        this.fenced = false;
    }

    static CaptureOwnership single() {
        return new CaptureOwnership();
    }

    CaptureOwnership(
            String clusterId,
            WorkloadOwner owner,
            ClusterMembershipGate membership,
            ClusterWorkloadClaims claims,
            Duration ttl) {
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
        this.owner = Objects.requireNonNull(owner, "owner");
        this.membership = Objects.requireNonNull(membership, "membership");
        this.claims = Objects.requireNonNull(claims, "claims");
        this.ttl = Objects.requireNonNull(ttl, "ttl");
        this.fenced = true;
    }

    Permit acquire(CaptureId captureId) {
        Objects.requireNonNull(captureId, "captureId");
        if (!fenced) {
            return Permit.unfenced();
        }
        ClusterMembership current = membership.committed();
        if (current == null) {
            return Permit.denied();
        }
        WorkloadClaimKey key =
                new WorkloadClaimKey(clusterId, WorkloadClaimType.CAPTURE, captureId.value());
        return claims.acquire(key, owner, current.revision(), ttl)
                .filter(attempt -> attempt.acquired() && attempt.claim().owner().equals(owner))
                .map(attempt -> new Permit(true, attempt.claim()))
                .orElseGet(Permit::denied);
    }

    Optional<WorkloadClaim> renew(WorkloadClaim expected) {
        if (!fenced || expected == null) {
            return Optional.ofNullable(expected);
        }
        return claims.renew(expected, ttl);
    }

    void release(WorkloadClaim expected) {
        if (fenced && expected != null) {
            claims.release(expected);
        }
    }

    /**
     * How long a claim outlives the member holding it: the lease a holder that stops renewing leaves
     * behind, and so how long it can take a claim to become free to anybody else.
     */
    Duration ttl() {
        return ttl;
    }
}
