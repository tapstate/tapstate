package io.tapstate.spi.store;

import java.time.Instant;
import java.util.Objects;

/** Durable owner lease and its monotonic claim/execution generations. */
public record WorkloadClaim(
        WorkloadClaimKey key,
        WorkloadOwner owner,
        long claimGeneration,
        long executionGeneration,
        long topologyRevision,
        Instant leaseUntil) {

    public WorkloadClaim {
        key = Objects.requireNonNull(key, "key");
        owner = Objects.requireNonNull(owner, "owner");
        leaseUntil = Objects.requireNonNull(leaseUntil, "leaseUntil");
        if (claimGeneration < 1 || executionGeneration < 0 || topologyRevision < 0) {
            throw new IllegalArgumentException("claim generations and topology revision are out of range");
        }
    }
}
