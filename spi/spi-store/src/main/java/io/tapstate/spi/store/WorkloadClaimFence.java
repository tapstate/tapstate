package io.tapstate.spi.store;

import java.util.Objects;

/**
 * Immutable expected owner and generations carried to a store-side side-effect fence.
 * It intentionally omits the client-observed lease deadline: the store compares its own current time and
 * current claim document atomically with the side-effect write.
 */
public record WorkloadClaimFence(
        WorkloadClaimKey key,
        WorkloadOwner owner,
        long claimGeneration,
        long executionGeneration,
        long topologyRevision) {

    public WorkloadClaimFence {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(owner, "owner");
        if (claimGeneration < 1 || executionGeneration < 0 || topologyRevision < 0) {
            throw new IllegalArgumentException("claim fence generations and topology revision are out of range");
        }
    }

    public static WorkloadClaimFence from(WorkloadClaim claim) {
        Objects.requireNonNull(claim, "claim");
        return new WorkloadClaimFence(
                claim.key(), claim.owner(), claim.claimGeneration(),
                claim.executionGeneration(), claim.topologyRevision());
    }
}
