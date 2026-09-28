package io.tapstate.spi.store;

import java.time.Instant;
import java.util.Objects;
import java.util.Set;

/**
 * Durable owner lease, monotonic generations, and the last execution's failure context.
 *
 * <p>The execution context is valid only when {@code contextExecutionGeneration} matches the current
 * execution. Older writers can advance that generation without writing these newer fields; zero or a
 * mismatch means the context is unknown, never evidence that the failure was independent.
 * A zero {@code failureClaimGeneration} means no holder recorded the failure before a takeover.
 */
public record WorkloadClaim(
        WorkloadClaimKey key,
        WorkloadOwner owner,
        long claimGeneration,
        long executionGeneration,
        long topologyRevision,
        Instant leaseUntil,
        long contextExecutionGeneration,
        long executionClaimGeneration,
        Set<String> executionNodeIds,
        long failureClaimGeneration,
        boolean failureAfterMemberLoss) {

    public WorkloadClaim {
        key = Objects.requireNonNull(key, "key");
        owner = Objects.requireNonNull(owner, "owner");
        leaseUntil = Objects.requireNonNull(leaseUntil, "leaseUntil");
        executionNodeIds = Set.copyOf(Objects.requireNonNull(executionNodeIds, "executionNodeIds"));
        if (claimGeneration < 1 || executionGeneration < 0 || topologyRevision < 0
                || contextExecutionGeneration < 0 || executionClaimGeneration < 0
                || failureClaimGeneration < 0) {
            throw new IllegalArgumentException("claim generations and topology revision are out of range");
        }
    }

    /** Claims written before execution context was recorded have no context until their next run. */
    public WorkloadClaim(WorkloadClaimKey key, WorkloadOwner owner, long claimGeneration,
            long executionGeneration, long topologyRevision, Instant leaseUntil) {
        this(key, owner, claimGeneration, executionGeneration, topologyRevision, leaseUntil,
                0, 0, Set.of(), 0, false);
    }
}
