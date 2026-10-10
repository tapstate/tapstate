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
        boolean failureAfterMemberLoss,
        long profileGeneration,
        ClusterExecutionProfile executionProfile,
        Long executionTopologyRevision,
        String executionIncarnation,
        String executionRevision) {

    public WorkloadClaim {
        key = Objects.requireNonNull(key, "key");
        owner = Objects.requireNonNull(owner, "owner");
        leaseUntil = Objects.requireNonNull(leaseUntil, "leaseUntil");
        executionNodeIds = Set.copyOf(Objects.requireNonNull(executionNodeIds, "executionNodeIds"));
        if (claimGeneration < 1 || executionGeneration < 0 || topologyRevision < 0
                || contextExecutionGeneration < 0 || executionClaimGeneration < 0
                || failureClaimGeneration < 0 || profileGeneration < 0) {
            throw new IllegalArgumentException("claim generations and topology revision are out of range");
        }
        if (executionProfile != null && !key.clusterId().equals(executionProfile.clusterId())) {
            throw new IllegalArgumentException("execution profile must belong to the claim's cluster");
        }
        if (executionTopologyRevision != null && executionTopologyRevision < 1) {
            throw new IllegalArgumentException("execution topology revision must be positive when known");
        }
    }

    public WorkloadClaim(WorkloadClaimKey key, WorkloadOwner owner, long claimGeneration,
            long executionGeneration, long topologyRevision, Instant leaseUntil,
            long contextExecutionGeneration, long executionClaimGeneration, Set<String> executionNodeIds,
            long failureClaimGeneration, boolean failureAfterMemberLoss, long profileGeneration,
            ClusterExecutionProfile executionProfile, Long executionTopologyRevision) {
        this(key, owner, claimGeneration, executionGeneration, topologyRevision, leaseUntil,
                contextExecutionGeneration, executionClaimGeneration, executionNodeIds,
                failureClaimGeneration, failureAfterMemberLoss, profileGeneration, executionProfile,
                executionTopologyRevision, null, null);
    }

    public WorkloadClaim(WorkloadClaimKey key, WorkloadOwner owner, long claimGeneration,
            long executionGeneration, long topologyRevision, Instant leaseUntil,
            long contextExecutionGeneration, long executionClaimGeneration, Set<String> executionNodeIds,
            long failureClaimGeneration, boolean failureAfterMemberLoss, long profileGeneration,
            ClusterExecutionProfile executionProfile) {
        this(key, owner, claimGeneration, executionGeneration, topologyRevision, leaseUntil,
                contextExecutionGeneration, executionClaimGeneration, executionNodeIds,
                failureClaimGeneration, failureAfterMemberLoss, profileGeneration, executionProfile, null);
    }

    public WorkloadClaim(WorkloadClaimKey key, WorkloadOwner owner, long claimGeneration,
            long executionGeneration, long topologyRevision, Instant leaseUntil,
            long contextExecutionGeneration, long executionClaimGeneration, Set<String> executionNodeIds,
            long failureClaimGeneration, boolean failureAfterMemberLoss, long profileGeneration) {
        this(key, owner, claimGeneration, executionGeneration, topologyRevision, leaseUntil,
                contextExecutionGeneration, executionClaimGeneration, executionNodeIds,
                failureClaimGeneration, failureAfterMemberLoss, profileGeneration, null);
    }

    /** Legacy protocol claims carry no execution-profile fence. */
    public WorkloadClaim(WorkloadClaimKey key, WorkloadOwner owner, long claimGeneration,
            long executionGeneration, long topologyRevision, Instant leaseUntil,
            long contextExecutionGeneration, long executionClaimGeneration, Set<String> executionNodeIds,
            long failureClaimGeneration, boolean failureAfterMemberLoss) {
        this(key, owner, claimGeneration, executionGeneration, topologyRevision, leaseUntil,
                contextExecutionGeneration, executionClaimGeneration, executionNodeIds,
                failureClaimGeneration, failureAfterMemberLoss, 0);
    }

    /** Claims written before execution context was recorded have no context until their next run. */
    public WorkloadClaim(WorkloadClaimKey key, WorkloadOwner owner, long claimGeneration,
            long executionGeneration, long topologyRevision, Instant leaseUntil) {
        this(key, owner, claimGeneration, executionGeneration, topologyRevision, leaseUntil,
                0, 0, Set.of(), 0, false, 0);
    }
}
