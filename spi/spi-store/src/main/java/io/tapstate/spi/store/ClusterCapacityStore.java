package io.tapstate.spi.store;

import io.tapstate.core.lifecycle.ClusterCapacityDemand;
import io.tapstate.core.lifecycle.ClusterCapacityLimits;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Shared atomic occupancy for ordinary starts and recovery; every write validates real authority. */
public interface ClusterCapacityStore {

    /**
     * Reserves computable demand under exact current artifact incarnation, complete desired intent,
     * profile and pipeline claim. Existing live execution occupancy and unexpired reservations share
     * one per-member budget. The full member set must be ACTIVE and compatible; unknown demand is
     * refused rather than counted as zero. A repeated exact intent returns the same reservation.
     */
    Result reserve(WorkloadClaim expectedPipelineClaim, ClusterExecutionProfile profile,
            String incarnationId, String intentFingerprint, Map<String, ClusterCapacityDemand> demandByNode,
            ClusterCapacityLimits limits, Duration ttl);

    /** Records the successor with the existing allocator in the same transaction as this reservation. */
    Result advanceExecution(ClusterCapacityReservation expected, WorkloadClaim expectedPipelineClaim,
            Set<String> executionNodeIds);

    /** Moves reserved demand to the matching execution's occupancy atomically, without a missing interval. */
    Result submitted(ClusterCapacityReservation expected, WorkloadClaimFence pipelineClaim, String nativeJobId);

    /**
     * Releases only after the exact recorded generation/owner no longer authorizes effects. Expiration
     * of a reservation alone does not authorize releasing an execution which can still call a target.
     */
    Result release(ClusterCapacityReservation expected);

    /** Every known occupied resource on each node, without cleanup or authority mutations. */
    Map<String, ClusterCapacityDemand> occupied(String clusterId);

    /** The profile and demand from one read-only snapshot; unknown demand remains a coded refusal. */
    default Optional<Snapshot> readOccupied(String clusterId) {
        return Optional.empty();
    }

    record Snapshot(ClusterExecutionProfile profile, Map<String, ClusterCapacityDemand> occupiedByNode) {
        public Snapshot {
            profile = Objects.requireNonNull(profile, "profile");
            occupiedByNode = Map.copyOf(Objects.requireNonNull(occupiedByNode, "occupiedByNode"));
        }
    }

    enum Outcome {
        APPLIED, ALREADY_RESERVED, CAPACITY_REFUSED, UNKNOWN_DEMAND,
        STALE_CLAIM, STALE_PROFILE, STALE_INTENT, STALE_EXECUTION, WAITING_QUORUM
    }

    record Result(Outcome outcome, ClusterCapacityReservation reservation,
            WorkloadClaim advancedPipelineClaim, List<ClusterCapacityLimits.Violation> violations, String refusedNode) {
        public Result {
            Objects.requireNonNull(outcome, "outcome");
            violations = List.copyOf(Objects.requireNonNull(violations, "violations"));
            if (advancedPipelineClaim != null && (reservation == null || reservation.executionGeneration() == null
                    || reservation.executionGeneration() != advancedPipelineClaim.executionGeneration())) {
                throw new IllegalArgumentException("advanced generation must match its capacity receipt");
            }
        }

        public Result(Outcome outcome, ClusterCapacityReservation reservation,
                WorkloadClaim advancedPipelineClaim, List<ClusterCapacityLimits.Violation> violations) {
            this(outcome, reservation, advancedPipelineClaim, violations, null);
        }
    }
}
