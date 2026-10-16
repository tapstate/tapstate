package io.tapstate.spi.store;

import java.time.Instant;
import java.util.Objects;
import java.util.Map;
import java.util.Set;

/** The existing allocator's real successor, retained through coordinator and pipeline-owner changes. */
public record ClusterRecoverySuccessor(
        WorkloadClaimFence pipelineClaim, ClusterExecutionProfile profile,
        Set<String> executionNodeIds, Set<String> requiredSourceIds, boolean sourceRequirementsRecorded, Instant allocatedAt,
        String nativeJobId, Instant submittedAt, Map<String, ClusterRecoveryPosition> requestedPositions,
        ClusterRecoveryStartupReceipt startupReceipt, ClusterRecoveryFailureNote failureNote) {
    public ClusterRecoverySuccessor {
        pipelineClaim = Objects.requireNonNull(pipelineClaim, "pipelineClaim");
        profile = Objects.requireNonNull(profile, "profile");
        executionNodeIds = Set.copyOf(Objects.requireNonNull(executionNodeIds, "executionNodeIds"));
        requiredSourceIds = Set.copyOf(Objects.requireNonNull(requiredSourceIds, "requiredSourceIds"));
        requestedPositions = Map.copyOf(Objects.requireNonNull(requestedPositions, "requestedPositions"));
        allocatedAt = Objects.requireNonNull(allocatedAt, "allocatedAt");
        if (pipelineClaim.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || pipelineClaim.executionGeneration() < 1 || executionNodeIds.isEmpty()
                || executionNodeIds.stream().anyMatch(String::isBlank)
                || requiredSourceIds.stream().anyMatch(String::isBlank)
                || (!sourceRequirementsRecorded && !requiredSourceIds.isEmpty())
                || !pipelineClaim.key().clusterId().equals(profile.clusterId())
                || pipelineClaim.profileGeneration() != profile.generation()
                || (nativeJobId == null) != (submittedAt == null)) {
            throw new IllegalArgumentException("recovery successor identity or submission fields are invalid");
        }
        if (nativeJobId != null) {
            nativeJobId = ClusterRecoveryKey.required(nativeJobId, "nativeJobId");
        }
        if (startupReceipt != null && (nativeJobId == null
                || !pipelineClaim.equals(startupReceipt.pipelineClaim())
                || !nativeJobId.equals(startupReceipt.nativeJobId()))) {
            throw new IllegalArgumentException("startup receipt must match the submitted successor");
        }
        if (failureNote != null && !pipelineClaim.equals(failureNote.pipelineClaim())) {
            throw new IllegalArgumentException("failure note must match the allocated successor");
        }
    }

    public ClusterRecoverySuccessor(WorkloadClaimFence pipelineClaim, ClusterExecutionProfile profile,
            Set<String> executionNodeIds, Set<String> requiredSourceIds, boolean sourceRequirementsRecorded, Instant allocatedAt,
            String nativeJobId, Instant submittedAt, Map<String, ClusterRecoveryPosition> requestedPositions,
            ClusterRecoveryStartupReceipt startupReceipt) {
        this(pipelineClaim, profile, executionNodeIds, requiredSourceIds, sourceRequirementsRecorded, allocatedAt,
                nativeJobId, submittedAt, requestedPositions, startupReceipt, null);
    }

    public ClusterRecoverySuccessor(WorkloadClaimFence pipelineClaim, ClusterExecutionProfile profile,
            Set<String> executionNodeIds, Set<String> requiredSourceIds, Instant allocatedAt,
            String nativeJobId, Instant submittedAt, Map<String, ClusterRecoveryPosition> requestedPositions,
            ClusterRecoveryStartupReceipt startupReceipt) {
        this(pipelineClaim, profile, executionNodeIds, requiredSourceIds, true, allocatedAt,
                nativeJobId, submittedAt, requestedPositions, startupReceipt);
    }

    /** Unknown source requirements in an older durable step are never inferred from archived positions. */
    public ClusterRecoverySuccessor(WorkloadClaimFence pipelineClaim, ClusterExecutionProfile profile,
            Set<String> executionNodeIds, Instant allocatedAt, String nativeJobId, Instant submittedAt,
            ClusterRecoveryStartupReceipt startupReceipt) {
        this(pipelineClaim, profile, executionNodeIds, Set.of(), false, allocatedAt, nativeJobId, submittedAt, Map.of(), startupReceipt);
    }

    public long executionGeneration() {
        return pipelineClaim.executionGeneration();
    }

    /** Structural proof shape only; adapters additionally consume the real prepared source records. */
    public boolean matchesStartup(ClusterRecoveryStartupReceipt receipt) {
        return receipt != null && failureNote == null && sourceRequirementsRecorded && pipelineClaim.equals(receipt.pipelineClaim())
                && Objects.equals(nativeJobId, receipt.nativeJobId())
                && requiredSourceIds.equals(receipt.preparedWitnesses().keySet())
                && requiredSourceIds.equals(receipt.requestedPositions().keySet())
                && requiredSourceIds.equals(receipt.acceptedPositions().keySet())
                && receipt.requestedPositions().equals(receipt.acceptedPositions())
                && (requestedPositions.isEmpty() || requestedPositions.equals(receipt.requestedPositions()));
    }
}
