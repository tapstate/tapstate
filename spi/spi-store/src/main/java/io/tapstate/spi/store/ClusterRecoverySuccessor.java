package io.tapstate.spi.store;

import java.time.Instant;
import java.util.Objects;
import java.util.Set;

/** The existing allocator's real successor, retained through coordinator and pipeline-owner changes. */
public record ClusterRecoverySuccessor(
        WorkloadClaimFence pipelineClaim, ClusterExecutionProfile profile,
        Set<String> executionNodeIds, Instant allocatedAt,
        String nativeJobId, Instant submittedAt, ClusterRecoveryStartupReceipt startupReceipt) {
    public ClusterRecoverySuccessor {
        pipelineClaim = Objects.requireNonNull(pipelineClaim, "pipelineClaim");
        profile = Objects.requireNonNull(profile, "profile");
        executionNodeIds = Set.copyOf(Objects.requireNonNull(executionNodeIds, "executionNodeIds"));
        allocatedAt = Objects.requireNonNull(allocatedAt, "allocatedAt");
        if (pipelineClaim.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || pipelineClaim.executionGeneration() < 1 || executionNodeIds.isEmpty()
                || executionNodeIds.stream().anyMatch(String::isBlank)
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
    }

    public long executionGeneration() {
        return pipelineClaim.executionGeneration();
    }
}
