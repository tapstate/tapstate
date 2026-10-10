package io.tapstate.spi.store;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** Native initialization and acceptance of the original durable positions for one exact execution. */
public record ClusterRecoveryStartupReceipt(
        WorkloadClaimFence pipelineClaim, String nativeJobId, Instant nativeInitializedAt,
        Map<String, ClusterRecoveryPosition> acceptedPositions, Instant positionsAcceptedAt,
        boolean executionCompleted) {
    public ClusterRecoveryStartupReceipt {
        pipelineClaim = Objects.requireNonNull(pipelineClaim, "pipelineClaim");
        nativeJobId = ClusterRecoveryKey.required(nativeJobId, "nativeJobId");
        nativeInitializedAt = Objects.requireNonNull(nativeInitializedAt, "nativeInitializedAt");
        acceptedPositions = Map.copyOf(Objects.requireNonNull(acceptedPositions, "acceptedPositions"));
        positionsAcceptedAt = Objects.requireNonNull(positionsAcceptedAt, "positionsAcceptedAt");
        if (pipelineClaim.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || pipelineClaim.profileGeneration() < 1
                || pipelineClaim.executionGeneration() < 1) {
            throw new IllegalArgumentException("startup receipt requires an exact pipeline execution claim");
        }
    }
}
