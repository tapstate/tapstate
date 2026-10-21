package io.tapstate.spi.store;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** Native initialization and qualified preparation/acceptance facts for this exact execution. */
public record ClusterRecoveryStartupReceipt(
        WorkloadClaimFence pipelineClaim, String nativeJobId, Instant nativeInitializedAt,
        Map<String, CaptureResumeWitness> preparedWitnesses,
        Map<String, ClusterRecoveryPosition> requestedPositions,
        Map<String, ClusterRecoveryPosition> acceptedPositions, Instant positionsAcceptedAt,
        boolean executionCompleted) {
    public ClusterRecoveryStartupReceipt {
        pipelineClaim = Objects.requireNonNull(pipelineClaim, "pipelineClaim");
        nativeJobId = ClusterRecoveryKey.required(nativeJobId, "nativeJobId");
        nativeInitializedAt = Objects.requireNonNull(nativeInitializedAt, "nativeInitializedAt");
        preparedWitnesses = Map.copyOf(Objects.requireNonNull(preparedWitnesses, "preparedWitnesses"));
        requestedPositions = Map.copyOf(Objects.requireNonNull(requestedPositions, "requestedPositions"));
        acceptedPositions = Map.copyOf(Objects.requireNonNull(acceptedPositions, "acceptedPositions"));
        positionsAcceptedAt = Objects.requireNonNull(positionsAcceptedAt, "positionsAcceptedAt");
        if (pipelineClaim.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || pipelineClaim.profileGeneration() < 1
                || pipelineClaim.executionGeneration() < 1) {
            throw new IllegalArgumentException("startup receipt requires an exact pipeline execution claim");
        }
        preparedWitnesses.forEach((id, witness) -> {
            if (!id.equals(witness.sourceId())) {
                throw new IllegalArgumentException("prepared witness keys must be source identities");
            }
        });
        for (Map<String, ClusterRecoveryPosition> positions : java.util.List.of(requestedPositions, acceptedPositions)) {
            positions.forEach((id, position) -> {
                if (!id.equals(position.sourceId())) {
                    throw new IllegalArgumentException("receipt position keys must be source identities");
                }
            });
        }
    }

    /** Older observations carry no prepared proof and cannot establish source acceptance. */
    public ClusterRecoveryStartupReceipt(WorkloadClaimFence pipelineClaim, String nativeJobId, Instant nativeInitializedAt,
            Map<String, ClusterRecoveryPosition> acceptedPositions, Instant positionsAcceptedAt, boolean executionCompleted) {
        this(pipelineClaim, nativeJobId, nativeInitializedAt, Map.of(), Map.of(), acceptedPositions,
                positionsAcceptedAt, executionCompleted);
    }
}
