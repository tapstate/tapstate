package io.tapstate.spi.store;

import java.time.Instant;
import java.util.Objects;

/** Immutable source facts committed before physical open for one exact successor execution. */
public record CaptureResumePreparation(WorkloadClaimFence pipelineClaim, CaptureResumeWitness witness,
        ClusterRecoveryPosition requestedPosition, Instant preparedAt, java.util.Set<String> requiredSourceIds) {
    public CaptureResumePreparation(WorkloadClaimFence pipelineClaim, CaptureResumeWitness witness,
            ClusterRecoveryPosition requestedPosition, Instant preparedAt) {
        this(pipelineClaim, witness, requestedPosition, preparedAt, java.util.Set.of(witness.sourceId()));
    }
    public CaptureResumePreparation {
        Objects.requireNonNull(pipelineClaim, "pipelineClaim");
        Objects.requireNonNull(witness, "witness");
        Objects.requireNonNull(preparedAt, "preparedAt");
        requiredSourceIds = java.util.Set.copyOf(requiredSourceIds);
        if (!requiredSourceIds.contains(witness.sourceId())) {
            throw new IllegalArgumentException("prepared source must belong to its complete required selection");
        }
    }
}
