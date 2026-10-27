package io.tapstate.spi.store;

import java.util.Objects;

/** The retained physical state identity of a Pipeline, independent of execution and snapshot generations. */
public record PipelineStateMapping(String pipelineId, long stateGeneration, Phase phase, String operationId) {
    public enum Phase { ACTIVE, REBUILDING, RETIRING, RETIRED }

    public PipelineStateMapping {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(phase, "phase");
        if (pipelineId.isBlank() || stateGeneration < 1 || stateGeneration == Long.MAX_VALUE
                || (operationId != null && operationId.isBlank()) || (phase != Phase.ACTIVE && operationId == null)) {
            throw new IllegalArgumentException("invalid Pipeline state mapping");
        }
    }
}
