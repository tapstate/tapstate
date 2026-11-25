package io.tapstate.spi.store;

import java.util.Objects;
import java.util.UUID;

/** One current physical generation's retained initial-checkpoint preparation facts. */
public record PipelinePreparation(String pipelineId, long stateGeneration, Phase phase,
        PipelineIntentScope scope, String preparationId, WorkloadClaimFence issuerNode) {
    public enum Phase { UNASSIGNED, ISSUED, COMPLETE }

    public PipelinePreparation {
        Objects.requireNonNull(pipelineId, "pipelineId"); Objects.requireNonNull(phase, "phase");
        if (pipelineId.isBlank() || stateGeneration < 1 || stateGeneration == Long.MAX_VALUE) {
            throw new IllegalArgumentException("invalid Pipeline preparation identity");
        }
        if (phase == Phase.UNASSIGNED) {
            if (scope != null || preparationId != null || issuerNode != null) {
                throw new IllegalArgumentException("unassigned preparation has no issued authority");
            }
        } else {
            Objects.requireNonNull(scope, "scope"); Objects.requireNonNull(preparationId, "preparationId");
            Objects.requireNonNull(issuerNode, "issuerNode");
            if (!scope.pipelineId().equals(pipelineId) || scope.stateGeneration() != stateGeneration
                    || !UUID.fromString(preparationId).toString().equals(preparationId)
                    || issuerNode.key().type() != WorkloadClaimType.NODE_SESSION
                    || !scope.clusterId().equals(issuerNode.key().clusterId())
                    || !issuerNode.key().resourceId().equals(issuerNode.owner().nodeId())) {
                throw new IllegalArgumentException("issued preparation retains its exact scope and original node");
            }
        }
    }
}
