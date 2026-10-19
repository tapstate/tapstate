package io.tapstate.spi.store;

import java.util.Objects;

/** Immutable confirmation inputs for one existing Pipeline state generation and its exact successor. */
public record PipelineRebuildRequest(String operationId, PipelineIntentSnapshot intent,
        WorkloadClaimFence originalClaim, long originalStateEpoch, PipelinePreparationInputs inputs) {
    public PipelineRebuildRequest {
        Objects.requireNonNull(operationId, "operationId"); Objects.requireNonNull(intent, "intent");
        Objects.requireNonNull(originalClaim, "originalClaim"); Objects.requireNonNull(inputs, "inputs");
        if (operationId.isBlank() || intent.deleted() || originalStateEpoch < 0 || originalStateEpoch >= Long.MAX_VALUE - 1
                || intent.scope().stateGeneration() >= Long.MAX_VALUE - 1
                || originalClaim.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || !intent.scope().clusterId().equals(originalClaim.key().clusterId())
                || !intent.scope().pipelineId().equals(originalClaim.key().resourceId()))
            throw new IllegalArgumentException("invalid original Pipeline rebuild request");
        var scope = successor(intent.scope());
        if (inputs.directChannels().stream().anyMatch(channel -> !scope.equals(channel.context().scope())))
            throw new IllegalArgumentException("rebuild inputs must name the fixed successor scope");
    }
    public PipelineIntentScope originalScope() { return intent.scope(); }
    public PipelineIntentScope targetScope() { return successor(intent.scope()); }
    private static PipelineIntentScope successor(PipelineIntentScope old) {
        return new PipelineIntentScope(old.clusterId(), old.bindingVersion(), old.cacheIncarnation(), old.pipelineId(), old.stateGeneration() + 1);
    }
}
