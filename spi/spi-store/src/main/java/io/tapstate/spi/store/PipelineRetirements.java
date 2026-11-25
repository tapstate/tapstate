package io.tapstate.spi.store;

import java.util.Optional;

/** CONFIG lifecycle commands for deletion and its one retained result; no CACHE or connector IO in callbacks. */
public interface PipelineRetirements {
    PipelineRetirements guardedBy(Runnable requireActive);
    PipelineRetirementPreparation begin(PipelineRetirementRequest original);
    /** Begins only while the exact stopped successor intent remains current in the CONFIG transaction. */
    default PipelineRetirementPreparation begin(PipelineRetirementRequest original, PipelineIntentSnapshot expectedIntent) {
        throw new UnsupportedOperationException("the retirement store must support exact intent admission");
    }
    Optional<PipelineRetirementPreparation> pending(PipelineIntentScope scope,String operationId);
    void requirePending(PipelineRetirementPreparation original);
    PipelineRetirementResult complete(PipelineRetirementReceipt original);
    Optional<PipelineRetirementResult> completed(String pipelineId,String operationId);
}
