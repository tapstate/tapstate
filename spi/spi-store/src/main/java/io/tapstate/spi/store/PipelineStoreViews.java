package io.tapstate.spi.store;

/**
 * Opens Pipeline views under the original physical scope. Read views never create state; each write
 * view retains one immutable claim through all transaction retries and never resolves a newer claim.
 */
public interface PipelineStoreViews {
    default DerivedSchemaStore pipelineDerivedSchemas(PipelineIntentScope scope) { throw unsupported(); }

    default DerivedSchemaStore pipelineDerivedSchemas(PipelineIntentScope scope, WorkloadClaimFence original, Runnable requireActive) { throw unsupported(); }

    private static io.tapstate.core.common.TapstateException unsupported() {
        return new io.tapstate.core.common.TapstateException(IoError.WORKLOAD_CLAIM_FENCED, java.util.Map.of(), null);
    }

    StateStore pipelineStates(PipelineIntentScope scope);

    StateStore pipelineStates(PipelineIntentScope scope, WorkloadClaimFence original, Runnable requireActive);

    ObservationStore pipelineObservations(PipelineIntentScope scope);

    ObservationStore pipelineObservations(PipelineIntentScope scope, WorkloadClaimFence original, Runnable requireActive);
}
