package io.tapstate.spi.store;

/**
 * Opens Pipeline views under the original physical scope. Read views never create state; each write
 * view retains one immutable claim through all transaction retries and never resolves a newer claim.
 */
public interface PipelineStoreViews {
    StateStore pipelineStates(PipelineIntentScope scope);

    StateStore pipelineStates(PipelineIntentScope scope, WorkloadClaimFence original, Runnable requireActive);

    ObservationStore pipelineObservations(PipelineIntentScope scope);

    ObservationStore pipelineObservations(PipelineIntentScope scope, WorkloadClaimFence original, Runnable requireActive);
}
