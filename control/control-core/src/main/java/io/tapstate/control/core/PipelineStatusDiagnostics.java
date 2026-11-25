package io.tapstate.control.core;

import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.PipelineIntentScope;
import java.util.Optional;

/** Read-only facts about the current engine run, independent of monitoring publication. */
@FunctionalInterface
public interface PipelineStatusDiagnostics {
    PipelineStatusDiagnostics NONE = (pipeline, observed) -> Optional.empty();
    Optional<ObservationFailure> failure(String pipelineId, PipelineState observedState);

    /** A current, fenced engine state; absence supplies no evidence of successful submission. */
    default Optional<PipelineState> currentState(String pipelineId, PipelineIntentScope scope) {
        return Optional.empty();
    }

    default java.util.Map<String, PipelineState> currentStates(java.util.Map<String, PipelineIntentScope> scopes) {
        var states = new java.util.HashMap<String, PipelineState>();
        scopes.forEach((id, scope) -> currentState(id, scope).ifPresent(state -> states.put(id, state)));
        return java.util.Map.copyOf(states);
    }
}
