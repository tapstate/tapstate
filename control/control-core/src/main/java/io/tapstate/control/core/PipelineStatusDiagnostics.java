package io.tapstate.control.core;

import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import java.util.Optional;

/** A read-only, independently proved failure of the current run; never a stored observation. */
@FunctionalInterface
public interface PipelineStatusDiagnostics {
    PipelineStatusDiagnostics NONE = (pipeline, observed) -> Optional.empty();
    Optional<ObservationFailure> failure(String pipelineId, PipelineState observedState);
}
