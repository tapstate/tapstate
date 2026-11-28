package io.tapstate.control.core;

import io.tapstate.core.lifecycle.PipelineRunProgress;
import java.util.Optional;

/** Read the latest run evidence within the active runtime storage scope. */
@FunctionalInterface
public interface PipelineRunProgressQuery {
    record Read(PipelineRunProgress progress, boolean awaitingMilestones) {}

    Optional<Read> find(String pipelineId);
}
