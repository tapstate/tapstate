package io.tapstate.control.core;

import io.tapstate.core.lifecycle.ExecutionPlan;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import java.time.Instant;
import java.util.Objects;

/**
 * The status read face: a pipeline's lifecycle state, the small stable observation dataset, plus the
 * coded reason it died when that state is a failure. Carries no counts / rates / snapshot progress, so
 * the status contract evolves with the state machine, not with the growing metric set — the failure
 * belongs here because it qualifies the state rather than measuring the run: a failed state that cannot
 * say what failed is only half an answer.
 *
 * <p>{@code observedAt} says when the projection this reads was taken, or {@code null} when that is not
 * known. It qualifies the state the same way the failure does: a state with no time against it cannot tell
 * a run that simply has not changed from one whose publisher stopped.
 *
 * <p>{@code plan} is the plan the pipeline's current run was submitted on - how wide each node runs and why - or
 * {@code null} when no run has one recorded. It qualifies the run the state describes rather than measuring it,
 * and is replaced only when a new run is submitted.
 */
public record PipelineStatus(String pipelineId, PipelineState state, ObservationFailure failure,
        Instant observedAt, ExecutionPlan plan) {

    public PipelineStatus {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(state, "state");
    }

    /** A status of a run with no plan recorded. */
    public PipelineStatus(String pipelineId, PipelineState state, ObservationFailure failure, Instant observedAt) {
        this(pipelineId, state, failure, observedAt, null);
    }

    /** A status carrying no observation time — the shape callers used before the projection recorded one. */
    public PipelineStatus(String pipelineId, PipelineState state, ObservationFailure failure) {
        this(pipelineId, state, failure, null);
    }

    /** A status with nothing wrong to report — the shape every healthy read takes. */
    public PipelineStatus(String pipelineId, PipelineState state) {
        this(pipelineId, state, null, null);
    }
}
