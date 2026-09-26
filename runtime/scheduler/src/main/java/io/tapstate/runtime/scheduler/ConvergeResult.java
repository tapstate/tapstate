package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.PipelineState;

import java.util.Objects;
import java.util.Optional;

/**
 * The result of one convergence pass: its {@link ConvergeStatus}, the resulting checkpoint when the
 * pass ended at a state, and the job failure cause when the pass drove a dead job to FAILED. The
 * checkpoint is present for converged, failed and deferred starts; the
 * failure is present only for {@link ConvergeStatus#FAILED}. A successful checkpoint transition
 * carries the state it moved from, so a delayed observer need not guess the preceding state from a
 * later checkpoint or the state last seen by this process.
 */
public record ConvergeResult(
        ConvergeStatus status, Optional<CheckpointDoc> checkpoint, Optional<Throwable> failure,
        Optional<PipelineState> transitionFrom) {

    public ConvergeResult {
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(checkpoint, "checkpoint");
        Objects.requireNonNull(failure, "failure");
        Objects.requireNonNull(transitionFrom, "transitionFrom");
    }

    /** Existing result construction with no newly applied state transition. */
    public ConvergeResult(ConvergeStatus status, Optional<CheckpointDoc> checkpoint,
            Optional<Throwable> failure) {
        this(status, checkpoint, failure, Optional.empty());
    }

    static ConvergeResult converged(CheckpointDoc checkpoint) {
        return new ConvergeResult(ConvergeStatus.CONVERGED, Optional.of(checkpoint), Optional.empty());
    }

    static ConvergeResult converged(CheckpointDoc checkpoint, PipelineState from) {
        return new ConvergeResult(ConvergeStatus.CONVERGED, Optional.of(checkpoint),
                Optional.empty(), Optional.of(from));
    }

    static ConvergeResult nothingToDo() {
        return new ConvergeResult(ConvergeStatus.NOTHING_TO_DO, Optional.empty(), Optional.empty());
    }

    static ConvergeResult superseded() {
        return new ConvergeResult(ConvergeStatus.SUPERSEDED, Optional.empty(), Optional.empty());
    }

    static ConvergeResult startDeferred(CheckpointDoc checkpoint, StartDeferred.Reason reason) {
        return new ConvergeResult(reason == StartDeferred.Reason.CAPACITY
                ? ConvergeStatus.START_CAPACITY : ConvergeStatus.START_PENDING,
                Optional.of(checkpoint), Optional.empty());
    }

    static ConvergeResult failed(CheckpointDoc checkpoint, Throwable cause,
            Optional<PipelineState> transitionFrom) {
        return new ConvergeResult(ConvergeStatus.FAILED, Optional.of(checkpoint),
                Optional.of(cause), transitionFrom);
    }
}
