package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.CheckpointDoc;

import java.util.Objects;
import java.util.Optional;

/**
 * The result of one convergence pass: its {@link ConvergeStatus}, the resulting checkpoint when the
 * pass ended at a state, and the job failure cause when the pass drove a dead job to FAILED. The
 * checkpoint is present for {@link ConvergeStatus#CONVERGED} and {@link ConvergeStatus#FAILED}; the
 * failure is present only for {@link ConvergeStatus#FAILED}.
 */
public record ConvergeResult(
        ConvergeStatus status, Optional<CheckpointDoc> checkpoint, Optional<Throwable> failure) {

    public ConvergeResult {
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(checkpoint, "checkpoint");
        Objects.requireNonNull(failure, "failure");
    }

    static ConvergeResult converged(CheckpointDoc checkpoint) {
        return new ConvergeResult(ConvergeStatus.CONVERGED, Optional.of(checkpoint), Optional.empty());
    }

    static ConvergeResult nothingToDo() {
        return new ConvergeResult(ConvergeStatus.NOTHING_TO_DO, Optional.empty(), Optional.empty());
    }

    static ConvergeResult recovering(CheckpointDoc checkpoint) {
        return new ConvergeResult(ConvergeStatus.RECOVERING, Optional.of(checkpoint), Optional.empty());
    }

    static ConvergeResult superseded() {
        return new ConvergeResult(ConvergeStatus.SUPERSEDED, Optional.empty(), Optional.empty());
    }

    static ConvergeResult failed(CheckpointDoc checkpoint, Throwable cause) {
        return new ConvergeResult(ConvergeStatus.FAILED, Optional.of(checkpoint), Optional.of(cause));
    }
}
