package io.tapstate.app;

import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.runtime.scheduler.ConvergeResult;
import io.tapstate.spi.store.ObservationStore;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Projects only newly applied checkpoint transitions into stable, scoped event candidates. */
final class PipelineStateEvents {

    private PipelineStateEvents() {
    }

    static List<PipelineEvent> of(String pipelineId, ObservationStore.Scope scope,
            ConvergeResult result, ObservationFailure failure) {
        return of(pipelineId, scope, result, failure, false);
    }

    static List<PipelineEvent> of(String pipelineId, ObservationStore.Scope scope,
            ConvergeResult result, ObservationFailure failure, boolean recovering) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (scope == null || result == null) {
            return List.of();
        }
        List<PipelineEvent> events = new ArrayList<>();
        if (result.executionBoundary().isPresent()) {
            var boundary = result.executionBoundary().orElseThrow();
            if (!scope.equals(boundary.scope())) { return List.of(); }
            PipelineState before = boundary.beforeState();
            if (before != PipelineState.RUNNING) {
                events.add(boundaryEvent(pipelineId, boundary, PipelineEvent.Kind.STATE_CHANGED));
            }
            if (boundary.recovering() || recovering || before == PipelineState.FAILED) {
                events.add(boundaryEvent(pipelineId, boundary, PipelineEvent.Kind.EXECUTION_RECOVERED));
            }
            if (scope.executionGeneration() > 1) {
                events.add(boundaryEvent(pipelineId, boundary, PipelineEvent.Kind.EXECUTION_RESTARTED));
            }
        }
        if (result.transitionFrom().isEmpty() || result.checkpoint().isEmpty()) { return List.copyOf(events); }
        CheckpointDoc checkpoint = result.checkpoint().orElseThrow();
        PipelineState from = result.transitionFrom().orElseThrow();
        PipelineState to = StateJson.parse(checkpoint.stateJson());
        if (from == to) {
            return List.copyOf(events);
        }
        events.add(event(pipelineId, scope, checkpoint, PipelineEvent.Kind.STATE_CHANGED,
                from, to, null));
        if (to == PipelineState.FAILED && failure != null) {
            events.add(event(pipelineId, scope, checkpoint, PipelineEvent.Kind.FAILURE,
                    from, to, failure));
        }
        if (to == PipelineState.RUNNING && (from == PipelineState.FAILED || recovering)) {
            events.add(event(pipelineId, scope, checkpoint, PipelineEvent.Kind.EXECUTION_RECOVERED,
                    from, to, null));
        }
        if (to == PipelineState.RUNNING && from != PipelineState.PAUSED
                && scope.executionGeneration() > 1) {
            events.add(event(pipelineId, scope, checkpoint, PipelineEvent.Kind.EXECUTION_RESTARTED,
                    from, to, null));
        }
        return List.copyOf(events);
    }

    private static PipelineEvent boundaryEvent(String pipelineId, ConvergeResult.ExecutionBoundary boundary,
            PipelineEvent.Kind kind) {
        var scope = boundary.scope();
        return new PipelineEvent(PipelineEvent.stateId(pipelineId, scope.pipelineIncarnationId(),
                scope.executionGeneration(), kind, boundary.checkpointEpoch()), pipelineId, scope.pipelineIncarnationId(),
                scope.executionGeneration(), kind, boundary.occurredAt(), boundary.beforeState(), PipelineState.RUNNING,
                null, null, null);
    }

    private static PipelineEvent event(String pipelineId, ObservationStore.Scope scope,
            CheckpointDoc checkpoint, PipelineEvent.Kind kind, PipelineState from,
            PipelineState to, ObservationFailure failure) {
        return new PipelineEvent(PipelineEvent.stateId(pipelineId, scope.pipelineIncarnationId(),
                scope.executionGeneration(), kind, checkpoint.epoch()),
                pipelineId, scope.pipelineIncarnationId(), scope.executionGeneration(), kind,
                checkpoint.touchTime(), from, to, failure, null, null);
    }
}
