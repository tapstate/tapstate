package io.tapstate.app;

import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.runtime.scheduler.ConvergeResult;
import io.tapstate.runtime.scheduler.ConvergeStatus;
import io.tapstate.spi.store.ObservationStore;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class PipelineStateEventsTest {

    private static final Instant AT = Instant.parse("2026-09-27T10:00:00Z");
    private static final ObservationStore.Scope FIRST = new ObservationStore.Scope("inc-a", 1);

    @Test
    void oneAppliedTransitionHasAStableIdentityButAnIdlePassProducesNothing() {
        ConvergeResult started = transition(PipelineState.NEW, PipelineState.RUNNING, 7);

        List<PipelineEvent> first = PipelineStateEvents.of("flow", FIRST, started, null);

        assertThat(first).extracting(PipelineEvent::kind)
                .containsExactly(PipelineEvent.Kind.STATE_CHANGED);
        assertThat(first.getFirst().beforeState()).isEqualTo(PipelineState.NEW);
        assertThat(first.getFirst().afterState()).isEqualTo(PipelineState.RUNNING);
        assertThat(first.getFirst().occurredAt()).isEqualTo(AT);
        assertThat(PipelineStateEvents.of("flow", FIRST, started, null)).containsExactlyElementsOf(first);
        assertThat(PipelineStateEvents.of("flow", FIRST,
                new ConvergeResult(ConvergeStatus.CONVERGED, started.checkpoint(), Optional.empty()), null))
                .isEmpty();
        assertThat(PipelineStateEvents.of("flow", null, started, null)).isEmpty();
    }

    @Test
    void failureAndRecoveryRemainReadableAfterCurrentStateMovesOn() {
        ObservationFailure coded = new ObservationFailure("engine.job-failed", Map.of("pipeline", "flow"));
        List<PipelineEvent> failed = PipelineStateEvents.of("flow", FIRST,
                transition(PipelineState.RUNNING, PipelineState.FAILED, 8), coded);
        List<PipelineEvent> recovered = PipelineStateEvents.of("flow",
                new ObservationStore.Scope("inc-a", 2),
                transition(PipelineState.FAILED, PipelineState.RUNNING, 9), null);

        assertThat(failed).extracting(PipelineEvent::kind).containsExactly(
                PipelineEvent.Kind.STATE_CHANGED, PipelineEvent.Kind.FAILURE);
        assertThat(failed.get(1).failure()).isEqualTo(coded);
        assertThat(recovered).extracting(PipelineEvent::kind).containsExactly(
                PipelineEvent.Kind.STATE_CHANGED, PipelineEvent.Kind.EXECUTION_RECOVERED,
                PipelineEvent.Kind.EXECUTION_RESTARTED);
        assertThat(recovered).extracting(PipelineEvent::executionGeneration)
                .containsOnly(2L);
        assertThat(recovered).extracting(PipelineEvent::id)
                .doesNotContainAnyElementsOf(failed.stream().map(PipelineEvent::id).toList());
    }

    @Test
    void ordinaryResumeKeepsItsExecutionAndDoesNotClaimARestart() {
        List<PipelineEvent> resumed = PipelineStateEvents.of("flow",
                new ObservationStore.Scope("inc-a", 2),
                transition(PipelineState.PAUSED, PipelineState.RUNNING, 10), null);

        assertThat(resumed).extracting(PipelineEvent::kind)
                .containsExactly(PipelineEvent.Kind.STATE_CHANGED);
    }

    @Test
    void explicitStopThenStartAfterFailureStillEmitsRecovery() {
        List<PipelineEvent> events = PipelineStateEvents.of("flow",
                new ObservationStore.Scope("inc-a", 2),
                transition(PipelineState.STOPPED, PipelineState.RUNNING, 11), null, true);

        assertThat(events).extracting(PipelineEvent::kind).containsExactly(
                PipelineEvent.Kind.STATE_CHANGED, PipelineEvent.Kind.EXECUTION_RECOVERED,
                PipelineEvent.Kind.EXECUTION_RESTARTED);
    }

    private static ConvergeResult transition(PipelineState from, PipelineState to, long epoch) {
        return new ConvergeResult(ConvergeStatus.CONVERGED,
                Optional.of(new CheckpointDoc("flow", StateJson.of(to), epoch, AT)),
                Optional.empty(), Optional.of(from));
    }
}
