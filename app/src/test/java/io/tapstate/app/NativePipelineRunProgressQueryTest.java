package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineRunProgress;
import io.tapstate.core.lifecycle.PipelineRunStage;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.ConnectorRunBindings;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import io.tapstate.spi.store.PipelineIntentStore;
import io.tapstate.spi.store.PipelinePreparation;
import io.tapstate.spi.store.PipelinePreparationInputs;
import io.tapstate.spi.store.PipelineStatePreparations;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class NativePipelineRunProgressQueryTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster", 1, "cache", "orders", 1);
    private static final String HASH = "a".repeat(64);
    private final PipelineIntentStore intents = mock(PipelineIntentStore.class);
    private final PipelineStatePreparations preparations = mock(PipelineStatePreparations.class);
    private final NativePipelineRunProgressQuery query = new NativePipelineRunProgressQuery(
            new CacheBusinessScope("cluster", 1, "cache"), intents, preparations, () -> true);

    @Test
    void startIsReadableBeforePreparationAndTheSameRunSurvivesTheHandoff() {
        var intent = intent(4, PipelineState.RUNNING);
        var pending = new PipelineIntentStore.PendingStart(intent, new PipelinePreparationInputs(HASH, Map.of(), Map.of(), List.of()));
        when(intents.pendingStart("orders")).thenReturn(Optional.of(pending));
        when(preparations.read("orders")).thenReturn(Optional.of(new PipelinePreparation(
                "orders", 1, PipelinePreparation.Phase.UNASSIGNED, null, null, null)));
        var queued = query.find("orders").orElseThrow().progress();
        assertThat(queued.active()).containsExactly(
                new PipelineRunProgress.Activity(PipelineRunStage.WAITING_FOR_SCHEDULER, null));

        when(intents.loadForActuation("orders")).thenReturn(Optional.of(intent));
        when(intents.executionBindings(SCOPE)).thenReturn(Optional.of(bindings(4, 4)));
        var prepared = query.find("orders").orElseThrow().progress();
        assertThat(prepared.runId()).isEqualTo(queued.runId());
        assertThat(prepared.version()).isGreaterThan(queued.version());
        assertThat(prepared.active()).isEmpty();
    }

    @Test
    void aStoppedRunWithoutAProjectionDoesNotAwaitMilestones() {
        when(intents.loadForActuation("orders")).thenReturn(Optional.of(intent(5, PipelineState.STOPPED)));
        when(intents.executionBindings(SCOPE)).thenReturn(Optional.of(bindings(4, 5)));
        var stopped = query.find("orders").orElseThrow();
        assertThat(stopped.awaitingMilestones()).isFalse();
        assertThat(stopped.progress().milestones()).extracting(PipelineRunProgress.Milestone::stage)
                .containsExactly(PipelineRunStage.START_ACCEPTED);
    }

    @Test
    void rejectsAConcurrentReplacementAndUsesItsOwnStartRevisionOnceTheReadAgrees() {
        when(intents.loadForActuation("orders")).thenReturn(Optional.of(intent(4, PipelineState.RUNNING)));
        when(intents.executionBindings(SCOPE)).thenReturn(Optional.of(bindings(4, 4)));
        String previousRun = query.find("orders").orElseThrow().progress().runId();
        when(intents.executionBindings(SCOPE)).thenReturn(Optional.of(bindings(6, 6)));
        assertThat(query.find("orders")).isEmpty();
        when(intents.loadForActuation("orders")).thenReturn(Optional.of(intent(6, PipelineState.RUNNING)));
        assertThat(query.find("orders").orElseThrow().progress().runId()).isNotEqualTo(previousRun);
    }

    private static PipelineIntentSnapshot intent(long revision, PipelineState state) {
        return new PipelineIntentSnapshot(SCOPE, revision, new DesiredState("orders", state, HASH));
    }

    private static PipelineIntentStore.ExecutionBindings bindings(long start, long revision) {
        return new PipelineIntentStore.ExecutionBindings(new ConnectorRunBindings(SCOPE, start, List.of(), null), revision);
    }
}
