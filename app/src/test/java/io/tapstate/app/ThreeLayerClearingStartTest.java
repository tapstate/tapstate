package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.*;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class ThreeLayerClearingStartTest {
    @ParameterizedTest
    @CsvSource({"true,true,true,false", "false,true,true,true", "true,false,true,true", "true,true,false,true"})
    void onlyAStartCarryingAnExplicitPrecedingClearCanDeferRetainedSelectionPreparation(
            boolean previouslyClearing, boolean clears, boolean reassembles, boolean mustPrepare) {
        var configuration = mock(MongoConfigurationDomain.class);
        var bound = mock(StorageRuntime.Bound.class);
        var mappings = mock(PipelineStateMappings.class);
        var intents = mock(PipelineIntentStore.class);
        var scope = new PipelineIntentScope("cluster", 1, "cache", "pipeline", 1);
        when(bound.preparation()).thenReturn(new CachePreparation("setup",
                new CacheAuthScope("cluster", 1, StorageBinding.Mode.OP, "cache", 1)));
        when(configuration.pipelineStates()).thenReturn(mappings);
        when(mappings.read("pipeline")).thenReturn(Optional.of(
                new PipelineStateMapping("pipeline", 1, PipelineStateMapping.Phase.ACTIVE, null)));
        when(configuration.intents()).thenReturn(intents);
        when(intents.guardedBy(any())).thenReturn(intents);
        String hash = "a".repeat(64);
        when(intents.load("pipeline")).thenReturn(Optional.of(new PipelineIntentSnapshot(scope, 5,
                new DesiredState("pipeline", PipelineState.STOPPED, hash, previouslyClearing))));
        var next = new DesiredState("pipeline", PipelineState.RUNNING, hash, clears, null, reassembles, 9L);
        when(intents.compareAndSet(scope, 5, next)).thenReturn(true);
        var failure = new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null);
        var inputs = new io.tapstate.spi.store.PipelinePreparationInputs(hash, Map.of(), Map.of(), java.util.List.of());
        var port = new ThreeLayerStorePort(configuration, bound, original -> { throw failure; }, (original, captured) -> {
            assertThat(original).isEqualTo(scope);
            assertThat(captured).isSameAs(inputs);
            throw failure;
        });
        var store = port.desired();
        var snapshot = store.readForStart("pipeline");
        var preparation = Optional.of(new io.tapstate.spi.store.DesiredStore.StartPreparation(scope, inputs));

        if (mustPrepare) {
            assertThatThrownBy(() -> store.save(next)).isSameAs(failure);
            assertThatThrownBy(() -> store.saveStart(next, snapshot, preparation)).isSameAs(failure);
            verify(intents, never()).compareAndSet(any(), anyLong(), any());
        } else {
            store.save(next);
            store.saveStart(next, snapshot, preparation);
            verify(intents, times(2)).compareAndSet(scope, 5, next);
        }
        verify(intents, times(2)).load("pipeline");
    }
}
