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
        when(intents.load("pipeline")).thenReturn(Optional.of(new PipelineIntentSnapshot(scope, 5,
                new DesiredState("pipeline", PipelineState.STOPPED, "hash", previouslyClearing))));
        var next = new DesiredState("pipeline", PipelineState.RUNNING, "hash", clears, null, reassembles, 9L);
        when(intents.compareAndSet(scope, 5, next)).thenReturn(true);
        var failure = new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null);
        var port = new ThreeLayerStorePort(configuration, bound, original -> { throw failure; });

        if (mustPrepare) {
            assertThatThrownBy(() -> port.desired().save(next)).isSameAs(failure);
            verify(intents, never()).compareAndSet(any(), anyLong(), any());
        } else {
            port.desired().save(next);
            verify(intents).compareAndSet(scope, 5, next);
        }
    }
}
