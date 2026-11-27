package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.tapstate.control.core.*;
import io.tapstate.core.lifecycle.*;
import io.tapstate.core.model.*;
import io.tapstate.spi.store.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class PipelineLifecycleWithoutMetricsTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster", 1, "cache", "orders", 1);

    @Test void listAndDetailUseTheEngineWithoutInventingAMetricsObservation() {
        var memory = new InMemoryStorePort();
        memory.artifacts().save(new PipelineResource("orders", null, List.of(SourceRef.bare("input")), List.of(),
                null, null, null, Map.of()));
        var current = new AtomicReference<>(Optional.of(PipelineState.RUNNING));
        var desired = new AtomicReference<>(PipelineState.RUNNING);
        var store = mock(StorePort.class);
        when(store.desired()).thenReturn(memory.desired());
        when(store.pipelineCatalogStates(List.of("orders"))).thenAnswer(call -> Map.of("orders",
                new StorePort.PipelineCatalogState(Optional.empty(), Optional.of(new DesiredState("orders", desired.get(), "hash")),
                        Optional.empty(), false, PipelineState.RUNNING, SCOPE)));
        var artifacts = new ArtifactQueryService(memory.artifacts());
        var diagnostics = new PipelineStatusDiagnostics() {
            @Override public Optional<ObservationFailure> failure(String id, PipelineState observed) { return Optional.empty(); }
            @Override public Optional<PipelineState> currentState(String id, PipelineIntentScope scope) {
                assertThat(id).isEqualTo("orders"); assertThat(scope).isEqualTo(SCOPE); return current.get();
            }
        };
        var observations = new PipelineObservationQueryService(artifacts, memory.observations(), ExecutionPlans.NONE,
                List::of, diagnostics);
        var catalog = new PipelineCatalogService(artifacts, new PipelineDraftService(mock(PipelineDraftStore.class)), observations, store);
        var detail = catalog.find("orders").orElseThrow();
        assertThat(detail.status().displayState()).isEqualTo(PipelineCatalogItem.UiState.RUNNING);
        assertThat(detail.status().state()).isEqualTo(PipelineCatalogItem.DisplayState.RUNNING);
        assertThat(catalog.list()).containsExactly(detail);
        assertThat(detail.status().observedState()).isNull();
        assertThat(detail.status().observedAt()).isNull();
        assertThat(memory.observations().read("orders")).isEmpty();

        desired.set(PipelineState.STOPPED);
        assertThat(catalog.find("orders").orElseThrow().status().displayState()).isEqualTo(PipelineCatalogItem.UiState.STOPPING);
        desired.set(PipelineState.RUNNING);
        current.set(Optional.empty());
        assertThat(catalog.find("orders").orElseThrow().status().displayState()).isEqualTo(PipelineCatalogItem.UiState.STARTING);
        assertThat(catalog.find("orders").orElseThrow().status().state()).isEqualTo(PipelineCatalogItem.DisplayState.STARTING);
    }

    @Test void stopResultsUseTheBatchedIntentAndCompletionFacts() {
        var memory = new InMemoryStorePort();
        var facts = Map.of(
                "clearing", stopFacts("clearing", PipelineState.STOPPED, true, false),
                "cleared", stopFacts("cleared", PipelineState.STOPPED, false, true),
                "retained", stopFacts("retained", PipelineState.STOPPED, false, false),
                "pending", stopFacts("pending", PipelineState.RUNNING, false, false));
        for (String id : facts.keySet()) {
            memory.artifacts().save(new PipelineResource(id, null, List.of(SourceRef.bare("input")), List.of(),
                    null, null, null, Map.of()));
        }
        var fallback = mock(DesiredStore.class);
        var store = mock(StorePort.class);
        when(store.desired()).thenReturn(fallback);
        when(store.pipelineCatalogStates(anyList())).thenReturn(facts);
        var artifacts = new ArtifactQueryService(memory.artifacts());
        var observations = new PipelineObservationQueryService(artifacts, memory.observations());
        var catalog = new PipelineCatalogService(
                artifacts, new PipelineDraftService(mock(PipelineDraftStore.class)), observations, store);

        var listed = catalog.list();
        for (var item : listed) {
            assertThat(catalog.find(item.id())).contains(item);
        }
        assertThat(listed).extracting(item -> item.status().stopState())
                .containsExactly(PipelineCatalogItem.StopState.CLEARED, PipelineCatalogItem.StopState.CLEARING,
                        null, PipelineCatalogItem.StopState.RETAINED);
        verifyNoInteractions(fallback);
    }

    private static StorePort.PipelineCatalogState stopFacts(
            String id, PipelineState checkpoint, boolean purge, boolean cleared) {
        var observation = new Observation(id, checkpoint, Map.of(), Map.of(), Map.of(), null, null);
        return new StorePort.PipelineCatalogState(Optional.of(observation),
                Optional.of(new DesiredState(id, PipelineState.STOPPED, "hash", purge)),
                Optional.empty(), false, checkpoint, null, cleared);
    }
}
