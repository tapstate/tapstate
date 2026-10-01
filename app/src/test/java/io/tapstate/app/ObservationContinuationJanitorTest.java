package io.tapstate.app;

import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ExecutionGenerationStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ObservationContinuationJanitorTest {
    private static final ObservationStore.Scope SOURCE = new ObservationStore.Scope("inc-a", 41);
    private static final ObservationStore.Scope PREVIOUS = new ObservationStore.Scope("inc-a", 42);
    private static final ObservationStore.Scope ADMITTED = new ObservationStore.Scope("inc-a", 43);

    @Test
    void anAdmittedContinuationProtectsItsOlderPublicAndPrivateFloors() {
        ObservationStore observations = mock(ObservationStore.class);
        ArtifactStore artifacts = mock(ArtifactStore.class);
        ExecutionGenerationStore generations = mock(ExecutionGenerationStore.class);
        StateStore states = mock(StateStore.class);
        var legacy = new ObservationStore.LatestSnapshot("orders", Optional.of(SOURCE), Optional.of(Instant.EPOCH));
        var manifest = new ObservationStore.ManifestSnapshot("cursor", "revision", List.of(SOURCE, PREVIOUS));
        when(observations.supportsManifestStorage()).thenReturn(true);
        when(observations.scanLatestAfter(any(), anyInt())).thenReturn(List.of(legacy));
        when(observations.scanManifestsAfter(any(), anyInt())).thenReturn(List.of(manifest));
        when(artifacts.pipelineIncarnationId("orders")).thenReturn(Optional.of("inc-a"));
        when(artifacts.pipelineIdForIncarnation("inc-a")).thenReturn(Optional.of("orders"));
        when(generations.currentGeneration("cluster", "orders")).thenReturn(OptionalLong.of(43));
        when(states.supportsStopReservations()).thenReturn(true);
        var marker = new StopReservation("orders", "handoff", 0, 3,
                new DesiredState("orders", PipelineState.RUNNING, "revision"),
                new StopReservation.Source("cluster", SOURCE, new StopReservation.JobIdentity("cluster", 41, "old-boot")),
                StopReservation.Phase.SUCCESSOR_ADMITTED, StopReservation.CounterPolicy.CONTINUE,
                StopAuthority.standalone("cluster", 43), new StopReservation.Successor(ADMITTED, "next-boot", null),
                StopReservation.CURRENT_FORMAT);
        when(states.readStopReservation("orders")).thenReturn(Optional.of(marker));

        try (var janitor = new ObservationJanitor(observations, artifacts, generations, states,
                "cluster", 4, Duration.ofSeconds(1), false)) {
            janitor.runOneBatch();
            janitor.runOneBatch();
        }

        verify(observations, never()).deleteIfUnchanged(any());
        verify(observations, never()).deleteOrphanIfUnchanged(any());
        verify(observations, never()).deleteManifestIfUnchanged(any());
        verify(observations, never()).deleteManifestIfUnchanged(any(), any());
    }
}
