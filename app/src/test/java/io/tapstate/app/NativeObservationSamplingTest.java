package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.scheduler.ConvergeResult;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineIntentStore;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class NativeObservationSamplingTest {
    private static final String PIPELINE = "flow";
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster", 1, "cache", PIPELINE, 7);
    private static final WorkloadClaimFence CLAIM = new WorkloadClaimFence(
            new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE),
            new WorkloadOwner("node", "boot"), 2, 3, 1);
    private static final ExecutionFence EXECUTION = new ExecutionFence(SCOPE, CLAIM);
    private static final CheckpointDoc CHECKPOINT = new CheckpointDoc(PIPELINE, "RUNNING", 4, Instant.EPOCH);
    private static final PipelineIntentStore.ActuationReading CONFIG =
            new PipelineIntentStore.ActuationReading(null, OptionalLong.of(1));

    @Test void aSynchronousRunningPublicationEnablesBatchingOnlyForItsOwnEpochAndExecution() {
        var f = new Fixture();
        assertThat(f.sampling.hasPublishedRunning(f.pass, f.result)).isFalse();
        f.warm();
        assertThat(f.sampling.hasPublishedRunning(f.pass, f.result)).isTrue();

        when(f.result.checkpoint()).thenReturn(Optional.of(new CheckpointDoc(PIPELINE, "RUNNING", 5, Instant.EPOCH)));
        assertThat(f.sampling.hasPublishedRunning(f.pass, f.result)).isFalse();
        when(f.result.checkpoint()).thenReturn(Optional.of(CHECKPOINT));
        when(f.pass.claim()).thenReturn(new WorkloadClaimFence(CLAIM.key(), CLAIM.owner(), 2, 4, 1));
        assertThat(f.sampling.hasPublishedRunning(f.pass, f.result)).isFalse();
    }

    @Test void supersededConfigurationSuppressesBothPublishPaths() {
        var f = new Fixture();
        f.warm();
        f.cacheHas(CHECKPOINT);
        when(f.configuration.observationConfiguration(CONFIG, "views", Map.of())).thenReturn(
                new MongoConfigurationDomain.ObservationCheck(true, Optional.empty()));

        assertThat(f.publish(CONFIG)).isEmpty();

        verify(f.publisher).publishScoped(SCOPE, f.state, f.observations, null);
        verify(f.publisher, never()).publishRunning(any(), any(), any(), any());
    }

    @Test void aDifferentCacheCheckpointFallsBackWithoutReadingConfiguration() {
        var f = new Fixture();
        f.warm();
        f.cacheHas(new CheckpointDoc(PIPELINE, "RUNNING", 5, Instant.EPOCH));

        assertThat(f.publish(CONFIG)).contains(f.running);

        verify(f.publisher, times(2)).publishScoped(SCOPE, f.state, f.observations, null);
        verifyNoInteractions(f.configuration);
    }

    @Test void fallbackReestablishesTheMarkerAfterThePipelineLeavesObservation() {
        var f = new Fixture();
        f.warm();
        f.sampling.retain(Set.of());
        assertThat(f.publish(CONFIG)).contains(f.running);
        assertThat(f.publish(new PipelineIntentStore.ActuationReading(null, OptionalLong.empty()))).contains(f.running);

        verify(f.publisher, times(3)).publishScoped(SCOPE, f.state, f.observations, null);
        verifyNoInteractions(f.cache, f.captures, f.configuration);
    }

    private static final class Fixture {
        final MongoCacheDomain cache = mock(MongoCacheDomain.class);
        final MongoConfigurationDomain configuration = mock(MongoConfigurationDomain.class);
        final ScopedPipelineCaptureCoordinator captures = mock(ScopedPipelineCaptureCoordinator.class);
        final ObservationPublisher publisher = mock(ObservationPublisher.class);
        final PipelineConvergencePass pass = mock(PipelineConvergencePass.class);
        final StateStore state = mock(StateStore.class);
        final ObservationStore observations = mock(ObservationStore.class);
        final Observation running = mock(Observation.class);
        final ConvergeResult result = mock(ConvergeResult.class);
        final NativeObservationSampling sampling = new NativeObservationSampling(configuration, cache, captures,
                id -> Optional.of(EXECUTION), id -> null, "views", Clock.systemUTC());

        Fixture() {
            when(pass.scope()).thenReturn(SCOPE);
            when(pass.claim()).thenReturn(CLAIM);
            when(pass.state()).thenReturn(state);
            when(pass.observationSample()).thenReturn(observations);
            when(result.checkpoint()).thenReturn(Optional.of(CHECKPOINT));
            when(running.state()).thenReturn(PipelineState.RUNNING);
            when(publisher.publishScoped(SCOPE, state, observations, null)).thenReturn(Optional.of(running));
        }

        void warm() { assertThat(publish(CONFIG)).contains(running); }

        void cacheHas(CheckpointDoc checkpoint) {
            when(captures.observationSelection(EXECUTION)).thenReturn(Optional.of(
                    new ScopedPipelineCaptureCoordinator.ObservationSelection(EXECUTION, Map.of(), Map.of())));
            when(cache.readObservation(eq(SCOPE), eq(CLAIM), eq(Map.of()), any())).thenReturn(Optional.of(
                    new MongoCacheDomain.ObservationFacts(checkpoint, Map.of(), Map.of(), Map.of())));
        }

        Optional<Observation> publish(PipelineIntentStore.ActuationReading config) {
            return sampling.publish(publisher, pass, result, config, null);
        }
    }
}
