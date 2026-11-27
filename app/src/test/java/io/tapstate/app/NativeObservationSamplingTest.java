package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.SnapshotReading;
import io.tapstate.runtime.scheduler.ConvergeResult;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineIntentStore;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
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

    @Test void terminalPublicationRejectsLateRunningCallbacksButAllowsANewEpoch() {
        var f = new Fixture();f.warm();
        f.sampling.terminal(new TerminalPublication(SCOPE, 2, 5, CLAIM, Instant.EPOCH));
        f.sampling.recordPublishedRunning(f.pass, CHECKPOINT);
        assertThat(f.sampling.hasPublishedRunning(f.pass, f.result)).isFalse();
        var called = new java.util.concurrent.atomic.AtomicBoolean();
        assertThat(f.sampling.publishRunning(f.publisher, f.pass, CHECKPOINT, CONFIG, () -> true, () -> {},
                () -> { called.set(true);return Optional.empty(); })).isEmpty();
        assertThat(called).isFalse();
        var newer = new CheckpointDoc(PIPELINE, "RUNNING", 6, Instant.EPOCH);
        f.sampling.recordPublishedRunning(f.pass, newer);
        when(f.result.checkpoint()).thenReturn(Optional.of(newer));
        assertThat(f.sampling.hasPublishedRunning(f.pass, f.result)).isTrue();
    }

    @Test void supersededConfigurationSuppressesBothPublishPaths() {
        var f = new Fixture();
        f.warm();
        f.cacheHas(CHECKPOINT);
        when(f.configuration.observationConfiguration(CONFIG, "views", Map.of())).thenReturn(
                new MongoConfigurationDomain.ObservationCheck(true, Optional.empty()));

        assertThat(f.publish(CONFIG)).isEmpty();

        verify(f.publisher).publishScopedReading(SCOPE, f.state, f.observations, null, false);
        verify(f.publisher, never()).publishRunning(any(), any(), any(), any(), any());
    }

    @Test void aDifferentCacheCheckpointFallsBackWithoutReadingConfiguration() {
        var f = new Fixture();
        f.warm();
        f.cacheHas(new CheckpointDoc(PIPELINE, "RUNNING", 5, Instant.EPOCH));

        assertThat(f.publish(CONFIG)).contains(f.running);

        verify(f.publisher, times(2)).publishScopedReading(SCOPE, f.state, f.observations, null, false);
        verifyNoInteractions(f.configuration);
    }

    @Test void fallbackReestablishesTheMarkerAfterThePipelineLeavesObservation() {
        var f = new Fixture();
        f.warm();
        f.sampling.retain(Set.of());
        assertThat(f.publish(CONFIG)).contains(f.running);
        assertThat(f.publish(new PipelineIntentStore.ActuationReading(null, OptionalLong.empty()))).contains(f.running);

        verify(f.publisher, times(3)).publishScopedReading(SCOPE, f.state, f.observations, null, false);
        verifyNoInteractions(f.cache, f.captures, f.configuration);
    }

    @Test void aQueuedCombinedObservationDoesNotRetainItsReadAndSchema() throws IllegalAccessException {
        var f = new Fixture();
        f.warm();
        var reading = f.cacheHas(CHECKPOINT);
        var fields = java.util.stream.IntStream.range(0, 4096)
                .mapToObj(i -> new SourceField("source-column-" + i, "text")).toList();
        var model = new DiscoveredSourceModel("source", "fixture", 0,
                new SourceModel(List.of(new SourceTable("orders", fields, List.of(), List.of()))));
        when(reading.facts()).thenReturn(new MongoCacheDomain.ObservationFacts(CHECKPOINT, Map.of(), Map.of(), Map.of("source", model)));
        when(f.configuration.observationConfiguration(CONFIG, "views", Map.of())).thenReturn(
                new MongoConfigurationDomain.ObservationCheck(false, Optional.of(
                        new MongoConfigurationDomain.ObservationConfiguration(null, Map.of(), Map.of()))));
        when(f.captures.observationSample(any(), any(), any(), any())).thenReturn(Optional.of(
                new ObservationPublisher.CaptureSample(SnapshotReading.NONE, SnapshotReading.NONE, Map.of(), null)));
        var projected = new Observation(PIPELINE, PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null, Instant.EPOCH);
        when(f.publisher.publishRunning(eq(SCOPE), eq(CHECKPOINT), any(), any(), any())).thenAnswer(call -> {
            call.<ObservationStore>getArgument(2).save(projected, CHECKPOINT.epoch());
            return Optional.of(new ObservationPublisher.Publication(projected, true));
        });
        when(f.pass.guardOriginal(CLAIM)).thenReturn(() -> {});
        try (var writer = new ProcessObservationWriter(f.cache, false)) {
            var stage = writer.stage(f.pass);
            assertThat(f.sampling.publish(f.publisher, f.pass, f.result, CONFIG, null, stage::wrap))
                    .map(ObservationPublisher.Publication::observation).contains(projected);
            assertThat(writer.submit(stage, () -> {}, () -> {})).isTrue();
            boolean retained = retainsRead(stage.write().active(), reading, 3);
            System.out.println("OBSERVATION_SCHEMA_RETAINED " + retained + " columns=" + fields.size()
                    + " chargedBytes=" + writer.usage().retainedBytes());
            assertThat(retained).as("a queued execution guard should not retain the synchronous observation read").isFalse();
        }
    }

    private static boolean retainsRead(Object capture, MongoCacheDomain.ObservationRead reading, int remaining)
            throws IllegalAccessException {
        if (capture == reading) return true;
        if (remaining == 0 || !(capture instanceof Runnable || capture instanceof ObservationStore)) return false;
        for (var field : capture.getClass().getDeclaredFields()) {
            if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) continue;
            field.setAccessible(true);
            if (retainsRead(field.get(capture), reading, remaining - 1)) return true;
        }
        return false;
    }

    @Test void aRevocationAfterFastUpdateRefusalAlsoFencesTheFallbackWrite() {
        var f = new Fixture();
        f.warm();
        var reading = f.cacheHas(CHECKPOINT);
        when(f.configuration.observationConfiguration(CONFIG, "views", Map.of())).thenReturn(
                new MongoConfigurationDomain.ObservationCheck(false, Optional.of(
                        new MongoConfigurationDomain.ObservationConfiguration(null, Map.of(), Map.of()))));
        when(f.captures.observationSample(any(), any(), isNull(), any())).thenReturn(Optional.of(
                new ObservationPublisher.CaptureSample(io.tapstate.core.lifecycle.SnapshotReading.NONE,
                        io.tapstate.core.lifecycle.SnapshotReading.NONE, Map.of(), null)));
        var admitted = new java.util.concurrent.atomic.AtomicBoolean(true);
        when(reading.updateExisting(f.running, CHECKPOINT.epoch())).thenAnswer(call -> {
            admitted.set(false);
            return false;
        });
        var writer = mock(ObservationStore.class);
        var writeGuard = new java.util.concurrent.atomic.AtomicReference<Runnable>();
        when(f.cache.pipelineObservations(eq(SCOPE), eq(CLAIM), any())).thenAnswer(call -> {
            writeGuard.set(call.getArgument(2));
            return writer;
        });
        doAnswer(call -> {
            writeGuard.get().run();
            return null;
        }).when(writer).save(f.running, CHECKPOINT.epoch());
        when(f.publisher.publishRunning(eq(SCOPE), eq(CHECKPOINT), any(), any(), any())).thenAnswer(call -> {
            call.<ObservationStore>getArgument(2).save(f.running, CHECKPOINT.epoch());
            return Optional.of(new ObservationPublisher.Publication(f.running, true));
        });

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> f.sampling.publishRunning(f.publisher, f.pass, CHECKPOINT,
                CONFIG, admitted::get, () -> { if (!admitted.get()) throw ThreeLayerStorePort.refused(); }, Optional::empty))
                .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class, failure ->
                        assertThat(failure.code()).isEqualTo(io.tapstate.spi.store.IoError.WORKLOAD_CLAIM_FENCED));
        verify(reading).updateExisting(f.running, CHECKPOINT.epoch());
        verify(f.observations, never()).save(f.running, CHECKPOINT.epoch());
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
            when(publisher.publishScopedReading(SCOPE, state, observations, null, false))
                    .thenReturn(Optional.of(new ObservationPublisher.Publication(running, true)));
        }

        void warm() { assertThat(publish(CONFIG)).contains(running); }

        MongoCacheDomain.ObservationRead cacheHas(CheckpointDoc checkpoint) {
            when(captures.observationSelection(EXECUTION)).thenReturn(Optional.of(
                    new ScopedPipelineCaptureCoordinator.ObservationSelection(EXECUTION, Map.of(), Map.of())));
            var reading = mock(MongoCacheDomain.ObservationRead.class);
            when(reading.facts()).thenReturn(new MongoCacheDomain.ObservationFacts(checkpoint, Map.of(), Map.of(), Map.of()));
            when(cache.readObservation(eq(SCOPE), eq(CLAIM), eq(Map.of()), any())).thenReturn(Optional.of(reading));
            return reading;
        }

        Optional<Observation> publish(PipelineIntentStore.ActuationReading config) {
            return sampling.publish(publisher, pass, result, config, null).map(ObservationPublisher.Publication::observation);
        }
    }
}
