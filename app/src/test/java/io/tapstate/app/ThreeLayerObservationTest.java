package io.tapstate.app;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.control.core.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.*;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.spi.store.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class ThreeLayerObservationTest {
    @ParameterizedTest @ValueSource(booleans = {false, true})
    void aPurgeInProgressRemainsStoppingWithoutReusingItsRetiredObservation(boolean recoveredCache) {
        var f = new Fixture();
        if (recoveredCache) when(f.recoveries.read("pipeline")).thenReturn(Optional.of(
                new PipelineCacheRecovery(UUID.randomUUID().toString(), null, f.scope)));
        f.mapping.set(new PipelineStateMapping("pipeline", 1, PipelineStateMapping.Phase.REBUILDING, "purge"));
        var artifacts = mock(ArtifactStore.class);
        when(artifacts.get("pipeline")).thenReturn(Optional.of(new PipelineResource("pipeline", null, List.of(), null, null, null, null, null)));
        var query = new ArtifactQueryService(artifacts);
        var drafts = mock(PipelineDraftService.class);
        when(drafts.find("pipeline")).thenReturn(Optional.empty());
        var desired = mock(DesiredStore.class);
        when(desired.read("pipeline")).thenReturn(Optional.of(new DesiredState("pipeline", PipelineState.STOPPED, "hash", true)));
        var catalog = new PipelineCatalogService(query, drafts, desired,
                new PipelineObservationQueryService(query, f.port.observations()), f.port::pipelineRecoveryRequired);

        var status = catalog.find("pipeline").orElseThrow().status();
        assertThat(status.state()).isEqualTo(PipelineCatalogItem.DisplayState.STOPPING);
        assertThat(status.observedState()).isNull();
        verifyNoInteractions(f.cache);
        assertThatThrownBy(() -> f.port.state().read("pipeline")).isInstanceOf(TapstateException.class);
    }

    @Test void aCoherentOriginalGenerationStillReturnsItsObservedState() {
        var f = new Fixture();
        assertThat(f.port.observations().read("pipeline")).contains(f.observed);
        verify(f.cache).pipelineObservations(f.scope);
    }

    @Test void aRebuildThatOvertakesAnObservationReadCannotPublishTheOldGeneration() {
        var f = new Fixture();
        when(f.observations.read("pipeline")).thenAnswer(call -> {
            f.mapping.set(new PipelineStateMapping("pipeline", 2, PipelineStateMapping.Phase.ACTIVE, null));
            return Optional.of(f.observed);
        });
        assertThat(f.port.observations().read("pipeline")).isEmpty();
        verify(f.cache).pipelineObservations(f.scope);
        verifyNoMoreInteractions(f.cache);
    }

    @ParameterizedTest @EnumSource(value = IoError.class, names = {"WORKLOAD_CLAIM_FENCED", "CACHE_BUSINESS_FENCED"})
    void anOldCacheFenceDuringTheKnownRebuildIsAnUnobservedTransition(IoError code) {
        var f = new Fixture();
        when(f.observations.read("pipeline")).thenAnswer(call -> {
            f.mapping.set(new PipelineStateMapping("pipeline", 1, PipelineStateMapping.Phase.REBUILDING, "purge"));
            throw new TapstateException(code, Map.of(), null);
        });
        assertThat(f.port.observations().read("pipeline")).isEmpty();
    }

    @ParameterizedTest @EnumSource(value = IoError.class, names = {"WORKLOAD_CLAIM_FENCED", "CACHE_BUSINESS_FENCED"})
    void anUnchangedScopeDoesNotHideAFenceFailure(IoError code) {
        var f = new Fixture();
        var failure = new TapstateException(code, Map.of(), null);
        when(f.observations.read("pipeline")).thenThrow(failure);
        assertThatThrownBy(() -> f.port.observations().read("pipeline")).isSameAs(failure);
    }

    @Test void aDependencyFailureIsNotAnEmptyObservationEvenDuringRebuild() {
        var f = new Fixture();
        var failure = StorageRuntime.unavailable();
        when(f.observations.read("pipeline")).thenAnswer(call -> {
            f.mapping.set(new PipelineStateMapping("pipeline", 1, PipelineStateMapping.Phase.REBUILDING, "purge"));
            throw failure;
        });
        assertThatThrownBy(() -> f.port.observations().read("pipeline")).isSameAs(failure);
    }

    @Test void anIncompleteConfirmedCacheRecoveryStillAppearsAsRequired() {
        var f = new Fixture();
        when(f.recoveries.read("pipeline")).thenReturn(Optional.of(new PipelineCacheRecovery(UUID.randomUUID().toString(), null, f.scope)));
        when(f.preparations.read("pipeline")).thenReturn(Optional.of(new PipelinePreparation("pipeline", 1,
                PipelinePreparation.Phase.UNASSIGNED, null, null, null)));
        assertThat(f.port.pipelineRecoveryRequired("pipeline")).isTrue();
    }

    @Test void aChangedSummaryCannotBeAdoptedByAReadThatCapturedTheOldGeneration() {
        var f = new Fixture();
        var next = new PipelineIntentScope("cluster", 1, "cache", "pipeline", 2);
        var summary = new PipelineCacheRecovery(UUID.randomUUID().toString(), null, next);
        when(f.recoveries.read("pipeline")).thenAnswer(call -> {
            f.mapping.set(new PipelineStateMapping("pipeline", 2, PipelineStateMapping.Phase.ACTIVE, null));
            return Optional.of(summary);
        });
        assertThat(f.port.pipelineRecoveryRequired("pipeline")).isFalse();
        verifyNoInteractions(f.cache, f.preparations);
    }

    @Test void aWrongSummaryWithoutAConcurrentChangeStillRefusesTheRead() {
        var f = new Fixture();
        var next = new PipelineIntentScope("cluster", 1, "cache", "pipeline", 2);
        when(f.recoveries.read("pipeline")).thenReturn(Optional.of(new PipelineCacheRecovery(UUID.randomUUID().toString(), null, next)));
        assertThatThrownBy(() -> f.port.pipelineRecoveryRequired("pipeline")).isInstanceOf(TapstateException.class);
    }

    private static final class Fixture {
        final MongoConfigurationDomain configuration = mock(MongoConfigurationDomain.class);
        final MongoCacheDomain cache = mock(MongoCacheDomain.class);
        final StorageRuntime.Bound bound = mock(StorageRuntime.Bound.class);
        final PipelineStateMappings mappings = mock(PipelineStateMappings.class);
        final PipelineStatePreparations preparations = mock(PipelineStatePreparations.class);
        final CacheRecoveryStore recoveries = mock(CacheRecoveryStore.class);
        final ObservationStore observations = mock(ObservationStore.class);
        final PipelineIntentScope scope = new PipelineIntentScope("cluster", 1, "cache", "pipeline", 1);
        final AtomicReference<PipelineStateMapping> mapping = new AtomicReference<>(
                new PipelineStateMapping("pipeline", 1, PipelineStateMapping.Phase.ACTIVE, null));
        final Observation observed = new Observation("pipeline", PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null, Instant.EPOCH);
        final ThreeLayerStorePort port;
        Fixture() {
            when(configuration.pipelineStates()).thenReturn(mappings);
            when(configuration.cacheRecoveries()).thenReturn(recoveries);
            when(recoveries.read("pipeline")).thenReturn(Optional.empty());
            when(configuration.preparations()).thenReturn(preparations);
            when(mappings.read("pipeline")).thenAnswer(call -> Optional.of(mapping.get()));
            when(bound.cache()).thenReturn(cache);
            when(bound.preparation()).thenReturn(new CachePreparation("setup",
                    new CacheAuthScope("cluster", 1, StorageBinding.Mode.OP, "cache", 1)));
            var node = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.NODE_SESSION, "node"),
                    new WorkloadOwner("node", "boot"), 1, 0, 1);
            when(preparations.read("pipeline")).thenReturn(Optional.of(new PipelinePreparation("pipeline", 1,
                    PipelinePreparation.Phase.COMPLETE, scope, UUID.randomUUID().toString(), node)));
            when(cache.pipelineObservations(scope)).thenReturn(observations);
            when(observations.read("pipeline")).thenReturn(Optional.of(observed));
            port = new ThreeLayerStorePort(configuration, bound, ignored -> {});
        }
    }
}
