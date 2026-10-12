package io.tapstate.app;

import io.tapstate.control.core.CloudRuntimeStatus;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class StoreBackedCloudRuntimeStatusProviderTest {

    private static final Instant STARTED = Instant.parse("2026-09-28T12:00:00Z");
    private static final Instant NOW = STARTED.plusSeconds(90);

    @Test
    void snapshotCountsOnlyActiveObservedPipelinesAndCarriesTheLatestErrorTime() {
        InMemoryStorePort store = new InMemoryStorePort();
        observe(store, "running", PipelineState.RUNNING, null, NOW.minusSeconds(20));
        observe(store, "paused", PipelineState.PAUSED, null, NOW.minusSeconds(15));
        observe(store, "failed-old", PipelineState.FAILED,
                new ObservationFailure("pipeline.failed", Map.of()), NOW.minusSeconds(10));
        observe(store, "failed-new", PipelineState.FAILED,
                new ObservationFailure("pipeline.failed", Map.of()), NOW.minusSeconds(5));
        store.desired().save(new DesiredState("unobserved", PipelineState.RUNNING, "revision"));

        CloudRuntimeStatus status = new StoreBackedCloudRuntimeStatusProvider(
                store, "0.6.0", Clock.fixed(NOW, ZoneOffset.UTC), STARTED).snapshot();

        assertThat(status.runtimeVersion()).isEqualTo("0.6.0");
        assertThat(status.uptimeMillis()).isEqualTo(90_000L);
        assertThat(status.activePipelines()).isEqualTo(2);
        assertThat(status.lastErrorAt()).isEqualTo(NOW.minusSeconds(5));
    }

    @Test
    void aClockCorrectionCannotProduceNegativeUptime() {
        InMemoryStorePort store = new InMemoryStorePort();

        CloudRuntimeStatus status = new StoreBackedCloudRuntimeStatusProvider(
                store, "0.6.0", Clock.fixed(STARTED.minusSeconds(1), ZoneOffset.UTC), STARTED).snapshot();

        assertThat(status.uptimeMillis()).isZero();
        assertThat(status.activePipelines()).isZero();
        assertThat(status.lastErrorAt()).isNull();
    }

    private static void observe(
            InMemoryStorePort store,
            String id,
            PipelineState state,
            ObservationFailure failure,
            Instant observedAt) {
        store.desired().save(new DesiredState(id, state, "revision"));
        store.observations().save(new Observation(id, state, Map.of(), Map.of(), Map.of(), failure, observedAt));
    }
}
