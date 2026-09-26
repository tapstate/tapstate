package io.tapstate.app;

import io.tapstate.control.core.TelemetryCleanupReporter;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.metrics.MetricsExport;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class TelemetryCleanupHealthTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-09-27T06:00:00Z"), ZoneOffset.UTC);

    @Test
    void failureProducesLowCardinalityHealthAndAnOldOwnerEventOffer() {
        List<PipelineEvent> offered = new ArrayList<>();
        AtomicReference<Supplier<List<MetricFact>>> process = new AtomicReference<>();
        MetricsExport export = new MetricsExport() {
            @Override public void offer(String id, PipelineState state, Instant at, List<MetricFact> facts) { }
            @Override public void forgetPipelinesOutside(Collection<String> ids) { }
            @Override public void observeProcess(String source, Supplier<List<MetricFact>> facts) {
                assertThat(source).isEqualTo("telemetry-cleanup");
                process.set(facts);
            }
        };
        TelemetryCleanupHealth health = new TelemetryCleanupHealth(CLOCK, export, offered::add);
        assertThat(process.get()).isNotNull();
        assertThat(process.get().get()).isEmpty();

        health.failed(new TelemetryCleanupReporter.Failure("flow", "inc-old", OptionalLong.of(4),
                "event-history", true));

        assertThat(health.health()).isEqualTo(new TelemetryCleanupHealth.Health(1, 1, true));
        assertThat(offered).singleElement().satisfies(event -> {
            assertThat(event.pipelineId()).isEqualTo("flow");
            assertThat(event.pipelineIncarnationId()).isEqualTo("inc-old");
            assertThat(event.executionGeneration()).isEqualTo(4);
            assertThat(event.kind()).isEqualTo(PipelineEvent.Kind.CLEANUP_INCOMPLETE);
            assertThat(event.reason()).isEqualTo("event-history");
        });
        assertThat(process.get()).isNotNull();
        assertThat(process.get().get()).extracting(MetricFact::name).containsExactly(
                "tapstate.process.telemetry.cleanup.failure",
                "tapstate.process.telemetry.cleanup.rejected",
                "tapstate.process.telemetry.cleanup.degraded");
        assertThat(process.get().get()).allSatisfy(fact ->
                assertThat(fact.points()).allSatisfy(point -> assertThat(point.attributes()).isEmpty()));
    }

    @Test
    void noExistingExecutionStillReportsFailureWithoutInventingAnEventOwner() {
        List<PipelineEvent> offered = new ArrayList<>();
        TelemetryCleanupHealth health = new TelemetryCleanupHealth(CLOCK, MetricsExport.none(), offered::add);

        health.failed(new TelemetryCleanupReporter.Failure("flow", "inc-old", OptionalLong.empty(),
                "observation", false));

        assertThat(offered).isEmpty();
        assertThat(health.health()).isEqualTo(new TelemetryCleanupHealth.Health(1, 0, true));
    }
}
