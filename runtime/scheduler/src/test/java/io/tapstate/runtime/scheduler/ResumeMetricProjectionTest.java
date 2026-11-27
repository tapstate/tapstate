package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.CdcDeliveryTotals;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.spi.store.ResumeMetrics;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ResumeMetricProjectionTest {
    private static final Instant OLD = Instant.ofEpochSecond(1), NEW = Instant.ofEpochSecond(2);
    private static final WorkloadClaimFence FENCE = new WorkloadClaimFence(
            new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "p"), new WorkloadOwner("n", "b"), 1, 1, 1);

    @Test void oldOperationsRemainInTheTotalWhenOnlyAnotherOperationHasNewRows() {
        var baseline = new ResumeMetrics.Baseline(2, 1, FENCE, 8, List.of(records("insert", OLD, 7)), false);
        var live = records("update", NEW, 2);
        var window = ResumeMetricProjection.window(3, 12, ResumeMetrics.Prior.ABSENT,
                Map.of(ResumeMetrics.Producer.OUT, NEW.toEpochMilli()));
        var points = ResumeMetricProjection.apply(baseline, List.of(live), window).getFirst().points();
        assertThat(points).extracting(MetricPoint::value).containsExactlyInAnyOrder(2L, 7L);
        assertThat(points).filteredOn(point -> point.value() == 7).singleElement()
                .satisfies(point -> {
                    assertThat(point.startTime()).isEqualTo(OLD);
                    assertThat(point.observedAt()).isEqualTo(NEW);
                });
        assertThat(ResumeMetricProjection.apply(baseline, List.of(), window)).isEmpty();
    }

    @Test void idleSinkDeliveryCountKeepsSavedOutputVisible() {
        assertIdleSinkCounterKeepsSavedOutputVisible(CdcDeliveryTotals.COUNT_METRIC, "{record}");
    }

    @Test void idleSinkDeliveryDurationKeepsSavedOutputVisible() {
        assertIdleSinkCounterKeepsSavedOutputVisible(CdcDeliveryTotals.TOTAL_MILLIS_METRIC, "ms");
    }

    private void assertIdleSinkCounterKeepsSavedOutputVisible(String name, String unit) {
        var saved = records("insert", OLD, 7);
        var baseline = new ResumeMetrics.Baseline(2, 1, FENCE, 8, List.of(saved), false);
        var idle = MetricFact.single(name, MetricType.COUNTER, unit,
                MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, "p"), NEW, NEW, 0));
        var window = ResumeMetricProjection.window(3, 12, ResumeMetrics.Prior.ABSENT,
                Map.of(ResumeMetrics.Producer.OUT, NEW.toEpochMilli()));

        var expected = MetricFact.single(saved.name(), saved.type(), saved.unit(),
                MetricPoint.accumulated(saved.points().getFirst().attributes(), OLD, NEW, 7));
        assertThat(ResumeMetricProjection.apply(baseline, List.of(idle), window))
                .containsExactlyInAnyOrder(idle, expected);
    }

    @Test void onlyTheRestartedProducerLosesItsBaselineAcrossPublications() {
        var input = ResumeMetrics.Producer.IN;
        var output = ResumeMetrics.Producer.OUT;
        var stage = ResumeMetrics.Producer.STAGE;
        var starts = Map.of(input, 10L, output, 20L, stage, 30L);
        var first = ResumeMetricProjection.window(3, 12, ResumeMetrics.Prior.ABSENT, starts);
        var prior = new ResumeMetrics.Prior(true, OLD, first, starts.keySet());
        var restarted = ResumeMetricProjection.window(3, 12, prior, Map.of(input, 10L, output, 40L));
        assertThat(restarted.slots()).containsEntry(input, new ResumeMetrics.Slot(10, true))
                .containsEntry(output, new ResumeMetrics.Slot(40, false)).containsEntry(stage, new ResumeMetrics.Slot(30, true));
        var recovered = new ResumeMetrics.Prior(true, NEW, restarted, starts.keySet());
        assertThat(ResumeMetricProjection.window(3, 12, recovered, Map.of(output, 40L))).isEqualTo(restarted);
        assertThat(ResumeMetricProjection.window(3, 13, recovered, starts).slots().values())
                .noneMatch(ResumeMetrics.Slot::carry);
        var legacy = new ResumeMetrics.Prior(true, OLD, null, java.util.Set.of(output));
        assertThat(ResumeMetricProjection.window(3, 12, legacy, starts).slots())
                .containsEntry(input, new ResumeMetrics.Slot(10, true)).containsEntry(output, new ResumeMetrics.Slot(20, false));
    }

    private static MetricFact records(String operation, Instant since, long count) {
        return MetricFact.single("tapstate.pipeline.records", MetricType.COUNTER, "{record}",
                MetricPoint.accumulated(Map.of("direction", "out", "op", operation), since, since, count));
    }
}
