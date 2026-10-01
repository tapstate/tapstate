package io.tapstate.app;

import io.tapstate.core.lifecycle.CardinalityBudget;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MetricProducerEpochsTest {
    private static final Instant ORIGINAL = Instant.parse("2026-10-01T00:00:00Z");
    private static final Instant FIRST = Instant.parse("2026-10-01T00:00:10.123456789Z");
    private static final Instant SECOND = FIRST.plusSeconds(10);
    private static final Map<String, String> TABLE = Map.of(MetricAttributes.PIPELINE_ID, "orders",
            MetricAttributes.TABLE_ID, "orders");

    @Test
    void aCheckpointRestoresTheSameNativeEpochWithoutAddingThePublishedTotalAgain() {
        MetricContinuation base = MetricContinuation.captureFacts(List.of(counter(ORIGINAL, ORIGINAL, 7)));
        MetricProducerEpochs live = new MetricProducerEpochs();
        assertThat(value(project(live, base, List.of(counter(FIRST, FIRST, 2)), FIRST))).isEqualTo(9);
        var snapshot = live.snapshot();
        assertThat(snapshot).singleElement().satisfies(state -> {
            assertThat(state.nativeStart()).isEqualTo(FIRST).isNotEqualTo(ORIGINAL);
            assertThat(state.nativeStart().getNano()).isEqualTo(123456789);
            assertThat(state.offsets()).isEmpty();
            assertThat(state.published()).singleElement().satisfies(point -> assertThat(point.value()).isEqualTo(9));
        });

        MetricProducerEpochs cold = MetricProducerEpochs.restore(snapshot, CardinalityBudget.folder());
        List<MetricFact> same = project(cold, base, List.of(counter(FIRST.plusSeconds(1), FIRST, 2)), FIRST.plusSeconds(1));
        assertThat(value(same)).isEqualTo(9);
        assertThat(same.getFirst().points().getFirst().startTime()).isEqualTo(ORIGINAL);
    }

    @Test
    void aRealNewNativeEpochContinuesFromItsPreviousLogicalHighwaterAfterColdRestore() {
        MetricContinuation base = MetricContinuation.captureFacts(List.of(counter(ORIGINAL, ORIGINAL, 7)));
        MetricProducerEpochs first = new MetricProducerEpochs();
        project(first, base, List.of(counter(FIRST, FIRST, 2)), FIRST);
        MetricProducerEpochs afterRestart = MetricProducerEpochs.restore(first.snapshot(), CardinalityBudget.folder());
        List<MetricFact> changed = project(afterRestart, base, List.of(counter(SECOND, SECOND, 3)), SECOND);
        assertThat(value(changed)).isEqualTo(12);
        var second = afterRestart.snapshot();
        assertThat(second).singleElement().satisfies(state -> {
            assertThat(state.nativeStart()).isEqualTo(SECOND);
            assertThat(state.offsets()).singleElement().satisfies(point -> assertThat(point.value()).isEqualTo(9));
            assertThat(state.published()).singleElement().satisfies(point -> assertThat(point.value()).isEqualTo(12));
        });

        MetricProducerEpochs coldAgain = MetricProducerEpochs.restore(second, CardinalityBudget.folder());
        assertThat(value(project(coldAgain, base, List.of(counter(SECOND.plusSeconds(1), SECOND, 3)), SECOND.plusSeconds(1))))
                .isEqualTo(12);
        assertThat(value(project(coldAgain, base, List.of(counter(SECOND.plusSeconds(2), FIRST, 2)), SECOND.plusSeconds(2))))
                .isEqualTo(12);
    }

    @Test
    void aHistogramCheckpointPreservesItsBucketsAndOriginalStartAcrossProducerRestart() {
        MetricContinuation base = MetricContinuation.captureFacts(List.of(histogram(ORIGINAL, ORIGINAL, 3)));
        MetricProducerEpochs live = new MetricProducerEpochs();
        assertThat(project(live, base, List.of(histogram(FIRST, FIRST, 1)), FIRST).getFirst().points().getFirst()
                .histogram().count()).isEqualTo(4);
        MetricProducerEpochs cold = MetricProducerEpochs.restore(live.snapshot(), CardinalityBudget.folder());
        MetricPoint continued = project(cold, base, List.of(histogram(SECOND, SECOND, 2)), SECOND)
                .getFirst().points().getFirst();
        assertThat(continued.histogram().count()).isEqualTo(6);
        assertThat(continued.histogram().sum()).isEqualTo(6.0);
        assertThat(continued.histogram().bucketCounts().getFirst()).isEqualTo(6);
        assertThat(continued.startTime()).isEqualTo(ORIGINAL);
        MetricProducerEpochs again = MetricProducerEpochs.restore(cold.snapshot(), CardinalityBudget.folder());
        assertThat(project(again, base, List.of(histogram(SECOND.plusSeconds(1), SECOND, 2)), SECOND.plusSeconds(1))
                .getFirst().points().getFirst().histogram().count()).isEqualTo(6);
    }

    private static List<MetricFact> project(MetricProducerEpochs epochs, MetricContinuation base,
            List<MetricFact> raw, Instant at) {
        var folder = CardinalityBudget.folder();
        List<MetricFact> folded = raw.stream().map(folder::fold).toList();
        return epochs.continueNative(raw, folded, base.apply(folded, at), at);
    }

    private static MetricFact counter(Instant at, Instant since, long count) {
        return MetricFact.single("tapstate.pipeline.snapshot.rows", MetricType.COUNTER, "{row}",
                MetricPoint.accumulated(TABLE, since, at, count));
    }

    private static MetricFact histogram(Instant at, Instant since, long count) {
        List<Long> buckets = new ArrayList<>(Collections.nCopies(HistogramBounds.RECORD_DELIVERY_DURATION.buckets(), 0L));
        buckets.set(0, count);
        return MetricFact.single("tapstate.pipeline.record.delivery.duration", MetricType.HISTOGRAM, HistogramBounds.UNIT,
                MetricPoint.distribution(TABLE, since, at, HistogramBounds.RECORD_DELIVERY_DURATION.value(count, count, buckets)));
    }

    private static long value(List<MetricFact> facts) { return facts.getFirst().points().getFirst().value(); }
}
