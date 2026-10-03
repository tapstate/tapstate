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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MetricProducerEpochsTest {
    private static final Instant ORIGINAL = Instant.parse("2026-10-01T00:00:00Z");
    private static final Instant FIRST = Instant.parse("2026-10-01T00:00:10.123456789Z");
    private static final Instant SECOND = FIRST.plusSeconds(10);
    private static final Map<String, String> TABLE = Map.of(MetricAttributes.PIPELINE_ID, "orders",
            MetricAttributes.TABLE_ID, "orders");

    @Test
    void quietNativePressureAccountsRetainTheirKnownPointsAndActualMeasurementTimes() {
        MetricContinuation empty = MetricContinuation.captureFacts(List.of());
        MetricProducerEpochs epochs = new MetricProducerEpochs();
        MetricFact refused = pressureCounter("source", FIRST, ORIGINAL, 155);
        MetricFact retried = pressureHistogram(HistogramBounds.STAGE_OUTPUT_RETRY_DURATION,
                Map.of(MetricAttributes.PIPELINE_ID, "orders", MetricAttributes.STAGE, "source"), FIRST, ORIGINAL, 11);
        MetricFact waited = pressureHistogram(HistogramBounds.SINK_BACKPRESSURE_DURATION,
                Map.of(MetricAttributes.PIPELINE_ID, "orders"), FIRST, ORIGINAL, 13);
        MetricFact gauge = MetricFact.single("tapstate.pipeline.lag", MetricType.GAUGE, "s",
                MetricPoint.reading(TABLE, FIRST, 30));
        project(epochs, empty, List.of(refused, retried, waited, gauge), FIRST);
        var checkpoint = epochs.snapshot();

        List<MetricFact> quiet = project(epochs, empty, List.of(), SECOND);
        assertThat(quiet).extracting(MetricFact::name).containsExactlyInAnyOrder(
                refused.name(), retried.name(), waited.name());
        for (MetricFact old : List.of(refused, retried, waited)) {
            assertThat(named(quiet, old.name()).points()).containsExactlyElementsOf(old.points());
        }
        assertThat(epochs.snapshot()).isEqualTo(checkpoint);

        List<MetricFact> partlyMeasured = project(epochs, empty,
                List.of(pressureCounter("transform", SECOND, SECOND, 2)), SECOND);
        assertThat(named(partlyMeasured, refused.name()).points()).contains(refused.points().getFirst()).hasSize(2);
        assertThat(named(partlyMeasured, retried.name()).points()).containsExactlyElementsOf(retried.points());
        assertThat(named(partlyMeasured, waited.name()).points()).containsExactlyElementsOf(waited.points());
        assertThat(partlyMeasured).noneMatch(fact -> fact.type() == MetricType.GAUGE);

        Instant third = SECOND.plusSeconds(10);
        List<MetricFact> measuredAgain = project(epochs, empty,
                List.of(pressureCounter("source", third, third, 3)), third);
        MetricPoint source = named(measuredAgain, refused.name()).points().stream()
                .filter(point -> "source".equals(point.attributes().get(MetricAttributes.STAGE))).findFirst().orElseThrow();
        assertThat(source.value()).isEqualTo(158);
        assertThat(source.startTime()).isEqualTo(ORIGINAL);
    }

    @Test
    void explicitNullOrMixedNativeEpochsDoNotBorrowKnownPointsFromTheRejectedGroup() {
        MetricContinuation empty = MetricContinuation.captureFacts(List.of());
        MetricProducerEpochs epochs = new MetricProducerEpochs();
        MetricFact rows = counter(FIRST, FIRST, 7);
        Map<String, String> source = Map.of(MetricAttributes.PIPELINE_ID, "orders", MetricAttributes.STAGE, "source");
        MetricFact retry = pressureHistogram(HistogramBounds.STAGE_OUTPUT_RETRY_DURATION, source, FIRST, FIRST, 2);
        project(epochs, empty, List.of(rows, retry), FIRST);
        var known = epochs.snapshot();

        MetricFact nullEpoch = pressureHistogram(HistogramBounds.STAGE_OUTPUT_RETRY_DURATION, source, SECOND, null, 1);
        List<MetricFact> rejectedNull = project(epochs, empty, List.of(nullEpoch), SECOND);
        assertThat(named(rejectedNull, retry.name()).points()).isEmpty();
        assertThat(epochs.snapshot()).isEqualTo(known);

        MetricFact mixed = new MetricFact(rows.name(), MetricType.COUNTER, "{row}", List.of(
                MetricPoint.accumulated(TABLE, FIRST, SECOND, 1),
                MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, "orders",
                        MetricAttributes.TABLE_ID, "other"), SECOND, SECOND, 1)));
        List<MetricFact> rejectedMixed = project(epochs, empty, List.of(mixed), SECOND);
        assertThat(named(rejectedMixed, rows.name()).points()).isEmpty();
        assertThat(epochs.snapshot()).isEqualTo(known);
    }

    @Test
    void aCurrentTypeOrUnitConflictCannotRecoverACachedAccount() {
        MetricContinuation empty = MetricContinuation.captureFacts(List.of());
        MetricProducerEpochs epochs = new MetricProducerEpochs();
        MetricFact rows = counter(FIRST, FIRST, 7);
        project(epochs, empty, List.of(rows), FIRST);
        var known = epochs.snapshot();

        MetricFact changedType = MetricFact.single(rows.name(), MetricType.GAUGE, rows.unit(),
                MetricPoint.reading(TABLE, SECOND, 99));
        assertThat(epochs.continueNative(List.of(), List.of(), List.of(changedType), SECOND)).containsExactly(changedType);
        MetricFact changedUnit = MetricFact.single(rows.name(), MetricType.COUNTER, "By",
                MetricPoint.accumulated(TABLE, SECOND, SECOND, 99));
        assertThat(epochs.continueNative(List.of(), List.of(), List.of(changedUnit), SECOND)).containsExactly(changedUnit);
        assertThat(epochs.snapshot()).isEqualTo(known);
    }

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

    @Test
    void aLowerCarriedFloorCannotReplaceAMoreRecentKnownWholePoint() {
        MetricContinuation counterBase = MetricContinuation.captureFacts(List.of(counter(ORIGINAL, ORIGINAL, 7)));
        MetricProducerEpochs counters = new MetricProducerEpochs();
        MetricPoint knownCounter = project(counters, counterBase, List.of(counter(FIRST, FIRST, 2)), FIRST)
                .getFirst().points().getFirst();
        MetricPoint quietCounter = project(counters, counterBase, List.of(), SECOND).getFirst().points().getFirst();
        assertThat(quietCounter.value()).isEqualTo(9);
        assertThat(quietCounter).isEqualTo(knownCounter);

        Map<String, String> source = Map.of(MetricAttributes.PIPELINE_ID, "orders", MetricAttributes.STAGE, "source");
        HistogramBounds bounds = HistogramBounds.STAGE_OUTPUT_RETRY_DURATION;
        MetricFact floor = pressureHistogram(bounds, source, ORIGINAL, ORIGINAL, 3);
        MetricContinuation histogramBase = MetricContinuation.captureFacts(List.of(floor));
        MetricProducerEpochs histograms = new MetricProducerEpochs();
        MetricPoint knownHistogram = project(histograms, histogramBase,
                List.of(pressureHistogram(bounds, source, FIRST, FIRST, 1)), FIRST).getFirst().points().getFirst();
        MetricPoint quietHistogram = project(histograms, histogramBase, List.of(), SECOND).getFirst().points().getFirst();
        assertThat(quietHistogram).isEqualTo(knownHistogram);
        var histogram = knownHistogram.histogram();
        MetricFact lowerSum = MetricFact.single(floor.name(), MetricType.HISTOGRAM, HistogramBounds.UNIT,
                MetricPoint.distribution(source, ORIGINAL, SECOND,
                        bounds.value(histogram.count(), histogram.sum() / 2, histogram.bucketCounts())));
        assertThat(histograms.continueNative(List.of(), List.of(), List.of(lowerSum), SECOND)
                .getFirst().points().getFirst()).isEqualTo(knownHistogram);

        // The registered fact boundary rejects an unregistered distribution before any epoch can accept it.
        assertThatThrownBy(() -> MetricFact.single("tapstate.pipeline.snapshot.rows", MetricType.HISTOGRAM,
                HistogramBounds.UNIT, pressureHistogram(bounds, TABLE, FIRST, FIRST, 1).points().getFirst()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("no bucket bounds are registered");
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

    private static MetricFact pressureCounter(String stage, Instant at, Instant since, long count) {
        return MetricFact.single("tapstate.pipeline.stage.output.refused", MetricType.COUNTER, "{offer}",
                MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, "orders", MetricAttributes.STAGE, stage), since, at, count));
    }

    private static MetricFact pressureHistogram(HistogramBounds bounds, Map<String, String> attributes,
            Instant at, Instant since, long count) {
        List<Long> buckets = new ArrayList<>(Collections.nCopies(bounds.buckets(), 0L));
        buckets.set(0, count);
        return MetricFact.single(bounds.instrument(), MetricType.HISTOGRAM, HistogramBounds.UNIT,
                MetricPoint.distribution(attributes, since, at, bounds.value(count, count * bounds.bounds().getFirst() / 2, buckets)));
    }

    private static MetricFact named(List<MetricFact> facts, String name) {
        return facts.stream().filter(fact -> fact.name().equals(name)).findFirst().orElseThrow();
    }

    private static MetricFact histogram(Instant at, Instant since, long count) {
        List<Long> buckets = new ArrayList<>(Collections.nCopies(HistogramBounds.RECORD_DELIVERY_DURATION.buckets(), 0L));
        buckets.set(0, count);
        return MetricFact.single("tapstate.pipeline.record.delivery.duration", MetricType.HISTOGRAM, HistogramBounds.UNIT,
                MetricPoint.distribution(TABLE, since, at, HistogramBounds.RECORD_DELIVERY_DURATION.value(count, count, buckets)));
    }

    private static long value(List<MetricFact> facts) { return facts.getFirst().points().getFirst().value(); }
}
