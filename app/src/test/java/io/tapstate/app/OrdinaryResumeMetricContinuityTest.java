package io.tapstate.app;

import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** A native processor restart can occur without a new durable observation scope. */
class OrdinaryResumeMetricContinuityTest {
    private static final String PIPELINE = "orders";
    private static final Instant START = Instant.parse("2026-09-30T00:00:00Z");

    @Test
    void aQuietPauseAndSameScopeResumeRetainEveryActualSettlementAndTheOriginalStart() {
        var scopes = new ObservationScopeRegistry();
        var scope = scopes.begin(PIPELINE, "inc-a", 1);
        scopes.continueFrame(frame(1, PipelineState.RUNNING, START, 12_128, 13_000), scope);
        var quiet = scopes.continueFrame(frame(2, PipelineState.PAUSED, START, null, 13_001), scope);
        assertThat(points(quiet, "out")).isEmpty();

        var resumed = scopes.continueFrame(frame(3, PipelineState.RUNNING, START.plusSeconds(3), 129, 13_002), scope);
        assertThat(value(resumed, "out")).isEqualTo(12_257);
        assertThat(points(resumed, "out").getFirst().startTime()).isEqualTo(START);
        assertThat(value(resumed, "in")).isEqualTo(13_002);
        var next = scopes.continueFrame(frame(4, PipelineState.RUNNING, START.plusSeconds(3), 130, 13_003), scope);
        assertThat(value(next, "out")).isEqualTo(12_258);
        assertThat(value(next, "in")).isEqualTo(13_003);
        assertThat(scopes.current(PIPELINE)).contains(scope);
    }

    @Test
    void repeatedNativeRestartsNeverAddAnAlreadyReprojectedBaselineTwice() {
        var scopes = new ObservationScopeRegistry();
        var scope = scopes.begin(PIPELINE, "inc-a", 1);
        scopes.continueFrame(frame(1, PipelineState.RUNNING, START, 7, 10), scope);
        scopes.continueFrame(frame(2, PipelineState.PAUSED, START, null, 11), scope);
        var second = scopes.continueFrame(frame(3, PipelineState.RUNNING, START.plusSeconds(3), 2, 12), scope);
        assertThat(value(second, "out")).isEqualTo(9);
        scopes.continueFrame(frame(4, PipelineState.PAUSED, START, null, 13), scope);
        var third = scopes.continueFrame(frame(5, PipelineState.RUNNING, START.plusSeconds(5), 3, 14), scope);
        assertThat(value(third, "out")).isEqualTo(12);
        assertThat(points(third, "out").getFirst().startTime()).isEqualTo(START);
        var next = scopes.continueFrame(frame(6, PipelineState.RUNNING, START.plusSeconds(5), 4, 15), scope);
        assertThat(value(next, "out")).isEqualTo(13);
        assertThat(value(next, "in")).isEqualTo(15);
    }

    @Test
    void alternatingOperationsKeepTheLatestKnownTotalWhenANativePointIsTemporarilyMissing() {
        var scopes = new ObservationScopeRegistry();
        var scope = scopes.begin(PIPELINE, "inc-a", 1);
        scopes.continueFrame(records(1, START, Map.of("read", 12_000L, "update", 128L)), scope);
        var resumed = scopes.continueFrame(records(3, START.plusSeconds(3), Map.of("update", 129L)), scope);
        assertThat(total(resumed, "tapstate.pipeline.records")).isEqualTo(12_257);
        var alternate = scopes.continueFrame(records(4, START.plusSeconds(3), Map.of("read", 3L)), scope);
        assertThat(total(alternate, "tapstate.pipeline.records")).isEqualTo(12_260);
        var nextEpoch = scopes.continueFrame(records(5, START.plusSeconds(5), Map.of("update", 1L)), scope);
        assertThat(total(nextEpoch, "tapstate.pipeline.records")).isEqualTo(12_261);
    }

    @Test
    void alternatingHistogramTablesKeepEverySettledBucketAcrossNativeEpochs() {
        var scopes = new ObservationScopeRegistry();
        var scope = scopes.begin(PIPELINE, "inc-a", 1);
        scopes.continueFrame(histograms(1, START, Map.of("a", 7L, "b", 2L)), scope);
        scopes.continueFrame(histograms(3, START.plusSeconds(3), Map.of("a", 3L)), scope);
        var alternate = scopes.continueFrame(histograms(4, START.plusSeconds(3), Map.of("b", 1L)), scope);
        assertThat(histogramCount(alternate)).isEqualTo(13);
        var nextEpoch = scopes.continueFrame(histograms(5, START.plusSeconds(5), Map.of("a", 1L)), scope);
        assertThat(histogramCount(nextEpoch)).isEqualTo(14);
        assertThat(nextEpoch.observation().facts().getFirst().points()).allSatisfy(point -> {
            assertThat(point.startTime()).isEqualTo(START);
            assertThat(point.histogram().bucketCounts().stream().mapToLong(Long::longValue).sum())
                    .isEqualTo(point.histogram().count());
        });
    }

    @Test
    void ordinaryNativeRestartAfterARebuildingResumeAddsItsBaselineOnlyOnce() {
        var scopes = new ObservationScopeRegistry();
        var first = scopes.begin(PIPELINE, "inc-a", 1);
        scopes.continueFrame(records(1, START, Map.of("update", 7L)), first);
        scopes.prepareRebuildingResume(PIPELINE, Optional.empty());
        var rebuilt = scopes.begin(PIPELINE, "inc-a", 2);
        assertThat(total(scopes.continueFrame(records(2, START.plusSeconds(2), Map.of("update", 2L)), rebuilt),
                "tapstate.pipeline.records")).isEqualTo(9);
        assertThat(total(scopes.continueFrame(records(3, START.plusSeconds(3), Map.of("update", 3L)), rebuilt),
                "tapstate.pipeline.records")).isEqualTo(12);
        assertThat(total(scopes.continueFrame(records(4, START.plusSeconds(3), Map.of("update", 4L)), rebuilt),
                "tapstate.pipeline.records")).isEqualTo(13);
        scopes.prepareRebuildingResume(PIPELINE, Optional.empty());
        var another = scopes.begin(PIPELINE, "inc-a", 3);
        assertThat(total(scopes.continueFrame(records(5, START.plusSeconds(5), Map.of("update", 1L)), another),
                "tapstate.pipeline.records")).isEqualTo(14);
    }

    @Test
    void aQuietPauseCannotEraseTheBaselinePreparedForARebuild() {
        var scopes = new ObservationScopeRegistry();
        var first = scopes.begin(PIPELINE, "inc-a", 1);
        scopes.continueFrame(frame(1, PipelineState.RUNNING, START, 7, 10), first);
        scopes.continueFrame(frame(2, PipelineState.PAUSED, START, null, 11), first);
        scopes.prepareRebuildingResume(PIPELINE, Optional.empty());
        var rebuilt = scopes.begin(PIPELINE, "inc-a", 2);
        var resumed = scopes.continueFrame(frame(3, PipelineState.RUNNING, START.plusSeconds(3), 2, 1), rebuilt);
        assertThat(value(resumed, "out")).isEqualTo(9);
        assertThat(value(resumed, "in")).isEqualTo(12);
    }

    @Test
    void anUncertainFirstReadingAfterRebuildCannotLoseTheBaselineOfTheNextRebuild() {
        var scopes = new ObservationScopeRegistry();
        var first = scopes.begin(PIPELINE, "inc-a", 1);
        scopes.continueFrame(records(1, START, Map.of("update", 7L, "read", 5L)), first);
        scopes.prepareRebuildingResume(PIPELINE, Optional.empty());
        var second = scopes.begin(PIPELINE, "inc-a", 2);
        var mixed = List.of(
                MetricPoint.accumulated(attributes("out"), START.plusSeconds(2), START.plusSeconds(3), 1),
                MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, PIPELINE,
                        MetricAttributes.TABLE_ID, "orders", MetricAttributes.DIRECTION, "out",
                        MetricAttributes.OP, "read"), START.plusSeconds(3), START.plusSeconds(3), 1));
        var unavailable = scopes.continueFrame(facts(3, List.of(new MetricFact("tapstate.pipeline.records",
                MetricType.COUNTER, "{record}", mixed))), second);
        assertThat(points(unavailable, "out")).isEmpty();
        scopes.prepareRebuildingResume(PIPELINE, Optional.empty());
        var third = scopes.begin(PIPELINE, "inc-a", 3);
        var continued = scopes.continueFrame(records(5, START.plusSeconds(5), Map.of("update", 2L)), third);
        assertThat(total(continued, "tapstate.pipeline.records")).isEqualTo(14);
    }

    @Test
    void anUnknownHistogramAfterRebuildKeepsItsKnownPredecessorForAnotherRebuild() {
        var scopes = new ObservationScopeRegistry();
        var first = scopes.begin(PIPELINE, "inc-a", 1);
        scopes.continueFrame(histograms(1, START, Map.of("a", 7L, "b", 2L)), first);
        scopes.prepareRebuildingResume(PIPELINE, Optional.empty());
        var second = scopes.begin(PIPELINE, "inc-a", 2);
        var unknown = scopes.continueFrame(histograms(2, null, Map.of("a", 1L)), second);
        assertThat(unknown.observation().facts().getFirst().points()).isEmpty();
        scopes.prepareRebuildingResume(PIPELINE, Optional.empty());
        var third = scopes.begin(PIPELINE, "inc-a", 3);
        var continued = scopes.continueFrame(histograms(3, START.plusSeconds(3), Map.of("a", 2L)), third);
        assertThat(histogramCount(continued)).isEqualTo(11);
    }

    @Test
    void foldedCounterAndHistogramTotalsGrowOnlyByTheNewNativeDelta() {
        for (boolean histogram : List.of(false, true)) {
            var scopes = new ObservationScopeRegistry();
            var scope = scopes.begin(PIPELINE, "inc-a", 1);
            Map<String, Long> initial = new java.util.LinkedHashMap<>();
            for (int table = 0; table < 1_010; table++) { initial.put("table-" + table, 1L); }
            scopes.continueFrame(tables(1, START, initial, histogram), scope);
            scopes.prepareRebuildingResume(PIPELINE, Optional.empty());
            var rebuilt = scopes.begin(PIPELINE, "inc-a", 2);
            for (int count = 2; count <= 4; count++) {
                var frame = scopes.continueFrame(tables(count + 1, START.plusSeconds(3),
                        Map.of("new-table", (long) count), histogram), rebuilt);
                assertThat(frame.observation().facts().getFirst().points()).hasSize(1_001);
                assertThat(histogram ? histogramCount(frame) : total(frame, "tapstate.pipeline.snapshot.rows"))
                        .isEqualTo(1_010 + count);
            }
            var restarted = scopes.continueFrame(tables(6, START.plusSeconds(6),
                    Map.of("another-table", 5L), histogram), rebuilt);
            assertThat(restarted.observation().facts().getFirst().points()).hasSize(1_001);
            assertThat(histogram ? histogramCount(restarted) : total(restarted, "tapstate.pipeline.snapshot.rows"))
                    .isEqualTo(1_019);
        }
    }

    @Test
    void distinctStageProducersCanRestartIndependently() {
        var scopes = new ObservationScopeRegistry();
        var scope = scopes.begin(PIPELINE, "inc-a", 1);
        scopes.continueFrame(stages(2, START, 7, START.plusSeconds(1), 5), scope);
        var next = scopes.continueFrame(stages(3, START.plusSeconds(3), 2, START.plusSeconds(1), 6), scope);
        var points = next.observation().facts().getFirst().points();
        assertThat(points).anySatisfy(point -> {
            assertThat(point.attributes().get(MetricAttributes.STAGE)).isEqualTo("sink");
            assertThat(point.value()).isEqualTo(9);
            assertThat(point.startTime()).isEqualTo(START);
        });
        assertThat(points).anySatisfy(point -> {
            assertThat(point.attributes().get(MetricAttributes.STAGE)).isEqualTo("source");
            assertThat(point.value()).isEqualTo(6);
            assertThat(point.startTime()).isEqualTo(START.plusSeconds(1));
        });
    }

    @Test
    void mixedAndUnknownNativeEpochsStayAbsentWithoutChangingTheKnownBaseline() {
        var scopes = new ObservationScopeRegistry();
        var scope = scopes.begin(PIPELINE, "inc-a", 1);
        scopes.continueFrame(records(1, START, Map.of("update", 7L, "read", 5L)), scope);
        var mixed = List.of(
                MetricPoint.accumulated(attributes("out"), START, START.plusSeconds(3), 8),
                MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, PIPELINE,
                        MetricAttributes.TABLE_ID, "orders", MetricAttributes.DIRECTION, "out",
                        MetricAttributes.OP, "read"), START.plusSeconds(3), START.plusSeconds(3), 1));
        var uncertain = scopes.continueFrame(facts(3, List.of(new MetricFact("tapstate.pipeline.records",
                MetricType.COUNTER, "{record}", mixed))), scope);
        assertThat(points(uncertain, "out")).isEmpty();
        var valid = scopes.continueFrame(records(4, START.plusSeconds(3), Map.of("update", 2L)), scope);
        assertThat(total(valid, "tapstate.pipeline.records")).isEqualTo(14);

        scopes.continueFrame(histograms(5, START.plusSeconds(5), Map.of("a", 7L)), scope);
        var unknown = scopes.continueFrame(histograms(6, null, Map.of("a", 1L)), scope);
        assertThat(unknown.observation().facts().getFirst().points()).isEmpty();
        var known = scopes.continueFrame(histograms(7, START.plusSeconds(7), Map.of("a", 2L)), scope);
        assertThat(histogramCount(known)).isEqualTo(9);
    }

    @Test
    void oldFramesAndOldNativeEpochsCannotChangeTheNextCumulativeBaseline() {
        var scopes = new ObservationScopeRegistry();
        var scope = scopes.begin(PIPELINE, "inc-a", 1);
        scopes.continueFrame(records(1, START, Map.of("update", 7L)), scope);
        scopes.continueFrame(records(6, START.plusSeconds(5), Map.of("update", 2L)), scope);
        scopes.continueFrame(records(4, START.plusSeconds(4), Map.of("update", 100L)), scope);
        var oldProducer = scopes.continueFrame(records(7, START, Map.of("update", 100L)), scope);
        assertThat(total(oldProducer, "tapstate.pipeline.records")).isEqualTo(9);
        var next = scopes.continueFrame(records(8, START.plusSeconds(8), Map.of("update", 1L)), scope);
        assertThat(total(next, "tapstate.pipeline.records")).isEqualTo(10);
    }

    @Test
    void aStoppedScopeKeepsKnownTotalsAndAFreshStartOrIncarnationDiscardsThem() {
        var scopes = new ObservationScopeRegistry();
        var first = scopes.begin(PIPELINE, "inc-a", 1);
        scopes.continueFrame(frame(1, PipelineState.RUNNING, START, 7, 10), first);
        var gauge = MetricFact.single("tapstate.pipeline.lag", MetricType.GAUGE, "s",
                MetricPoint.reading(Map.of(MetricAttributes.PIPELINE_ID, PIPELINE,
                        MetricAttributes.TABLE_ID, "orders"), START.plusSeconds(2), 3));
        var stopped = new ObservationPublisher.Prepared(new Observation(PIPELINE, PipelineState.STOPPED,
                Map.of(), Map.of(), Map.of(), null, START.plusSeconds(2), List.of(gauge)), false,
                Map.of(), Map.of(), Map.of());
        var finalFrame = scopes.continueFrame(stopped, first);
        assertThat(value(finalFrame, "out")).isEqualTo(7);
        assertThat(finalFrame.observation().facts()).anySatisfy(fact -> {
            assertThat(fact.name()).isEqualTo("tapstate.pipeline.lag");
            assertThat(fact.points().getFirst().value()).isEqualTo(3);
        });
        scopes.clearContinuation(PIPELINE);
        var next = scopes.begin(PIPELINE, "inc-a", 2);
        assertThat(value(scopes.continueFrame(frame(3, PipelineState.RUNNING, START.plusSeconds(3), 1, 1), next),
                "out")).isEqualTo(1);
        scopes.forgetIncarnation(PIPELINE, "inc-a");
        var recreated = scopes.begin(PIPELINE, "inc-b", 3);
        assertThat(value(scopes.continueFrame(frame(4, PipelineState.RUNNING, START.plusSeconds(4), 2, 2), recreated),
                "out")).isEqualTo(2);
    }

    private static ObservationPublisher.Prepared stages(int at, Instant sinkSince, long sink,
            Instant sourceSince, long source) {
        return facts(at, List.of(new MetricFact("tapstate.pipeline.stage.output.refused", MetricType.COUNTER,
                "{offer}", List.of(
                MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, PIPELINE,
                        MetricAttributes.STAGE, "sink"), sinkSince, START.plusSeconds(at), sink),
                MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, PIPELINE,
                        MetricAttributes.STAGE, "source"), sourceSince, START.plusSeconds(at), source)))));
    }

    private static ObservationPublisher.Prepared tables(int at, Instant since, Map<String, Long> counts,
            boolean histogram) {
        if (histogram) { return histograms(at, since, counts); }
        return facts(at, List.of(new MetricFact("tapstate.pipeline.snapshot.rows", MetricType.COUNTER, "{row}",
                counts.entrySet().stream().map(entry -> MetricPoint.accumulated(
                        Map.of(MetricAttributes.PIPELINE_ID, PIPELINE, MetricAttributes.TABLE_ID, entry.getKey()),
                        since, START.plusSeconds(at), entry.getValue())).toList())));
    }

    private static ObservationPublisher.Prepared records(int at, Instant since, Map<String, Long> counts) {
        var points = counts.entrySet().stream().map(entry -> MetricPoint.accumulated(
                Map.of(MetricAttributes.PIPELINE_ID, PIPELINE, MetricAttributes.TABLE_ID, "orders",
                        MetricAttributes.DIRECTION, "out", MetricAttributes.OP, entry.getKey()),
                since, START.plusSeconds(at), entry.getValue())).toList();
        return facts(at, List.of(new MetricFact("tapstate.pipeline.records", MetricType.COUNTER, "{record}", points)));
    }

    private static ObservationPublisher.Prepared histograms(int at, Instant since, Map<String, Long> counts) {
        var points = counts.entrySet().stream().map(entry -> {
            var buckets = new java.util.ArrayList<>(java.util.Collections.nCopies(
                    HistogramBounds.RECORD_DELIVERY_DURATION.buckets(), 0L));
            buckets.set(0, entry.getValue());
            return MetricPoint.distribution(Map.of(MetricAttributes.PIPELINE_ID, PIPELINE,
                    MetricAttributes.TABLE_ID, entry.getKey()), since, START.plusSeconds(at),
                    new HistogramValue(entry.getValue(), entry.getValue() * 0.001,
                            HistogramBounds.RECORD_DELIVERY_DURATION.bounds(), buckets));
        }).toList();
        return facts(at, List.of(new MetricFact(HistogramBounds.RECORD_DELIVERY_DURATION.instrument(),
                MetricType.HISTOGRAM, HistogramBounds.UNIT, points)));
    }

    private static ObservationPublisher.Prepared facts(int at, List<MetricFact> facts) {
        return new ObservationPublisher.Prepared(new Observation(PIPELINE, PipelineState.RUNNING,
                Map.of(), Map.of(), Map.of(), null, START.plusSeconds(at), facts), false,
                Map.of(), Map.of(), Map.of());
    }

    private static long total(ObservationPublisher.Prepared frame, String name) {
        return frame.observation().facts().stream().filter(fact -> fact.name().equals(name))
                .flatMap(fact -> fact.points().stream()).mapToLong(MetricPoint::value).sum();
    }

    private static long histogramCount(ObservationPublisher.Prepared frame) {
        return frame.observation().facts().getFirst().points().stream()
                .mapToLong(point -> point.histogram().count()).sum();
    }

    private static ObservationPublisher.Prepared frame(int at, PipelineState state, Instant outboundSince,
            Integer outbound, long inbound) {
        var points = new java.util.ArrayList<MetricPoint>();
        points.add(MetricPoint.accumulated(attributes("in"), START, START.plusSeconds(at), inbound));
        if (outbound != null) {
            points.add(MetricPoint.accumulated(attributes("out"), outboundSince, START.plusSeconds(at), outbound));
        }
        MetricFact fact = new MetricFact("tapstate.pipeline.records", MetricType.COUNTER, "{record}", points);
        Observation observation = new Observation(PIPELINE, state, Map.of(), Map.of(), Map.of(), null,
                START.plusSeconds(at), List.of(fact));
        return new ObservationPublisher.Prepared(observation, false, Map.of(), Map.of(), Map.of());
    }

    private static Map<String, String> attributes(String direction) {
        return Map.of(MetricAttributes.PIPELINE_ID, PIPELINE, MetricAttributes.TABLE_ID, "orders",
                MetricAttributes.DIRECTION, direction, MetricAttributes.OP, "update");
    }

    private static List<MetricPoint> points(ObservationPublisher.Prepared frame, String direction) {
        return frame.observation().facts().stream().filter(fact -> fact.name().equals("tapstate.pipeline.records"))
                .flatMap(fact -> fact.points().stream())
                .filter(point -> direction.equals(point.attributes().get(MetricAttributes.DIRECTION))).toList();
    }

    private static long value(ObservationPublisher.Prepared frame, String direction) {
        return points(frame, direction).getFirst().value();
    }
}
