package io.tapstate.control.core;

import io.tapstate.control.core.HistoryAggregator.Emitted;
import io.tapstate.control.core.PipelineMetricsHistory.StartReason;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.RateHistoryStore.Entry;
import io.tapstate.spi.store.RateHistoryStore.Key;
import io.tapstate.spi.store.ObservationStore;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;

class HistoryAggregatorTest {

    @Test
    void executionChangeStartsANewSegmentWithoutCallingAContinuedCounterReset() {
        Instant from = Instant.parse("2026-09-21T10:00:00Z");
        HistoryAggregator aggregator = new HistoryAggregator(from, from.plusSeconds(180), from,
                Duration.ofMinutes(1), Duration.ofSeconds(30), List.of(), StartReason.WINDOW_START, 10);
        aggregator.add(scoped(1, from, 7, 41));
        aggregator.add(scoped(2, from.plusSeconds(30), 9, 42));
        aggregator.add(scoped(3, from.plusSeconds(60), 12, 42));

        HistoryAggregator.Projection result = aggregator.finish(null);

        assertThat(result.points()).extracting(Emitted::segment).containsExactly(0, 1, 1);
        assertThat(result.points()).extracting(Emitted::startReason)
                .containsExactly(StartReason.WINDOW_START, StartReason.CONTINUATION, StartReason.CONTINUATION);
        assertThat(result.points()).anySatisfy(point -> {
            assertThat(point.point().recordsOut()).isNotNull();
            assertThat(point.point().recordsOut().delta()).isEqualByComparingTo("3");
        });
        assertThat(result.gaps()).isEmpty();
    }

    @Test
    void anUnknownStartOnEitherSideHasNoRateAndIsNotEvidenceOfAReset() {
        Instant from = Instant.parse("2026-09-21T10:00:00Z");
        HistoryAggregator knownThenUnknown = history(from);
        knownThenUnknown.add(known(1, from, 100, START, 41));
        knownThenUnknown.add(unknown(2, from.plusSeconds(30), 41));
        HistoryAggregator.Projection lostStart = knownThenUnknown.finish(null);
        assertThat(lostStart.points()).extracting(Emitted::startReason)
                .doesNotContain(StartReason.COUNTER_RESET);
        assertThat(lostStart.points()).allSatisfy(point ->
                assertThat(point.point().recordsOut()).isNull());
        assertThat(lostStart.gaps()).isEmpty();

        HistoryAggregator unknownThenKnown = history(from);
        unknownThenKnown.add(unknown(1, from, 41));
        unknownThenKnown.add(known(2, from.plusSeconds(30), 140, START, 41));
        unknownThenKnown.add(known(3, from.plusSeconds(60), 150, START, 41));
        HistoryAggregator.Projection recovered = unknownThenKnown.finish(null);
        assertThat(recovered.points()).extracting(Emitted::startReason)
                .doesNotContain(StartReason.COUNTER_RESET);
        assertThat(recovered.points().stream().map(point -> point.point().recordsOut())
                .filter(Objects::nonNull).toList()).singleElement().satisfies(rate ->
                        assertThat(rate.delta()).isEqualByComparingTo("10"));
        assertThat(recovered.gaps()).isEmpty();
    }

    @Test
    void aPausedUnknownStartBetweenKnownSamplesDoesNotInventRatesOrResetInOneScope() {
        assertKnownRatesOnBothSidesOfUnknown(pauseAndResume(41), false);
    }

    @Test
    void anExecutionChangeAcrossAnUnknownStartContinuesKnownCountersWithoutAReset() {
        assertKnownRatesOnBothSidesOfUnknown(pauseAndResume(42), true);
    }

    @Test
    void aRealResetAcrossAnUnknownSampleRemainsVisibleWithoutInventingRates() {
        Instant from = Instant.parse("2026-09-21T10:00:00Z");
        HistoryAggregator aggregator = history(from);
        aggregator.add(known(1, from, 3, START, 41));
        aggregator.add(new Entry(new Key(from.plusSeconds(30), "002"),
                new RateSample("orders", from.plusSeconds(30), Map.of(), Map.of(), null),
                Optional.of(new ObservationStore.Scope("inc-a", 42))));
        aggregator.add(known(3, from.plusSeconds(60), 1, START.plusSeconds(1), 42));

        HistoryAggregator.Projection result = aggregator.finish(null);

        assertThat(result.points()).extracting(Emitted::startReason).contains(StartReason.COUNTER_RESET);
        assertThat(result.points()).allSatisfy(point -> {
            assertThat(point.point().recordsOut()).isNull();
            assertThat(point.point().bytesOut()).isNull();
        });
        assertThat(result.gaps()).isEmpty();
        assertThat(result.peakRawEntriesHeld()).isLessThanOrEqualTo(2);
    }

    @Test
    void aContinuousStartAcrossAnUnknownNewExecutionDoesNotInventResetOrRates() {
        Instant from = Instant.parse("2026-09-21T10:00:00Z");
        HistoryAggregator aggregator = history(from);
        aggregator.add(known(1, from, 3, START, 41));
        aggregator.add(new Entry(new Key(from.plusSeconds(30), "002"),
                new RateSample("orders", from.plusSeconds(30), Map.of(), Map.of(), null),
                Optional.of(new ObservationStore.Scope("inc-a", 42))));
        aggregator.add(known(3, from.plusSeconds(60), 5, START, 42));
        aggregator.add(known(4, from.plusSeconds(90), 6, START, 42));

        HistoryAggregator.Projection result = aggregator.finish(null);

        assertThat(result.points()).extracting(Emitted::startReason)
                .contains(StartReason.CONTINUATION).doesNotContain(StartReason.COUNTER_RESET);
        assertThat(result.points().stream().map(point -> point.point().recordsOut())
                .filter(Objects::nonNull).toList()).singleElement().satisfies(rate ->
                        assertThat(rate.delta()).isEqualByComparingTo("1"));
        assertThat(result.points().stream().map(point -> point.point().bytesOut())
                .filter(Objects::nonNull).toList()).singleElement().satisfies(rate ->
                        assertThat(rate.delta()).isEqualByComparingTo("10"));
        assertThat(result.gaps()).isEmpty();
        assertThat(result.peakRawEntriesHeld()).isLessThanOrEqualTo(2);
    }

    @Test
    void aChangedKnownStartOrADecreasedKnownCounterStillReportsARealReset() {
        Instant from = Instant.parse("2026-09-21T10:00:00Z");
        HistoryAggregator newStart = history(from);
        newStart.add(known(1, from, 100, START, 41));
        newStart.add(known(2, from.plusSeconds(30), 110, START.plusSeconds(1), 41));
        assertThat(newStart.finish(null).points()).extracting(Emitted::startReason)
                .contains(StartReason.COUNTER_RESET);

        HistoryAggregator decreased = history(from);
        decreased.add(known(1, from, 100, START, 41));
        decreased.add(known(2, from.plusSeconds(30), 90, START, 41));
        assertThat(decreased.finish(null).points()).extracting(Emitted::startReason)
                .contains(StartReason.COUNTER_RESET);
    }

    private static HistoryAggregator.Projection pauseAndResume(long resumedGeneration) {
        Instant from = Instant.parse("2026-09-21T10:00:00Z");
        HistoryAggregator aggregator = history(from);
        aggregator.add(known(1, from, 100, START, 41));
        aggregator.add(known(2, from.plusSeconds(30), 110, START, 41));
        aggregator.add(unknown(3, from.plusSeconds(60), 41));
        aggregator.add(known(4, from.plusSeconds(90), 140, START, resumedGeneration));
        aggregator.add(known(5, from.plusSeconds(120), 150, START, resumedGeneration));
        return aggregator.finish(null);
    }

    private static void assertKnownRatesOnBothSidesOfUnknown(
            HistoryAggregator.Projection result, boolean changedExecution) {
        assertThat(result.points()).extracting(Emitted::startReason)
                .doesNotContain(StartReason.COUNTER_RESET);
        if (changedExecution) {
            assertThat(result.points()).extracting(Emitted::startReason)
                    .contains(StartReason.CONTINUATION);
        }
        assertThat(result.points().stream().map(point -> point.point().recordsOut())
                .filter(Objects::nonNull).toList()).hasSize(2).allSatisfy(rate ->
                        assertThat(rate.delta()).isEqualByComparingTo("10"));
        assertThat(result.points().stream().map(point -> point.point().bytesOut())
                .filter(Objects::nonNull).toList()).hasSize(2).allSatisfy(rate ->
                        assertThat(rate.delta()).isEqualByComparingTo("100"));
        assertThat(result.gaps()).isEmpty();
    }

    private static HistoryAggregator history(Instant from) {
        return new HistoryAggregator(from, from.plusSeconds(180), from,
                Duration.ofMinutes(1), Duration.ofSeconds(30), List.of(), StartReason.WINDOW_START, 10);
    }

    private static Entry known(int key, Instant at, long records, Instant since, long generation) {
        RateSample sample = new RateSample("orders", at,
                Map.of("records.out", records, "bytes.out", records * 10), Map.of(), since);
        return new Entry(new Key(at, "%03d".formatted(key)), sample,
                Optional.of(new ObservationStore.Scope("inc-a", generation)));
    }

    private static Entry unknown(int key, Instant at, long generation) {
        RateSample sample = new RateSample("orders", at, Map.of("recordCount", 0L), Map.of(), null);
        return new Entry(new Key(at, "%03d".formatted(key)), sample,
                Optional.of(new ObservationStore.Scope("inc-a", generation)));
    }

    private static Entry scoped(int key, Instant at, long records, long generation) {
        RateSample sample = new RateSample("orders", at, Map.of("records.out", records), Map.of(), START);
        return new Entry(new Key(at, "%03d".formatted(key)), sample,
                Optional.of(new ObservationStore.Scope("inc-a", generation)));
    }

    private static final Instant START = Instant.parse("2026-09-21T00:00:00Z");
    private static final Instant FROM = Instant.parse("2026-09-21T10:07:00Z");
    private static final Instant TO = Instant.parse("2026-09-21T10:43:00Z");

    @Test
    void partialUtcBucketsUseOnlyTheRequestedIntervalAndIgnoreSuccessorLag() {
        HistoryAggregator aggregator = new HistoryAggregator(FROM, TO, FROM, Duration.ofMinutes(30),
                Duration.ofMinutes(30), List.of("orders"), StartReason.WINDOW_START, 10);
        aggregator.begin(entry(0, "2026-09-21T10:00:00Z", 0, 0));
        aggregator.add(entry(1, "2026-09-21T10:10:00Z", 100, 2));
        aggregator.add(entry(2, "2026-09-21T10:40:00Z", 400, 12));

        HistoryAggregator.Projection projection =
                aggregator.finish(entry(3, "2026-09-21T10:50:00Z", 600, 99));

        assertThat(projection.points()).hasSize(2);
        Emitted first = projection.points().get(0);
        Emitted second = projection.points().get(1);
        assertThat(first.point().intervalStart()).isEqualTo(FROM);
        assertThat(first.point().intervalEnd()).isEqualTo(Instant.parse("2026-09-21T10:30:00Z"));
        assertThat(first.point().recordsOut().delta()).isEqualByComparingTo("230");
        assertThat(first.point().recordsOut().delta().toString()).isEqualTo("230");
        assertThat(first.point().recordsOut().averageRate()).isEqualByComparingTo("0.166666667");
        assertThat(first.point().lag()).extracting(PipelineMetricsHistory.Lag::last).containsExactly(2L);

        assertThat(second.point().intervalStart()).isEqualTo(Instant.parse("2026-09-21T10:30:00Z"));
        assertThat(second.point().intervalEnd()).isEqualTo(TO);
        assertThat(second.point().recordsOut().delta()).isEqualByComparingTo("160");
        assertThat(second.point().recordsOut().delta().toString()).isEqualTo("160");
        assertThat(second.point().recordsOut().averageRate()).isEqualByComparingTo("0.205128205");
        assertThat(second.point().recordsOut().maxRate()).isEqualByComparingTo("0.333333333");
        assertThat(second.point().lag()).extracting(PipelineMetricsHistory.Lag::last).containsExactly(12L);
        assertThat(second.point().lag()).extracting(PipelineMetricsHistory.Lag::max).containsExactly(12L);
    }

    @Test
    void resetAndGapBoundariesSurviveArbitraryInputBatches() {
        Instant from = Instant.parse("2026-09-21T10:00:00Z");
        Instant to = Instant.parse("2026-09-21T10:11:00Z");
        List<Entry> samples = List.of(
                entry(1, "2026-09-21T10:00:00Z", 60, 1),
                entry(2, "2026-09-21T10:01:00Z", 120, 3),
                entry(3, "2026-09-21T10:02:00Z", 5, 2, START.plusSeconds(1)),
                entry(4, "2026-09-21T10:10:00Z", 15, 4, START.plusSeconds(1)));

        HistoryAggregator.Projection oneBatch = project(from, to, samples, List.of(4));
        HistoryAggregator.Projection threeBatches = project(from, to, samples, List.of(1, 2, 1));

        assertThat(threeBatches).isEqualTo(oneBatch);
        assertThat(oneBatch.points()).extracting(Emitted::startReason)
                .containsExactly(StartReason.WINDOW_START, StartReason.COUNTER_RESET, StartReason.GAP);
        assertThat(oneBatch.gaps()).singleElement().satisfies(gap -> {
            assertThat(gap.gap().intervalStart()).isEqualTo(Instant.parse("2026-09-21T10:02:00Z"));
            assertThat(gap.gap().intervalEnd()).isEqualTo(Instant.parse("2026-09-21T10:10:00Z"));
        });
        assertThat(oneBatch.points().get(0).point().recordsOut().delta()).isEqualByComparingTo("60");
        assertThat(oneBatch.points().get(1).point().recordsOut()).isNull();
        assertThat(oneBatch.points().get(2).point().recordsOut()).isNull();
    }

    private static HistoryAggregator.Projection project(Instant from, Instant to, List<Entry> samples,
            List<Integer> batchSizes) {
        HistoryAggregator aggregator = new HistoryAggregator(from, to, from, Duration.ofMinutes(5),
                Duration.ofMinutes(2), List.of("orders"), StartReason.WINDOW_START, 100);
        aggregator.begin(entry(0, "2026-09-21T09:59:00Z", 0, 0));
        int offset = 0;
        for (int size : batchSizes) {
            for (Entry sample : samples.subList(offset, offset + size)) {
                aggregator.add(sample);
            }
            offset += size;
        }
        return aggregator.finish(null);
    }

    private static Entry entry(int key, String at, long records, long lag) {
        return entry(key, at, records, lag, START);
    }

    private static Entry entry(int key, String at, long records, long lag, Instant countingSince) {
        Instant observedAt = Instant.parse(at);
        RateSample sample = new RateSample("orders", observedAt,
                Map.of("records.out", records), Map.of("orders", lag), countingSince);
        return new Entry(new Key(observedAt, "%03d".formatted(key)), sample);
    }
}
