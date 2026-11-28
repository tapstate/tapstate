package io.tapstate.control.core;

import io.tapstate.core.lifecycle.CdcDeliveryTotals;
import io.tapstate.core.lifecycle.RateSample;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.tapstate.control.core.HistoryTestSupport.COUNTING_SINCE;
import static io.tapstate.control.core.HistoryTestSupport.NOW;
import static io.tapstate.control.core.HistoryTestSupport.points;
import static io.tapstate.control.core.HistoryTestSupport.query;
import static io.tapstate.control.core.HistoryTestSupport.sample;
import static io.tapstate.control.core.HistoryTestSupport.service;
import static org.assertj.core.api.Assertions.assertThat;

/** Resolution choice and aggregation keep stable UTC buckets without crossing reset or gap boundaries. */
class HistoryResolutionAggregationTest {

    @Test
    void autoAndEveryExplicitResolutionHaveOneStableEffectiveValue() {
        HistoryTestSupport.MutableHistory empty = new HistoryTestSupport.MutableHistory();
        Map<Duration, EffectiveHistoryResolution> auto = new LinkedHashMap<>();
        auto.put(Duration.ofHours(1), EffectiveHistoryResolution.PT1M);
        auto.put(Duration.ofHours(6), EffectiveHistoryResolution.PT5M);
        auto.put(Duration.ofDays(1), EffectiveHistoryResolution.PT30M);
        auto.put(Duration.ofDays(3), EffectiveHistoryResolution.PT1H);
        auto.put(Duration.ofDays(7), EffectiveHistoryResolution.PT3H);
        auto.put(Duration.ofDays(15), EffectiveHistoryResolution.PT6H);

        auto.forEach((span, expected) -> assertThat(service(empty, 1024, 25_000).query(
                new PipelineHistoryQuery("orders", NOW.minus(span), NOW, HistoryResolution.AUTO,
                        PipelineHistoryQuery.DEFAULT_LIMIT, List.of(), null)).effectiveResolution())
                .as("auto resolution for %s", span)
                .isEqualTo(expected));

        Map<HistoryResolution, EffectiveHistoryResolution> explicit = Map.of(
                HistoryResolution.RAW, EffectiveHistoryResolution.PT1M,
                HistoryResolution.PT5M, EffectiveHistoryResolution.PT5M,
                HistoryResolution.PT30M, EffectiveHistoryResolution.PT30M,
                HistoryResolution.PT1H, EffectiveHistoryResolution.PT1H,
                HistoryResolution.PT3H, EffectiveHistoryResolution.PT3H,
                HistoryResolution.PT6H, EffectiveHistoryResolution.PT6H);
        explicit.forEach((requested, expected) -> assertThat(service(empty, 1024, 25_000).query(
                new PipelineHistoryQuery("orders", NOW.minus(Duration.ofHours(1)), NOW, requested,
                        PipelineHistoryQuery.DEFAULT_LIMIT, List.of(), null)).effectiveResolution())
                .as("explicit resolution %s", requested)
                .isEqualTo(expected));
    }

    @Test
    void oneUtcBucketRetainsDeltaRateAndLagPeaksThenSplitsAtResetAndGap() {
        HistoryTestSupport.MutableHistory store = new HistoryTestSupport.MutableHistory();
        store.add(sample("2026-09-21T09:59:00Z", 0, 0, 0, Map.of(), COUNTING_SINCE));
        store.add(sample("2026-09-21T10:00:00Z", 60, 600, 0,
                Map.of("orders", 9L), COUNTING_SINCE));
        store.add(sample("2026-09-21T10:01:00Z", 120, 1_200, 0,
                Map.of("orders", 2L), COUNTING_SINCE));
        store.add(sample("2026-09-21T10:02:00Z", 240, 2_400, 0,
                Map.of("orders", 4L), COUNTING_SINCE));
        store.add(sample("2026-09-21T10:03:00Z", 5, 50, 0,
                Map.of("orders", 1L), COUNTING_SINCE.plusSeconds(1)));
        store.add(sample("2026-09-21T10:04:00Z", 65, 650, 0,
                Map.of("orders", 3L), COUNTING_SINCE.plusSeconds(1)));
        store.add(sample("2026-09-21T10:10:00Z", 425, 4_250, 0,
                Map.of("orders", 7L), COUNTING_SINCE.plusSeconds(1)));

        PipelineMetricsHistory answer = service(store, 2, 25_000).query(
                query("2026-09-21T10:00:00Z", "2026-09-21T10:11:00Z",
                        HistoryResolution.PT30M, 20, List.of("orders"), null));

        assertThat(answer.effectiveResolution()).isEqualTo(EffectiveHistoryResolution.PT30M);
        assertThat(answer.segments()).extracting(PipelineMetricsHistory.Segment::startReason)
                .containsExactly(
                        PipelineMetricsHistory.StartReason.WINDOW_START,
                        PipelineMetricsHistory.StartReason.COUNTER_RESET,
                        PipelineMetricsHistory.StartReason.GAP);
        assertThat(answer.gaps()).singleElement().satisfies(gap -> {
            assertThat(gap.intervalStart().toString()).isEqualTo("2026-09-21T10:04:00Z");
            assertThat(gap.intervalEnd().toString()).isEqualTo("2026-09-21T10:10:00Z");
        });

        List<PipelineMetricsHistory.Point> points = points(answer);
        assertThat(points.get(0).recordsOut().delta()).isEqualByComparingTo("180");
        assertThat(points.get(0).recordsOut().averageRate()).isEqualByComparingTo("1.5");
        assertThat(points.get(0).recordsOut().maxRate()).isEqualByComparingTo("2");
        assertThat(points.get(0).lag()).singleElement().satisfies(lag -> {
            assertThat(lag.last()).isEqualTo(4L);
            assertThat(lag.max()).isEqualTo(9L);
        });
        assertThat(points.get(1).recordsOut().delta()).isEqualByComparingTo("60");
        assertThat(points.get(2).recordsOut()).isNull();
    }
    @Test
    void cdcBucketsWeightEachRecordOnceAndOmitIdleIntervals() {
        var store = new HistoryTestSupport.MutableHistory();
        store.add(cdcSample("2026-09-21T10:00:00Z", 0, 0));
        store.add(cdcSample("2026-09-21T10:01:00Z", 2, 600));
        store.add(cdcSample("2026-09-21T10:02:00Z", 6, 3_600));
        store.add(cdcSample("2026-09-21T10:03:00Z", 6, 3_600));
        store.add(cdcSample("2026-09-21T10:08:00Z", 10, 30_000));
        store.add(cdcSample("2026-09-21T10:09:00Z", 10, 30_000));
        store.add(cdcSample("2026-09-21T10:10:00Z", 11, 30_000));

        var answer = service(store, 2, 25_000).query(
                query("2026-09-21T10:00:00Z", "2026-09-21T10:11:00Z",
                        HistoryResolution.PT5M, 20, List.of(), null));
        var readings = points(answer).stream().map(PipelineMetricsHistory.Point::cdcDelivery).toList();

        assertThat(readings.getFirst().sampleCount()).isEqualTo(6);
        assertThat(readings.getFirst().averageMillis()).isEqualByComparingTo("600");
        assertThat(readings.get(1)).isNull();
        assertThat(readings.getLast().sampleCount()).isEqualTo(1);
        assertThat(readings.getLast().averageMillis()).isZero();
    }

    @Test
    void orderedSamplesWithTheSameTimestampStillCountEachCdcDelivery() {
        var store = new HistoryTestSupport.MutableHistory();
        store.add(cdcSample("2026-09-21T10:00:00Z", 0, 0));
        store.add(cdcSample("2026-09-21T10:01:00Z", 1, 100));
        store.add(cdcSample("2026-09-21T10:01:00Z", 2, 300));
        store.add(cdcSample("2026-09-21T10:02:00Z", 3, 600));

        var history = service(store, 2, 25_000);
        var raw = points(history.query(query("2026-09-21T10:00:00Z", "2026-09-21T10:05:00Z",
                HistoryResolution.RAW, 20, List.of(), null)));
        assertThat(raw).hasSize(4);
        var sameTime = raw.get(2);
        assertThat(sameTime.intervalEnd()).isEqualTo(raw.get(1).intervalEnd());
        assertThat(sameTime.recordsOut()).isNull();
        assertThat(sameTime.cdcDelivery().sampleCount()).isEqualTo(1);
        assertThat(sameTime.cdcDelivery().averageMillis()).isEqualByComparingTo("200");

        var buckets = points(history.query(query("2026-09-21T10:00:00Z", "2026-09-21T10:05:00Z",
                HistoryResolution.PT5M, 20, List.of(), null)));
        assertThat(buckets).singleElement().satisfies(point -> {
            assertThat(point.cdcDelivery().sampleCount()).isEqualTo(3);
            assertThat(point.cdcDelivery().averageMillis()).isEqualByComparingTo("200");
        });
    }

    @ParameterizedTest
    @EnumSource(value = HistoryResolution.class, names = {"RAW", "PT5M"})
    void firstConfirmedSnapshotBatchAppearsInOutboundHistory(HistoryResolution resolution) {
        String from = "2026-09-21T10:00:00Z";
        Instant firstObservedAt = Instant.parse(from);
        Instant countingSince = firstObservedAt.plusMillis(4_016);
        var store = new HistoryTestSupport.MutableHistory();
        store.add(new RateSample("orders", firstObservedAt, Map.of("recordCount", 0L), Map.of(), null,
                new RateSample.StartupEvidence(1, 2, null)));
        Map<String, Long> confirmed = Map.of("records.out", 10_125L, "bytes.out", 1_237_470L,
                CdcDeliveryTotals.COUNT_METRIC, 0L, CdcDeliveryTotals.TOTAL_MILLIS_METRIC, 0L);
        store.add(new RateSample("orders", firstObservedAt.plusMillis(60_863), confirmed,
                Map.of(), countingSince, new RateSample.StartupEvidence(1, 2, countingSince)));
        store.add(new RateSample("orders", firstObservedAt.plusMillis(120_863), confirmed,
                Map.of(), countingSince, new RateSample.StartupEvidence(1, 2, countingSince)));

        var answer = service(store, 2, 25_000).query(query(from, "2026-09-21T10:03:00Z",
                resolution, 20, List.of(), null));
        assertThat(answer.gaps()).isEmpty();
        assertThat(cdcCount(answer)).isZero();

        var readings = points(answer);
        var records = readings.stream().map(PipelineMetricsHistory.Point::recordsOut)
                .filter(Objects::nonNull).map(PipelineMetricsHistory.Rate::delta)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        var bytes = readings.stream().map(PipelineMetricsHistory.Point::bytesOut)
                .filter(Objects::nonNull).map(PipelineMetricsHistory.Rate::delta)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        assertThat(List.of(records, bytes))
                .as("%s outbound records and bytes with segment starts %s", resolution,
                        answer.segments().stream().map(PipelineMetricsHistory.Segment::startReason).toList())
                .usingElementComparator(BigDecimal::compareTo)
                .containsExactly(BigDecimal.valueOf(10_125), BigDecimal.valueOf(1_237_470));
    }

    @ParameterizedTest
    @EnumSource(value = HistoryResolution.class, names = {"RAW", "PT5M"})
    void aRetainedStartupBaselineRequiresBothObservedZeros(HistoryResolution resolution) {
        Instant first = Instant.parse("2026-09-21T10:00:00Z");
        Instant origin = first.minusSeconds(40);
        var evidence = new RateSample.StartupEvidence(1, 2, origin);
        var right = new RateSample("orders", first.plusSeconds(10), Map.of("records.out", 4L, "bytes.out", 100L),
                Map.of(), first.minusSeconds(30), evidence);
        for (Map<String, Long> counters : List.of(
                Map.of("recordCount", 0L), Map.of("records.out", 0L, "bytes.out", 0L))) {
            var store = new HistoryTestSupport.MutableHistory();
            store.add(new RateSample("orders", first, counters, Map.of(), null, evidence));
            store.add(right);
            var answer = service(store, 2, 25_000).query(new PipelineHistoryQuery("orders", first,
                    first.plusSeconds(20), resolution, 20, List.of(), null));
            var measured = points(answer).stream()
                    .filter(point -> point.recordsOut() != null || point.bytesOut() != null).toList();
            if (counters.containsKey("records.out")) {
                assertThat(measured).singleElement().satisfies(point -> {
                    assertThat(point.intervalStart()).isEqualTo(first);
                    assertThat(point.recordsOut().delta()).isEqualByComparingTo("4");
                    assertThat(point.bytesOut().delta()).isEqualByComparingTo("100");
                });
            } else {
                assertThat(measured).isEmpty();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(value = HistoryResolution.class, names = {"RAW", "PT5M"})
    void theSameStartupSamplesWithoutEvidenceOnlyMeasureTheLaterIdleInterval(HistoryResolution resolution) {
        String from = "2026-09-21T10:00:00Z";
        Instant firstObservedAt = Instant.parse(from);
        Instant countingSince = firstObservedAt.plusMillis(4_016);
        Instant firstConfirmedAt = firstObservedAt.plusMillis(60_863);
        var store = new HistoryTestSupport.MutableHistory();
        store.add(new RateSample("orders", firstObservedAt, Map.of("recordCount", 0L), Map.of(), null));
        Map<String, Long> confirmed = Map.of("records.out", 10_125L, "bytes.out", 1_237_470L,
                CdcDeliveryTotals.COUNT_METRIC, 0L, CdcDeliveryTotals.TOTAL_MILLIS_METRIC, 0L);
        store.add(new RateSample("orders", firstConfirmedAt, confirmed, Map.of(), countingSince));
        store.add(new RateSample("orders", firstObservedAt.plusMillis(120_863), confirmed, Map.of(), countingSince));

        var answer = service(store, 2, 25_000).query(query(from, "2026-09-21T10:03:00Z",
                resolution, 20, List.of(), null));
        var measured = points(answer).stream()
                .filter(point -> point.recordsOut() != null || point.bytesOut() != null).toList();
        assertThat(measured).singleElement().satisfies(point -> {
            assertThat(point.intervalStart()).isEqualTo(firstConfirmedAt);
            assertThat(point.recordsOut().delta()).isZero();
            assertThat(point.bytesOut().delta()).isZero();
        });
    }

    @ParameterizedTest
    @EnumSource(value = HistoryResolution.class, names = {"RAW", "PT5M"})
    void startupEvidenceDoesNotBridgeAnExecutionChangeAGapOrAnOutsideBaseline(HistoryResolution resolution) {
        Instant first = Instant.parse("2026-09-21T10:00:00Z");
        Instant origin = first.plusSeconds(4);
        var left = new RateSample("orders", first, Map.of("recordCount", 0L), Map.of(), null,
                new RateSample.StartupEvidence(1, 2, null));
        Map<String, Long> counters = Map.of("records.out", 4L, "bytes.out", 120L);
        var same = new RateSample("orders", first.plusSeconds(60), counters, Map.of(), origin,
                new RateSample.StartupEvidence(1, 2, origin));
        var replaced = new RateSample("orders", same.observedAt(), counters, Map.of(), origin,
                new RateSample.StartupEvidence(1, 3, origin));
        var late = new RateSample("orders", first.plusSeconds(120), counters, Map.of(), origin, same.startupEvidence());

        for (var right : List.of(same, replaced, late)) {
            var store = new HistoryTestSupport.MutableHistory();
            store.add(left);
            store.add(right);
            Instant from = right == same ? origin.plusMillis(1) : first;
            var answer = service(store, 2, 25_000).query(new PipelineHistoryQuery("orders", from,
                    first.plusSeconds(180), resolution, 20, List.of(), null));
            assertThat(points(answer)).isNotEmpty().allSatisfy(point -> {
                assertThat(point.recordsOut()).isNull();
                assertThat(point.bytesOut()).isNull();
            });
            assertThat(answer.gaps()).hasSize(right == late ? 1 : 0);
        }
    }

    @ParameterizedTest
    @EnumSource(value = HistoryResolution.class, names = {"RAW", "PT5M"})
    void firstAcknowledgementsUseTheObservedZeroBaselineEvenWhenTheJobStartedEarlier(HistoryResolution resolution) {
        var store = new HistoryTestSupport.MutableHistory();
        Instant runStart = Instant.parse("2026-09-21T09:59:00Z");
        Instant restart = Instant.parse("2026-09-21T10:02:30Z");
        store.add(awaitingDelivery("2026-09-21T10:00:00Z"));
        store.add(cdcSample("2026-09-21T10:01:00Z", 2, 600, runStart));
        store.add(cdcSample("2026-09-21T10:02:00Z", 3, 900, runStart));
        store.add(awaitingDelivery("2026-09-21T10:03:00Z"));
        store.add(cdcSample("2026-09-21T10:04:00Z", 4, 800, restart));

        var answer = service(store, 2, 25_000).query(query("2026-09-21T10:00:00Z",
                "2026-09-21T10:05:00Z", resolution, 20, List.of(), null));
        var readings = points(answer).stream().map(PipelineMetricsHistory.Point::cdcDelivery)
                .filter(Objects::nonNull).toList();
        assertThat(readings.stream().mapToLong(PipelineMetricsHistory.CdcDelivery::sampleCount).sum()).isEqualTo(7);
        assertThat(readings.getFirst().averageMillis()).isEqualByComparingTo("300");
        assertThat(readings.getLast().sampleCount()).isEqualTo(4);
        assertThat(readings.getLast().averageMillis()).isEqualByComparingTo("200");

        var clipped = service(store, 2, 25_000).query(query("2026-09-21T10:00:30Z",
                "2026-09-21T10:05:00Z", resolution, 20, List.of(), null));
        assertThat(cdcCount(clipped)).isEqualTo(5);
    }

    @ParameterizedTest
    @EnumSource(value = HistoryResolution.class, names = {"RAW", "PT5M"})
    void continuationWithAPurgedCursorSampleDoesNotCountItsDeliveriesAgain(HistoryResolution resolution) {
        var store = new HistoryTestSupport.MutableHistory();
        String from = "2026-09-21T10:00:00Z";
        String to = "2026-09-21T10:06:00Z";
        Instant start = Instant.parse(from);
        var older = store.add(cdcSample("2026-09-21T10:04:00Z", 1, 100, start));
        var cursorSample = store.add(cdcSample("2026-09-21T10:04:30Z", 2, 200, start));
        store.add(cdcSample("2026-09-21T10:05:00Z", 3, 300, start));
        store.add(cdcSample("2026-09-21T10:05:30Z", 4, 400, start));
        var history = service(store, 2, 25_000);
        int limit = resolution == HistoryResolution.RAW ? 2 : 1;
        var first = history.query(query(from, to, resolution, limit, List.of(), null));
        assertThat(first.nextCursor()).isNotBlank();
        assertThat(cdcCount(first)).isEqualTo(2);
        var continuation = query(from, to, resolution, limit, List.of(), first.nextCursor());
        assertThat(cdcCount(history.query(continuation))).isEqualTo(2);

        store.removeAt(cursorSample.sample().observedAt());
        assertThat(cdcCount(history.query(continuation))).as("only an older fallback remains").isEqualTo(1);

        store.removeAt(older.sample().observedAt());
        assertThat(cdcCount(history.query(continuation))).as("no predecessor remains").isEqualTo(1);
    }

    @ParameterizedTest
    @EnumSource(value = HistoryResolution.class, names = {"RAW", "PT5M"})
    void unavailableDeliveryMetricsDoNotResetExistingCdcTotalsToZero(HistoryResolution resolution) {
        var store = new HistoryTestSupport.MutableHistory();
        Instant start = Instant.parse("2026-09-21T09:50:00Z");
        store.add(cdcSample("2026-09-21T09:59:00Z", 6, 600, start));
        store.add(new RateSample("orders", Instant.parse("2026-09-21T10:00:00Z"),
                Map.of("records.in", 10L), Map.of(), null));
        store.add(cdcSample("2026-09-21T10:01:00Z", 7, 700, start));
        store.add(cdcSample("2026-09-21T10:02:00Z", 8, 800, start));

        var answer = service(store, 2, 25_000).query(query("2026-09-21T10:00:00Z",
                "2026-09-21T10:05:00Z", resolution, 20, List.of(), null));
        assertThat(cdcCount(answer)).isEqualTo(1);
    }

    private static long cdcCount(PipelineMetricsHistory history) {
        return points(history).stream().map(PipelineMetricsHistory.Point::cdcDelivery)
                .filter(Objects::nonNull).mapToLong(PipelineMetricsHistory.CdcDelivery::sampleCount).sum();
    }

    private static RateSample awaitingDelivery(String at) {
        return new RateSample("orders", Instant.parse(at), Map.of("records.in", 1L,
                CdcDeliveryTotals.COUNT_METRIC, 0L, CdcDeliveryTotals.TOTAL_MILLIS_METRIC, 0L), Map.of(), null);
    }

    private static RateSample cdcSample(String at, long count, long totalMillis) {
        return cdcSample(at, count, totalMillis, COUNTING_SINCE);
    }

    private static RateSample cdcSample(String at, long count, long totalMillis, Instant start) {
        return new RateSample("orders", Instant.parse(at), Map.of("records.out", count,
                CdcDeliveryTotals.COUNT_METRIC, count, CdcDeliveryTotals.TOTAL_MILLIS_METRIC, totalMillis),
                Map.of(), start);
    }
}
