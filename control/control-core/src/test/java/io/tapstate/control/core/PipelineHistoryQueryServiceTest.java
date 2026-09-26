package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.RateHistoryStore;
import io.tapstate.spi.store.RateHistoryStore.Entry;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.HistoryRollupStore;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

class PipelineHistoryQueryServiceTest {

    @Test
    void completeDayRollupAnswersWithoutReadingRawHistory() {
        RecordingHistory raw = new RecordingHistory();
        RecordingRollups cache = new RecordingRollups();
        Instant from = Instant.parse("2026-09-20T00:00:00Z");
        for (int bucket = 0; bucket < 48; bucket++) {
            cache.add(rollup(from.plusSeconds(bucket * 1_800L),
                    HistoryRollupStore.Resolution.PT30M, 1, NOW.plusSeconds(120)));
        }
        PipelineHistoryQueryService service = cachedService(raw, cache);

        PipelineHistoryQueryService.QueryRun run = service.execute(new PipelineHistoryQuery(
                "orders", from, from.plus(Duration.ofDays(1)), HistoryResolution.PT30M,
                100, List.of(), null));

        assertThat(run.history().status()).isEqualTo(PipelineMetricsHistory.Status.OK);
        assertThat(points(run.history())).hasSize(48);
        assertThat(run.history().nextCursor()).isNull();
        assertThat(run.cost().rawDocumentsScanned()).isZero();
        assertThat(raw.rawReads).isZero();
        assertThat(cache.rangeReads).isLessThanOrEqualTo(2);
    }

    @Test
    void completeFifteenDayRollupAnswersWithoutReadingRawHistory() {
        RecordingHistory raw = new RecordingHistory();
        RecordingRollups cache = new RecordingRollups();
        Instant from = NOW.minus(Duration.ofDays(15));
        for (int bucket = 0; bucket < 60; bucket++) {
            cache.add(rollup(from.plusSeconds(bucket * 21_600L),
                    HistoryRollupStore.Resolution.PT6H, 1, NOW.plusSeconds(120)));
        }

        PipelineHistoryQueryService.QueryRun run = cachedService(raw, cache).execute(
                new PipelineHistoryQuery("orders", from, NOW,
                        HistoryResolution.PT6H, 100, List.of(), null));

        assertThat(points(run.history())).hasSize(60);
        assertThat(run.cost().rawDocumentsScanned()).isZero();
        assertThat(raw.rawReads).isZero();
        assertThat(cache.rangeReads).isLessThanOrEqualTo(2);
    }

    @Test
    void missingMiddleRollupReadsRawOnlyForItsOwnBucket() {
        RecordingHistory raw = oneHourOfSamples();
        RecordingRollups cache = new RecordingRollups();
        Instant from = Instant.parse("2026-09-21T10:00:00Z");
        cache.add(rollup(from, HistoryRollupStore.Resolution.PT30M, 30, NOW.plusSeconds(120)));
        cache.add(rollup(from.plusSeconds(3_600), HistoryRollupStore.Resolution.PT30M,
                1, NOW.plusSeconds(120)));

        PipelineMetricsHistory history = cachedService(raw, cache).query(new PipelineHistoryQuery(
                "orders", from, from.plusSeconds(5_400), HistoryResolution.PT30M,
                100, List.of(), null));

        assertThat(history.status()).isEqualTo(PipelineMetricsHistory.Status.OK);
        assertThat(points(history)).hasSize(3);
        assertThat(raw.pageRanges).containsExactly(new TimeRange(from.plusSeconds(1_800),
                from.plusSeconds(3_600)));
    }

    @Test
    void partialFirstBucketUsesRawOnlyAtThatBoundary() {
        RecordingHistory raw = oneHourOfSamples();
        RecordingRollups cache = new RecordingRollups();
        Instant from = Instant.parse("2026-09-21T10:00:00Z");
        cache.add(rollup(from.plusSeconds(1_800), HistoryRollupStore.Resolution.PT30M,
                30, NOW.plusSeconds(120)));

        PipelineMetricsHistory history = cachedService(raw, cache).query(new PipelineHistoryQuery(
                "orders", from.plusSeconds(300), from.plusSeconds(3_600),
                HistoryResolution.PT30M, 100, List.of(), null));

        assertThat(points(history)).hasSize(2);
        assertThat(raw.pageRanges).containsExactly(new TimeRange(from.plusSeconds(300),
                from.plusSeconds(1_800)));
    }

    @Test
    void expiredRollupDescendsToLateRawSamples() {
        RecordingHistory raw = oneHourOfSamples();
        RecordingRollups cache = new RecordingRollups();
        Instant from = Instant.parse("2026-09-21T10:00:00Z");
        cache.add(rollup(from, HistoryRollupStore.Resolution.PT30M, 1, NOW.minusSeconds(1)));
        cache.add(rollup(from.plusSeconds(1_800), HistoryRollupStore.Resolution.PT30M,
                30, NOW.plusSeconds(120)));

        PipelineMetricsHistory history = cachedService(raw, cache).query(new PipelineHistoryQuery(
                "orders", from, from.plusSeconds(3_600), HistoryResolution.PT30M,
                100, List.of(), null));

        assertThat(points(history)).hasSize(2);
        assertThat(points(history).getFirst().recordsOut().delta()).isEqualByComparingTo("1800");
        assertThat(raw.pageRanges).containsExactly(new TimeRange(from, from.plusSeconds(1_800)));
    }

    @Test
    void missingAndExpiredBucketsOfferBoundedRefreshWithoutChangingTheRawAnswer() {
        RecordingHistory raw = oneHourOfSamples();
        RecordingRollups cache = new RecordingRollups();
        Instant from = Instant.parse("2026-09-21T10:00:00Z");
        cache.add(rollup(from, HistoryRollupStore.Resolution.PT30M, 1, NOW.minusSeconds(1)));
        List<HistoryRollupStore.Key> hinted = new ArrayList<>();
        PipelineHistoryQueryService service = new PipelineHistoryQueryService(
                artifactsWith("orders"), raw, cache, key -> {
                    hinted.add(key);
                    if (hinted.size() == 2) {
                        throw new IllegalStateException("refresh worker unavailable");
                    }
                }, Duration.ofMinutes(1), fixedClock(), cursorCodec(), 128, 25_000);
        PipelineHistoryQuery request = new PipelineHistoryQuery("orders", from,
                from.plusSeconds(3_600), HistoryResolution.PT30M, 100, List.of("orders"), null);

        assertThat(service.query(request)).isEqualTo(service(raw, 128, 25_000).query(request));
        assertThat(hinted).containsExactly(
                new HistoryRollupStore.Key("orders", HistoryRollupStore.Scope.legacy(),
                        HistoryRollupStore.Resolution.PT30M, from),
                new HistoryRollupStore.Key("orders", HistoryRollupStore.Scope.legacy(),
                        HistoryRollupStore.Resolution.PT30M, from.plusSeconds(1_800)));
    }

    @Test
    void cachedPagesResumeWithoutRawReadsAndRetainQueryBinding() {
        RecordingHistory raw = new RecordingHistory();
        RecordingRollups cache = new RecordingRollups();
        Instant from = Instant.parse("2026-09-21T10:00:00Z");
        cache.add(anchoredRollup(from, "left"));
        cache.add(anchoredRollup(from.plusSeconds(1_800), "right"));
        PipelineHistoryQueryService service = cachedService(raw, cache);
        PipelineHistoryQuery request = new PipelineHistoryQuery("orders", from,
                from.plusSeconds(3_600), HistoryResolution.PT30M, 1, List.of(), null);

        PipelineMetricsHistory first = service.query(request);
        PipelineMetricsHistory second = service.query(new PipelineHistoryQuery("orders", from,
                from.plusSeconds(3_600), HistoryResolution.PT30M, 1, List.of(), first.nextCursor()));

        assertThat(points(first)).singleElement().satisfies(point ->
                assertThat(point.intervalStart()).isEqualTo(from));
        assertThat(points(second)).singleElement().satisfies(point ->
                assertThat(point.intervalStart()).isEqualTo(from.plusSeconds(1_800)));
        assertThat(second.segments()).extracting(PipelineMetricsHistory.Segment::startReason)
                .containsExactly(PipelineMetricsHistory.StartReason.CONTINUATION);
        assertThat(second.nextCursor()).isNull();
        assertThat(raw.rawReads).isZero();

        TapstateException mismatch = catchThrowableOfType(() -> service.query(new PipelineHistoryQuery(
                "orders", from, from.plusSeconds(3_600), HistoryResolution.PT30M,
                2, List.of(), first.nextCursor())), TapstateException.class);
        assertThat(mismatch.code()).isEqualTo(MonitorError.INVALID_CURSOR);
    }

    @Test
    void aPageEndingInOneRawFallbackKeepsTheNextCachedBucket() {
        RecordingHistory raw = samplesFor(Duration.ofHours(2));
        RecordingRollups cache = new RecordingRollups();
        Instant from = Instant.parse("2026-09-21T10:00:00Z");
        cache.add(projectRollup(raw, from));
        cache.add(projectRollup(raw, from.plusSeconds(3_600)));
        PipelineHistoryQueryService service = cachedService(raw, cache);
        PipelineHistoryQuery firstRequest = new PipelineHistoryQuery("orders", from,
                from.plusSeconds(5_400), HistoryResolution.PT30M, 2, List.of("orders"), null);

        PipelineMetricsHistory first = service.query(firstRequest);
        assertThat(points(first)).hasSize(2);
        assertThat(first.nextCursor()).isNotNull();
        assertThat(raw.pageRanges).containsExactly(new TimeRange(from.plusSeconds(1_800),
                from.plusSeconds(3_600)));

        PipelineMetricsHistory second = service.query(new PipelineHistoryQuery("orders", from,
                from.plusSeconds(5_400), HistoryResolution.PT30M, 2, List.of("orders"),
                first.nextCursor()));
        assertThat(points(second)).singleElement().satisfies(point ->
                assertThat(point.intervalStart()).isEqualTo(from.plusSeconds(3_600)));
        assertThat(raw.pageRanges).containsOnly(new TimeRange(from.plusSeconds(1_800),
                from.plusSeconds(3_600)));
    }

    @Test
    void cachedProjectionMatchesRawAcrossResetAndGapBoundaries() {
        RecordingHistory raw = new RecordingHistory();
        Instant counting = Instant.parse("2026-09-21T00:00:00Z");
        raw.add(sample("2026-09-21T09:59:00Z", 0, 0, 0, Map.of("orders", 9L), counting));
        raw.add(sample("2026-09-21T10:00:00Z", 60, 600, 0, Map.of("orders", 8L), counting));
        raw.add(sample("2026-09-21T10:01:00Z", 120, 1_200, 0, Map.of("orders", 7L), counting));
        raw.add(sample("2026-09-21T10:02:00Z", 0, 0, 0, Map.of("orders", 6L),
                counting.plusSeconds(1)));
        raw.add(sample("2026-09-21T10:03:00Z", 60, 600, 0, Map.of("orders", 5L),
                counting.plusSeconds(1)));
        raw.add(sample("2026-09-21T10:30:00Z", 120, 1_200, 0, Map.of("orders", 4L),
                counting.plusSeconds(1)));
        raw.add(sample("2026-09-21T10:31:00Z", 180, 1_800, 0, Map.of("orders", 3L),
                counting.plusSeconds(1)));
        RecordingRollups cache = new RecordingRollups();
        Instant from = Instant.parse("2026-09-21T10:00:00Z");
        cache.add(projectRollup(raw, from));
        cache.add(projectRollup(raw, from.plusSeconds(1_800)));
        PipelineHistoryQuery request = new PipelineHistoryQuery("orders", from,
                from.plusSeconds(3_600), HistoryResolution.PT30M, 100, List.of("orders"), null);

        PipelineMetricsHistory expected = service(raw, 128, 25_000).query(request);
        raw.rawReads = 0;
        raw.pageRanges.clear();
        PipelineMetricsHistory actual = cachedService(raw, cache).query(request);

        assertThat(actual).isEqualTo(expected);
        assertThat(raw.rawReads).isZero();

        RecordingRollups firstOnly = new RecordingRollups();
        firstOnly.add(cache.buckets.get(new HistoryRollupStore.Key("orders",
                HistoryRollupStore.Scope.legacy(), HistoryRollupStore.Resolution.PT30M, from)));
        raw.pageRanges.clear();
        assertThat(cachedService(raw, firstOnly).query(request)).isEqualTo(expected);
        assertThat(raw.pageRanges).containsExactly(new TimeRange(from.plusSeconds(1_800),
                from.plusSeconds(3_600)));

        RecordingRollups secondOnly = new RecordingRollups();
        secondOnly.add(cache.buckets.get(new HistoryRollupStore.Key("orders",
                HistoryRollupStore.Scope.legacy(), HistoryRollupStore.Resolution.PT30M,
                from.plusSeconds(1_800))));
        raw.pageRanges.clear();
        assertThat(cachedService(raw, secondOnly).query(request)).isEqualTo(expected);
        assertThat(raw.pageRanges).containsExactly(new TimeRange(from, from.plusSeconds(1_800)));

        String cachedCursor = null;
        String rawCursor = null;
        for (int page = 0; page < 3; page++) {
            PipelineMetricsHistory cachedPage = cachedService(raw, cache).query(new PipelineHistoryQuery(
                    "orders", from, from.plusSeconds(3_600), HistoryResolution.PT30M,
                    1, List.of("orders"), cachedCursor));
            PipelineMetricsHistory rawPage = service(raw, 128, 25_000).query(new PipelineHistoryQuery(
                    "orders", from, from.plusSeconds(3_600), HistoryResolution.PT30M,
                    1, List.of("orders"), rawCursor));
            assertThat(cachedPage.segments()).isEqualTo(rawPage.segments());
            assertThat(cachedPage.gaps()).isEqualTo(rawPage.gaps());
            assertThat(cachedPage.nextCursor() == null).isEqualTo(rawPage.nextCursor() == null);
            cachedCursor = cachedPage.nextCursor();
            rawCursor = rawPage.nextCursor();
        }
        assertThat(cachedCursor).isNull();
    }

    @Test
    void aLegacyCacheWithoutSampleProofFallsBackToRaw() {
        RecordingHistory raw = oneHourOfSamples();
        RecordingRollups cache = new RecordingRollups();
        Instant from = Instant.parse("2026-09-21T10:00:00Z");
        HistoryRollupStore.Bucket current = projectRollup(raw, from);
        cache.add(new HistoryRollupStore.Bucket(current.key(), current.computedAt(),
                current.inputReadStartedAt(), current.validUntil(), current.requiresFinerResolution(),
                current.fragments(), current.gaps()));

        PipelineMetricsHistory history = cachedService(raw, cache).query(new PipelineHistoryQuery(
                "orders", from, from.plusSeconds(1_800), HistoryResolution.PT30M,
                100, List.of(), null));

        assertThat(history.status()).isEqualTo(PipelineMetricsHistory.Status.OK);
        assertThat(raw.pageRanges).containsExactly(new TimeRange(from, from.plusSeconds(1_800)));
    }

    @Test
    void derivedIntervalsWithoutInWindowSamplesDoNotInventRetainedHistory() {
        RecordingHistory raw = new RecordingHistory();
        RecordingRollups cache = new RecordingRollups();
        Instant from = Instant.parse("2026-09-21T10:00:00Z");
        cache.add(rollup(from, HistoryRollupStore.Resolution.PT30M, 0, NOW.plusSeconds(120)));

        PipelineMetricsHistory history = cachedService(raw, cache).query(new PipelineHistoryQuery(
                "orders", from, from.plusSeconds(1_800), HistoryResolution.PT30M,
                100, List.of(), null));

        assertThat(history.status()).isEqualTo(PipelineMetricsHistory.Status.NO_RETAINED_SAMPLES);
        assertThat(history.segments()).isEmpty();
        assertThat(raw.rawReads).isZero();
    }

    @Test
    void anExpiredCursorAnchorContinuesThroughTheRawKey() {
        RecordingHistory raw = oneHourOfSamples();
        RecordingRollups cache = new RecordingRollups();
        Instant from = Instant.parse("2026-09-21T10:00:00Z");
        HistoryRollupStore.Bucket firstBucket = projectRollup(raw, from);
        cache.add(firstBucket);
        cache.add(projectRollup(raw, from.plusSeconds(1_800)));
        PipelineHistoryQuery request = new PipelineHistoryQuery("orders", from,
                from.plusSeconds(3_600), HistoryResolution.PT30M, 1, List.of(), null);
        PipelineHistoryQueryService cached = cachedService(raw, cache);

        PipelineMetricsHistory first = cached.query(request);
        cache.add(new HistoryRollupStore.Bucket(firstBucket.key(), firstBucket.computedAt(),
                firstBucket.inputReadStartedAt(), NOW.minusSeconds(1), false,
                firstBucket.fragments(), firstBucket.gaps(), firstBucket.inWindowSamples()));
        PipelineMetricsHistory resumed = cached.query(new PipelineHistoryQuery("orders", from,
                from.plusSeconds(3_600), HistoryResolution.PT30M, 1, List.of(), first.nextCursor()));

        PipelineMetricsHistory rawFirst = service(raw, 128, 25_000).query(request);
        PipelineMetricsHistory rawSecond = service(raw, 128, 25_000).query(new PipelineHistoryQuery(
                "orders", from, from.plusSeconds(3_600), HistoryResolution.PT30M,
                1, List.of(), rawFirst.nextCursor()));
        assertThat(resumed).isEqualTo(rawSecond);
        assertThat(raw.rawReads).isGreaterThan(0);
    }

    @Test
    void aRecreatedResourceCannotReceiveTheOldIncarnationCacheResult() {
        AtomicReference<String> owner = new AtomicReference<>("inc-a");
        RecordingHistory raw = new RecordingHistory();
        RecordingRollups cache = new RecordingRollups();
        Instant from = Instant.parse("2026-09-21T10:00:00Z");
        HistoryRollupStore.Bucket legacy = rollup(from,
                HistoryRollupStore.Resolution.PT30M, 1, NOW.plusSeconds(120));
        cache.add(new HistoryRollupStore.Bucket(new HistoryRollupStore.Key("orders",
                HistoryRollupStore.Scope.incarnation("inc-a"),
                HistoryRollupStore.Resolution.PT30M, from), legacy.computedAt(),
                legacy.inputReadStartedAt(), legacy.validUntil(), false,
                legacy.fragments(), legacy.gaps(), legacy.inWindowSamples()));
        cache.onRangeRead = () -> owner.set("inc-b");
        PipelineHistoryQueryService service = new PipelineHistoryQueryService(
                artifactsWithOwnerFrom(owner::get, "orders"), raw, cache,
                Duration.ofMinutes(1), fixedClock(), cursorCodec());

        TapstateException mismatch = catchThrowableOfType(() -> service.query(
                new PipelineHistoryQuery("orders", from, from.plusSeconds(1_800),
                        HistoryResolution.PT30M, 100, List.of(), null)), TapstateException.class);

        assertThat(mismatch.code()).isEqualTo(MonitorError.INVALID_CURSOR);
        assertThat(mismatch.args()).containsEntry("reason", "QUERY_MISMATCH");
        assertThat(raw.rawReads).isZero();
    }

    private static PipelineHistoryQueryService cachedService(RecordingHistory raw, RecordingRollups cache) {
        return new PipelineHistoryQueryService(artifactsWith("orders"), raw, cache,
                Duration.ofMinutes(1), fixedClock(), cursorCodec(), 128, 25_000);
    }

    private static HistoryRollupStore.Bucket rollup(Instant at,
            HistoryRollupStore.Resolution resolution, int inWindowSamples, Instant validUntil) {
        HistoryRollupStore.Key key = new HistoryRollupStore.Key("orders",
                HistoryRollupStore.Scope.legacy(), resolution, at);
        HistoryRollupStore.Rate rate = new HistoryRollupStore.Rate(
                java.math.BigDecimal.valueOf(inWindowSamples * 60L),
                java.math.BigDecimal.ONE, java.math.BigDecimal.ONE);
        HistoryRollupStore.Fragment fragment = new HistoryRollupStore.Fragment(0,
                HistoryRollupStore.StartReason.CONTINUATION, at, key.bucketEnd(),
                rate, rate, List.of());
        Instant computed = key.bucketEnd().isAfter(NOW.minusSeconds(30)) ? NOW : NOW.minusSeconds(30);
        return new HistoryRollupStore.Bucket(key, computed, computed,
                validUntil, false, List.of(fragment), List.of(), inWindowSamples);
    }

    private static HistoryRollupStore.Bucket anchoredRollup(Instant at, String rawKey) {
        HistoryRollupStore.Key key = new HistoryRollupStore.Key("orders",
                HistoryRollupStore.Scope.legacy(), HistoryRollupStore.Resolution.PT30M, at);
        HistoryRollupStore.Rate rate = new HistoryRollupStore.Rate(
                java.math.BigDecimal.valueOf(1_800), java.math.BigDecimal.ONE, java.math.BigDecimal.ONE);
        HistoryRollupStore.Fragment fragment = new HistoryRollupStore.Fragment(0,
                HistoryRollupStore.StartReason.CONTINUATION, at, key.bucketEnd(),
                rate, rate, List.of(), null, null,
                new RateHistoryStore.Key(key.bucketEnd().minusSeconds(60), rawKey), key.bucketEnd());
        return new HistoryRollupStore.Bucket(key, NOW.minusSeconds(30), NOW.minusSeconds(30),
                NOW.plusSeconds(120), false, List.of(fragment), List.of(), 30);
    }

    private static HistoryRollupStore.Bucket projectRollup(RecordingHistory raw, Instant at) {
        Instant end = at.plusSeconds(1_800);
        Entry predecessor = raw.entries.stream().filter(entry -> entry.key().observedAt().isBefore(at))
                .max(RecordingHistory.ORDER).orElse(null);
        List<Entry> entries = raw.entries.stream()
                .filter(entry -> !entry.key().observedAt().isBefore(at)
                        && entry.key().observedAt().isBefore(end))
                .toList();
        Entry successor = raw.entries.stream().filter(entry -> !entry.key().observedAt().isBefore(end))
                .min(RecordingHistory.ORDER).orElse(null);
        HistoryAggregator aggregator = new HistoryAggregator(at, end, at, Duration.ofMinutes(30),
                Duration.ofMinutes(1), List.of("orders"),
                predecessor == null ? PipelineMetricsHistory.StartReason.WINDOW_START
                        : PipelineMetricsHistory.StartReason.CONTINUATION, 65);
        aggregator.begin(predecessor);
        entries.forEach(aggregator::add);
        HistoryAggregator.Projection projection = aggregator.finish(successor);
        List<HistoryRollupStore.Fragment> fragments = projection.points().stream()
                .map(point -> new HistoryRollupStore.Fragment(point.segment(),
                        HistoryRollupStore.StartReason.valueOf(point.startReason().name()),
                        point.point().intervalStart(), point.point().intervalEnd(),
                        cachedRate(point.point().recordsOut()), cachedRate(point.point().bytesOut()),
                        point.point().lag().stream().map(lag -> new HistoryRollupStore.Lag(
                                lag.table(), lag.observedAt(), lag.last(), lag.max())).toList(),
                        null, null, point.resumeAfter(), point.resumeAt()))
                .toList();
        List<HistoryRollupStore.Gap> gaps = projection.gaps().stream()
                .map(gap -> new HistoryRollupStore.Gap(gap.segment(), gap.gap().intervalStart(),
                        gap.gap().intervalEnd(), HistoryRollupStore.GapReason.SAMPLE_GAP))
                .toList();
        return new HistoryRollupStore.Bucket(new HistoryRollupStore.Key("orders",
                HistoryRollupStore.Scope.legacy(), HistoryRollupStore.Resolution.PT30M, at),
                NOW.minusSeconds(30), NOW.minusSeconds(30), NOW.plusSeconds(120),
                false, fragments, gaps, entries.size());
    }

    private static HistoryRollupStore.Rate cachedRate(PipelineMetricsHistory.Rate rate) {
        return rate == null ? null : new HistoryRollupStore.Rate(
                rate.delta(), rate.averageRate(), rate.maxRate());
    }

    private record TimeRange(Instant from, Instant to) {
    }

    @Test
    void rawHistorySplitsARebuiltExecutionWithoutInventingACounterReset() {
        RecordingHistory history = new RecordingHistory();
        ObservationStore.Scope old = new ObservationStore.Scope("inc-a", 41);
        ObservationStore.Scope rebuilt = new ObservationStore.Scope("inc-a", 42);
        history.addScoped(sample("2026-09-21T10:00:00Z", 7, 70, 7, Map.of(), COUNTING_SINCE), old);
        history.addScoped(sample("2026-09-21T10:01:00Z", 9, 90, 9, Map.of(), COUNTING_SINCE), rebuilt);
        history.addScoped(sample("2026-09-21T10:02:00Z", 12, 120, 12, Map.of(), COUNTING_SINCE), rebuilt);
        PipelineHistoryQueryService service = new PipelineHistoryQueryService(
                artifactsWithOwner("inc-a", "orders"), history, Duration.ofMinutes(1), fixedClock(),
                cursorCodec(), 10, 100);

        PipelineMetricsHistory raw = service.query(query("2026-09-21T10:00:00Z",
                "2026-09-21T10:03:00Z", HistoryResolution.RAW, 10, List.of(), null));

        assertThat(raw.segments()).extracting(PipelineMetricsHistory.Segment::startReason)
                .containsExactly(PipelineMetricsHistory.StartReason.WINDOW_START,
                        PipelineMetricsHistory.StartReason.CONTINUATION);
        assertThat(raw.segments().get(1).points().getFirst().recordsOut()).isNull();
        assertThat(raw.segments().get(1).points().getLast().recordsOut().delta())
                .isEqualByComparingTo("3");
        assertThat(raw.gaps()).isEmpty();
    }

    private static final Instant NOW = Instant.parse("2026-09-21T12:00:00Z");
    private static final Instant COUNTING_SINCE = Instant.parse("2026-09-21T00:00:00Z");
    private static final byte[] SECRET = "history-query-test-secret".getBytes(StandardCharsets.UTF_8);

    @Test
    void rawPagesUseThePredecessorKeepDuplicateTimesAndNeverProjectInboundRates() {
        RecordingHistory history = new RecordingHistory();
        history.add(sample("2026-09-21T10:00:00Z", 60, 600, 10_059, Map.of("orders", 1L), COUNTING_SINCE));
        history.add(sample("2026-09-21T10:01:00Z", 120, 1_200, 10_119, Map.of("orders", 2L), COUNTING_SINCE));
        history.add(sample("2026-09-21T10:02:00Z", 180, 1_800, 10_179, Map.of(), COUNTING_SINCE));
        history.add(sample("2026-09-21T10:02:00Z", 190, 1_900, 10_189, Map.of("orders", 5L), COUNTING_SINCE));
        history.add(sample("2026-09-21T10:10:00Z", 5, 50, 10_200, Map.of("orders", 7L),
                COUNTING_SINCE.plusSeconds(1)));
        PipelineHistoryQueryService service = service(history, 1024, 25_000);
        PipelineHistoryQuery firstQuery = query("2026-09-21T10:00:30Z", "2026-09-21T10:20:00Z",
                HistoryResolution.RAW, 2, List.of("orders"), null);

        PipelineMetricsHistory first = service.query(firstQuery);
        PipelineMetricsHistory second = service.query(new PipelineHistoryQuery(firstQuery.pipelineId(),
                firstQuery.from(), firstQuery.to(), firstQuery.resolution(), firstQuery.limit(),
                firstQuery.tables(), first.nextCursor()));

        List<PipelineMetricsHistory.Point> firstPoints = points(first);
        List<PipelineMetricsHistory.Point> secondPoints = points(second);
        assertThat(first.effectiveResolution()).isEqualTo(EffectiveHistoryResolution.PT1M);
        assertThat(first.consistency()).isEqualTo(PipelineMetricsHistory.Consistency.EVENTUAL);
        assertThat(firstPoints).hasSize(2);
        assertThat(firstPoints.get(0).intervalStart()).isEqualTo(Instant.parse("2026-09-21T10:00:00Z"));
        assertThat(firstPoints.get(0).recordsOut().averageRate()).isEqualByComparingTo("1");
        assertThat(firstPoints.get(0).bytesOut().averageRate()).isEqualByComparingTo("10");
        assertThat(first.nextCursor()).isNotBlank();

        assertThat(secondPoints).hasSize(2);
        assertThat(secondPoints.get(0).intervalStart()).isEqualTo(secondPoints.get(0).intervalEnd());
        assertThat(secondPoints.get(0).recordsOut()).isNull();
        assertThat(secondPoints.get(0).lag()).extracting(PipelineMetricsHistory.Lag::last).containsExactly(5L);
        assertThat(second.segments()).extracting(PipelineMetricsHistory.Segment::startReason)
                .containsExactly(PipelineMetricsHistory.StartReason.CONTINUATION,
                        PipelineMetricsHistory.StartReason.GAP);
        assertThat(second.gaps()).singleElement().satisfies(gap ->
                assertThat(gap.intervalStart()).isEqualTo(Instant.parse("2026-09-21T10:02:00Z")));
        assertThat(second.nextCursor()).isNull();

        TapstateException mismatch = catchThrowableOfType(() -> service.query(new PipelineHistoryQuery(
                firstQuery.pipelineId(), firstQuery.from(), firstQuery.to(), firstQuery.resolution(),
                firstQuery.limit(), List.of("items"), first.nextCursor())), TapstateException.class);
        assertThat(mismatch.code()).isEqualTo(MonitorError.INVALID_CURSOR);
        assertThat(mismatch.args()).containsEntry("reason", "QUERY_MISMATCH");
    }

    @Test
    void autoUsesTheFixedResolutionLadder() {
        RecordingHistory history = new RecordingHistory();
        PipelineHistoryQueryService service = service(history, 1024, 25_000);

        assertThat(auto(service, Duration.ofHours(1))).isEqualTo(EffectiveHistoryResolution.PT1M);
        assertThat(auto(service, Duration.ofHours(6))).isEqualTo(EffectiveHistoryResolution.PT5M);
        assertThat(auto(service, Duration.ofDays(1))).isEqualTo(EffectiveHistoryResolution.PT30M);
        assertThat(auto(service, Duration.ofDays(3))).isEqualTo(EffectiveHistoryResolution.PT1H);
        assertThat(auto(service, Duration.ofDays(7))).isEqualTo(EffectiveHistoryResolution.PT3H);
        assertThat(auto(service, Duration.ofDays(15))).isEqualTo(EffectiveHistoryResolution.PT6H);
    }

    @Test
    void aggregateIsIdenticalAcrossRawBatchesAndReportsBoundedCost() {
        RecordingHistory byTwo = oneHourOfSamples();
        RecordingHistory bySeven = oneHourOfSamples();
        PipelineHistoryQuery request = query("2026-09-21T10:00:00Z", "2026-09-21T11:00:00Z",
                HistoryResolution.PT30M, 10, List.of("orders"), null);

        PipelineHistoryQueryService.QueryRun two = service(byTwo, 2, 25_000).execute(request);
        PipelineHistoryQueryService.QueryRun seven = service(bySeven, 7, 25_000).execute(request);

        assertThat(two.history()).isEqualTo(seven.history());
        assertThat(points(two.history())).hasSize(2);
        assertThat(points(two.history()).get(0).recordsOut().delta()).isEqualByComparingTo("1800");
        assertThat(points(two.history()).get(1).recordsOut().delta()).isEqualByComparingTo("1800");
        assertThat(two.cost().peakRawEntriesHeld()).isLessThanOrEqualTo(3);
        assertThat(seven.cost().peakRawEntriesHeld()).isLessThanOrEqualTo(8);
        assertThat(two.cost().rawDocumentsScanned()).isLessThanOrEqualTo(100);
        assertThat(byTwo.requestedLimits).containsOnly(2);
        assertThat(bySeven.requestedLimits).containsOnly(7);
    }

    @Test
    void aggregateCursorContinuesAtACompleteBucketWithoutDuplicatingItsDelta() {
        RecordingHistory history = oneHourOfSamples();
        PipelineHistoryQueryService service = service(history, 7, 25_000);
        PipelineHistoryQuery firstQuery = query("2026-09-21T10:00:00Z", "2026-09-21T11:00:00Z",
                HistoryResolution.PT30M, 1, List.of("orders"), null);

        PipelineMetricsHistory first = service.query(firstQuery);
        PipelineMetricsHistory second = service.query(new PipelineHistoryQuery(firstQuery.pipelineId(),
                firstQuery.from(), firstQuery.to(), firstQuery.resolution(), firstQuery.limit(),
                firstQuery.tables(), first.nextCursor()));

        assertThat(points(first)).singleElement().satisfies(point ->
                assertThat(point.recordsOut().delta()).isEqualByComparingTo("1800"));
        assertThat(points(second)).singleElement().satisfies(point -> {
            assertThat(point.intervalStart()).isEqualTo(Instant.parse("2026-09-21T10:30:00Z"));
            assertThat(point.intervalEnd()).isEqualTo(Instant.parse("2026-09-21T11:00:00Z"));
            assertThat(point.recordsOut().delta()).isEqualByComparingTo("1800");
        });
        assertThat(second.nextCursor()).isNull();
    }

    @Test
    void cumulativeRawScanBudgetFailsBeforeReturningAnIncompleteBucket() {
        RecordingHistory history = oneHourOfSamples();
        PipelineHistoryQueryService service = service(history, 2, 5);
        PipelineHistoryQuery request = query("2026-09-21T10:00:00Z", "2026-09-21T11:00:00Z",
                HistoryResolution.PT30M, 10, List.of(), null);

        TapstateException refusal = catchThrowableOfType(() -> service.query(request), TapstateException.class);

        assertThat(refusal.code()).isEqualTo(MonitorError.QUERY_BUDGET_EXCEEDED);
        assertThat(refusal.args()).containsEntry("budget", "RAW_SCAN").containsEntry("limit", 5);
    }

    @Test
    void oneHourOneDayAndFifteenDaysStayInsideTheMachineIndependentCostGates() {
        List<Duration> spans = List.of(Duration.ofHours(1), Duration.ofDays(1), Duration.ofDays(15));
        List<EffectiveHistoryResolution> resolutions = List.of(
                EffectiveHistoryResolution.PT1M,
                EffectiveHistoryResolution.PT30M,
                EffectiveHistoryResolution.PT6H);
        List<Integer> expectedPoints = List.of(60, 48, 60);

        for (int i = 0; i < spans.size(); i++) {
            Duration span = spans.get(i);
            RecordingHistory history = samplesFor(span);
            PipelineHistoryQueryService.QueryRun run = service(history, 1024, 25_000).execute(
                    new PipelineHistoryQuery("orders", NOW.minus(span), NOW,
                            HistoryResolution.AUTO, PipelineHistoryQuery.DEFAULT_LIMIT, List.of("orders"), null));

            assertThat(run.history().effectiveResolution()).isEqualTo(resolutions.get(i));
            assertThat(run.cost().outputPoints()).isEqualTo(expectedPoints.get(i));
            assertThat(run.cost().rawDocumentsScanned()).isLessThanOrEqualTo(25_000);
            assertThat(run.cost().peakRawEntriesHeld()).isLessThanOrEqualTo(1025);
            assertThat(run.history().nextCursor()).isNull();
        }
    }

    @Test
    void emptyHistoryIsNotGuessedAndUnknownPipelineIsDifferent() {
        RecordingHistory history = new RecordingHistory();
        PipelineHistoryQueryService service = service(history, 1024, 25_000);
        PipelineHistoryQuery request = query("2026-09-21T10:00:00Z", "2026-09-21T11:00:00Z",
                HistoryResolution.RAW, 10, List.of(), null);

        PipelineMetricsHistory empty = service.query(request);

        assertThat(empty.status()).isEqualTo(PipelineMetricsHistory.Status.NO_RETAINED_SAMPLES);
        assertThat(empty.retentionCutoff()).isEqualTo(NOW.minus(Duration.ofDays(15)));
        assertThat(empty.segments()).isEmpty();
        assertThat(empty.unavailable()).isEmpty();

        PipelineHistoryQueryService unknown = new PipelineHistoryQueryService(artifactsWith(), history,
                Duration.ofMinutes(1), fixedClock(), cursorCodec(), 1024, 25_000);
        TapstateException refusal = catchThrowableOfType(() -> unknown.query(request), TapstateException.class);
        assertThat(refusal.code()).isEqualTo(LifecycleError.UNKNOWN_PIPELINE);
    }

    @Test
    void aTtlSweepLagCannotUseAnExpiredPredecessorToManufactureTheFirstRate() {
        Instant cutoff = NOW.minus(Duration.ofDays(15));
        RecordingHistory history = new RecordingHistory();
        history.add(new RateSample("orders", cutoff.minusSeconds(60),
                Map.of("records.out", 0L, "bytes.out", 0L), Map.of(), COUNTING_SINCE));
        history.add(new RateSample("orders", cutoff.plusSeconds(60),
                Map.of("records.out", 120L, "bytes.out", 1_200L), Map.of(), COUNTING_SINCE));

        PipelineMetricsHistory response = service(history, 1024, 25_000).query(new PipelineHistoryQuery(
                "orders", cutoff, cutoff.plusSeconds(120), HistoryResolution.RAW, 10, List.of(), null));

        assertThat(points(response)).singleElement().satisfies(point -> {
            assertThat(point.intervalStart()).isEqualTo(cutoff.plusSeconds(60));
            assertThat(point.intervalEnd()).isEqualTo(cutoff.plusSeconds(60));
            assertThat(point.recordsOut()).isNull();
            assertThat(point.bytesOut()).isNull();
        });
        assertThat(response.unavailable()).extracting(PipelineMetricsHistory.Unavailable::metric)
                .containsExactly("records.out", "bytes.out");
    }

    private static EffectiveHistoryResolution auto(PipelineHistoryQueryService service, Duration span) {
        return service.query(new PipelineHistoryQuery("orders", NOW.minus(span), NOW,
                HistoryResolution.AUTO, 10, List.of(), null)).effectiveResolution();
    }

    private static PipelineHistoryQuery query(String from, String to, HistoryResolution resolution,
            int limit, List<String> tables, String cursor) {
        return new PipelineHistoryQuery("orders", Instant.parse(from), Instant.parse(to),
                resolution, limit, tables, cursor);
    }

    private static PipelineHistoryQueryService service(RecordingHistory history, int batch, int budget) {
        return new PipelineHistoryQueryService(artifactsWith("orders"), history, Duration.ofMinutes(1),
                fixedClock(), cursorCodec(), batch, budget);
    }

    private static Clock fixedClock() {
        return Clock.fixed(NOW, ZoneOffset.UTC);
    }

    private static HistoryCursorCodec cursorCodec() {
        return new HistoryCursorCodec(SECRET, fixedClock());
    }

    private static List<PipelineMetricsHistory.Point> points(PipelineMetricsHistory history) {
        return history.segments().stream().flatMap(segment -> segment.points().stream()).toList();
    }

    private static RecordingHistory oneHourOfSamples() {
        RecordingHistory history = new RecordingHistory();
        Instant first = Instant.parse("2026-09-21T09:59:00Z");
        for (int minute = 0; minute <= 61; minute++) {
            Instant at = first.plus(Duration.ofMinutes(minute));
            long seconds = minute * 60L;
            history.add(new RateSample("orders", at,
                    Map.of("records.out", seconds, "bytes.out", seconds * 10, "records.in", seconds * 99),
                    Map.of("orders", (long) (minute % 7)), COUNTING_SINCE));
        }
        return history;
    }

    private static RecordingHistory samplesFor(Duration span) {
        RecordingHistory history = new RecordingHistory();
        Instant first = NOW.minus(span).minus(Duration.ofMinutes(1));
        long minutes = span.toMinutes() + 1;
        for (long minute = 0; minute <= minutes; minute++) {
            Instant at = first.plus(Duration.ofMinutes(minute));
            long seconds = minute * 60L;
            history.add(new RateSample("orders", at,
                    Map.of("records.out", seconds, "bytes.out", seconds * 10),
                    Map.of("orders", minute % 11), COUNTING_SINCE));
        }
        return history;
    }

    private static RateSample sample(String at, long records, long bytes, long inbound,
            Map<String, Long> lag, Instant countingSince) {
        return new RateSample("orders", Instant.parse(at),
                Map.of("records.out", records, "bytes.out", bytes, "records.in", inbound), lag, countingSince);
    }

    private static ArtifactQueryService artifactsWith(String... ids) {
        return artifactsWithOwner(null, ids);
    }

    private static ArtifactQueryService artifactsWithOwner(String incarnation, String... ids) {
        return artifactsWithOwnerFrom(() -> incarnation, ids);
    }

    private static ArtifactQueryService artifactsWithOwnerFrom(Supplier<String> incarnation, String... ids) {
        Map<String, Resource> resources = new LinkedHashMap<>();
        for (String id : ids) {
            resources.put(id, new DslParser().parse("""
                    version: tapstate/v1
                    kind: pipeline
                    id: %s
                    source: source
                    serve:
                      from: /.*/
                      sync:
                        - id: sink
                          source: target
                          write_mode: upsert
                          ddl: apply
                    """.formatted(id)));
        }
        return new ArtifactQueryService(new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> artifacts) {
                artifacts.forEach(resource -> resources.put(resource.id(), resource));
            }

            @Override
            public Optional<Resource> get(String id) {
                return Optional.ofNullable(resources.get(id));
            }

            @Override
            public List<Resource> list() {
                return List.copyOf(resources.values());
            }

            @Override
            public Optional<HistoryOwner> pipelineHistoryOwner(String id) {
                String current = incarnation.get();
                return resources.containsKey(id)
                        ? Optional.of(new HistoryOwner(new RateHistoryStore.Visibility(
                                current, current == null)))
                        : Optional.empty();
            }
        });
    }

    private static final class RecordingHistory implements RateHistoryStore {
        private static final Comparator<Entry> ORDER = Comparator
                .comparing((Entry entry) -> entry.key().observedAt())
                .thenComparing(entry -> entry.key().internalKey());

        private final List<Entry> entries = new ArrayList<>();
        private final List<Integer> requestedLimits = new ArrayList<>();
        private final List<TimeRange> pageRanges = new ArrayList<>();
        private int rawReads;
        private long nextKey;

        void add(RateSample sample) {
            append(sample);
        }

        void addScoped(RateSample sample, ObservationStore.Scope scope) {
            addEntry(sample, Optional.of(scope));
        }

        @Override
        public void append(RateSample sample) {
            addEntry(sample, Optional.empty());
        }

        private void addEntry(RateSample sample, Optional<ObservationStore.Scope> scope) {
            Entry entry = new Entry(new Key(sample.observedAt(), "%020d".formatted(nextKey++)), sample, scope);
            boolean ordered = entries.isEmpty() || ORDER.compare(entries.getLast(), entry) <= 0;
            entries.add(entry);
            if (!ordered) {
                entries.sort(ORDER);
            }
        }

        @Override
        public Page readPage(String pipelineId, Instant from, Instant to, Key after, int limit) {
            return page(pipelineId, null, from, to, after, limit);
        }

        private Page page(String pipelineId, Visibility visibility, Instant from, Instant to, Key after, int limit) {
            rawReads++;
            pageRanges.add(new TimeRange(from, to));
            requestedLimits.add(limit);
            List<Entry> found = entries.stream()
                    .filter(entry -> visible(entry, visibility))
                    .filter(entry -> entry.sample().pipelineId().equals(pipelineId))
                    .filter(entry -> !entry.key().observedAt().isBefore(from)
                            && entry.key().observedAt().isBefore(to))
                    .filter(entry -> after == null || compare(entry.key(), after) > 0)
                    .limit((long) limit + 1)
                    .toList();
            boolean hasMore = found.size() > limit;
            return new Page(hasMore ? found.subList(0, limit) : found, hasMore);
        }

        @Override
        public Page readPageVisible(String pipelineId, Visibility visibility,
                Instant from, Instant to, Key after, int limit) {
            return page(pipelineId, visibility, from, to, after, limit);
        }

        @Override
        public Optional<Entry> read(String pipelineId, Key key) {
            return entries.stream().filter(entry -> entry.sample().pipelineId().equals(pipelineId))
                    .filter(entry -> entry.key().equals(key)).findFirst();
        }

        @Override
        public Optional<Entry> readVisible(String pipelineId, Visibility visibility, Key key) {
            rawReads++;
            return entries.stream().filter(entry -> visible(entry, visibility))
                    .filter(entry -> entry.sample().pipelineId().equals(pipelineId))
                    .filter(entry -> entry.key().equals(key)).findFirst();
        }

        @Override
        public Optional<Entry> predecessor(String pipelineId, Instant at) {
            return entries.stream().filter(entry -> entry.sample().pipelineId().equals(pipelineId))
                    .filter(entry -> entry.key().observedAt().isBefore(at)).max(ORDER);
        }

        @Override
        public Optional<Entry> predecessorVisible(String pipelineId, Visibility visibility, Instant at) {
            rawReads++;
            return entries.stream().filter(entry -> visible(entry, visibility))
                    .filter(entry -> entry.sample().pipelineId().equals(pipelineId))
                    .filter(entry -> entry.key().observedAt().isBefore(at)).max(ORDER);
        }

        @Override
        public Optional<Entry> successor(String pipelineId, Instant at) {
            return entries.stream().filter(entry -> entry.sample().pipelineId().equals(pipelineId))
                    .filter(entry -> !entry.key().observedAt().isBefore(at)).min(ORDER);
        }

        @Override
        public Optional<Entry> successorVisible(String pipelineId, Visibility visibility, Instant at) {
            rawReads++;
            return entries.stream().filter(entry -> visible(entry, visibility))
                    .filter(entry -> entry.sample().pipelineId().equals(pipelineId))
                    .filter(entry -> !entry.key().observedAt().isBefore(at)).min(ORDER);
        }

        private static boolean visible(Entry entry, Visibility visibility) {
            if (visibility == null) {
                return true;
            }
            return entry.scope().map(scope -> scope.pipelineIncarnationId().equals(visibility.incarnationId()))
                    .orElseGet(visibility::includeLegacy);
        }

        @Override
        public void deleteAll(String pipelineId) {
            entries.removeIf(entry -> entry.sample().pipelineId().equals(pipelineId));
        }

        @Override
        public Duration retention() {
            return Duration.ofDays(15);
        }

        private static int compare(Key left, Key right) {
            int time = left.observedAt().compareTo(right.observedAt());
            return time != 0 ? time : left.internalKey().compareTo(right.internalKey());
        }
    }

    private static final class RecordingRollups implements HistoryRollupStore {
        private final Map<HistoryRollupStore.Key, HistoryRollupStore.Bucket> buckets = new LinkedHashMap<>();
        private int rangeReads;
        private Runnable onRangeRead = () -> { };

        void add(HistoryRollupStore.Bucket bucket) {
            buckets.put(bucket.key(), bucket);
        }

        @Override
        public void upsert(HistoryRollupStore.Bucket bucket) {
            add(bucket);
        }

        @Override
        public Optional<HistoryRollupStore.Bucket> read(HistoryRollupStore.Key key) {
            return Optional.ofNullable(buckets.get(key));
        }

        @Override
        public List<HistoryRollupStore.Bucket> readRange(String pipelineId, Scope scope,
                Resolution resolution, Instant from, Instant to, int limit) {
            rangeReads++;
            onRangeRead.run();
            return buckets.values().stream()
                    .filter(bucket -> bucket.key().pipelineId().equals(pipelineId))
                    .filter(bucket -> bucket.key().scope().equals(scope))
                    .filter(bucket -> bucket.key().resolution() == resolution)
                    .filter(bucket -> !bucket.key().bucketStart().isBefore(from)
                            && bucket.key().bucketStart().isBefore(to))
                    .sorted(Comparator.comparing(bucket -> bucket.key().bucketStart()))
                    .limit(limit).toList();
        }

        @Override
        public void deleteIncarnation(String pipelineId, String incarnationId) {
            buckets.keySet().removeIf(key -> key.pipelineId().equals(pipelineId)
                    && key.scope().incarnationId().filter(incarnationId::equals).isPresent());
        }

        @Override
        public void deleteLegacy(String pipelineId) {
            buckets.keySet().removeIf(key -> key.pipelineId().equals(pipelineId)
                    && key.scope().incarnationId().isEmpty());
        }

        @Override
        public Duration retention() {
            return Duration.ofDays(15);
        }
    }
}
