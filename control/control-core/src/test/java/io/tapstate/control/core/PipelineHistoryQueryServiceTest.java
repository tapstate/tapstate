package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.core.model.Resource;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.RateHistoryStore;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

class PipelineHistoryQueryServiceTest {

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
        });
    }

    private static final class RecordingHistory implements RateHistoryStore {
        private static final Comparator<Entry> ORDER = Comparator
                .comparing((Entry entry) -> entry.key().observedAt())
                .thenComparing(entry -> entry.key().internalKey());

        private final List<Entry> entries = new ArrayList<>();
        private final List<Integer> requestedLimits = new ArrayList<>();
        private long nextKey;

        void add(RateSample sample) {
            append(sample);
        }

        @Override
        public void append(RateSample sample) {
            Entry entry = new Entry(new Key(sample.observedAt(), "%020d".formatted(nextKey++)), sample);
            boolean ordered = entries.isEmpty() || ORDER.compare(entries.getLast(), entry) <= 0;
            entries.add(entry);
            if (!ordered) {
                entries.sort(ORDER);
            }
        }

        @Override
        public Page readPage(String pipelineId, Instant from, Instant to, Key after, int limit) {
            requestedLimits.add(limit);
            List<Entry> found = entries.stream()
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
        public Optional<Entry> read(String pipelineId, Key key) {
            return entries.stream().filter(entry -> entry.sample().pipelineId().equals(pipelineId))
                    .filter(entry -> entry.key().equals(key)).findFirst();
        }

        @Override
        public Optional<Entry> predecessor(String pipelineId, Instant at) {
            return entries.stream().filter(entry -> entry.sample().pipelineId().equals(pipelineId))
                    .filter(entry -> entry.key().observedAt().isBefore(at)).max(ORDER);
        }

        @Override
        public Optional<Entry> successor(String pipelineId, Instant at) {
            return entries.stream().filter(entry -> entry.sample().pipelineId().equals(pipelineId))
                    .filter(entry -> !entry.key().observedAt().isBefore(at)).min(ORDER);
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
}
