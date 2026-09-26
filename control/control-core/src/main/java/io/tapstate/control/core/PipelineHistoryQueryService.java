package io.tapstate.control.core;

import io.tapstate.control.core.HistoryAggregator.Emitted;
import io.tapstate.control.core.HistoryAggregator.EmittedGap;
import io.tapstate.control.core.HistoryCursorCodec.QueryBinding;
import io.tapstate.control.core.HistoryCursorCodec.State;
import io.tapstate.control.core.PipelineMetricsHistory.Consistency;
import io.tapstate.control.core.PipelineMetricsHistory.Gap;
import io.tapstate.control.core.PipelineMetricsHistory.GapReason;
import io.tapstate.control.core.PipelineMetricsHistory.Lag;
import io.tapstate.control.core.PipelineMetricsHistory.Point;
import io.tapstate.control.core.PipelineMetricsHistory.Rate;
import io.tapstate.control.core.PipelineMetricsHistory.Segment;
import io.tapstate.control.core.PipelineMetricsHistory.StartReason;
import io.tapstate.control.core.PipelineMetricsHistory.Status;
import io.tapstate.control.core.PipelineMetricsHistory.Unavailable;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.RateHistoryStore;
import io.tapstate.spi.store.RateHistoryStore.Entry;
import io.tapstate.spi.store.RateHistoryStore.Key;
import io.tapstate.spi.store.RateHistoryStore.Page;
import io.tapstate.spi.store.RateHistoryStore.Visibility;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/** The bounded, store-backed query service behind {@code pipeline.metrics.history}. */
public final class PipelineHistoryQueryService {

    public static final int DEFAULT_RAW_BATCH_SIZE = RateHistoryStore.MAX_PAGE_SIZE;
    public static final int DEFAULT_RAW_SCAN_BUDGET = 25_000;
    public static final int MAX_SERIES = 22;
    public static final Duration MAX_RANGE = Duration.ofDays(15);

    private static final String PIPELINE_KIND = "pipeline";
    private static final BigDecimal NANOS_PER_SECOND = BigDecimal.valueOf(1_000_000_000L);

    private final ArtifactQueryService artifacts;
    private final RateHistoryStore history;
    private final Duration sampleInterval;
    private final Clock clock;
    private final HistoryCursorCodec cursors;
    private final int rawBatchSize;
    private final int rawScanBudget;

    public PipelineHistoryQueryService(ArtifactQueryService artifacts, RateHistoryStore history,
            Duration sampleInterval, Clock clock, HistoryCursorCodec cursors) {
        this(artifacts, history, sampleInterval, clock, cursors,
                DEFAULT_RAW_BATCH_SIZE, DEFAULT_RAW_SCAN_BUDGET);
    }

    PipelineHistoryQueryService(ArtifactQueryService artifacts, RateHistoryStore history,
            Duration sampleInterval, Clock clock, HistoryCursorCodec cursors,
            int rawBatchSize, int rawScanBudget) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.history = Objects.requireNonNull(history, "history");
        this.sampleInterval = Objects.requireNonNull(sampleInterval, "sampleInterval");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.cursors = Objects.requireNonNull(cursors, "cursors");
        if (sampleInterval.isZero() || sampleInterval.isNegative()) {
            throw new IllegalArgumentException("the history sample interval is positive");
        }
        if (rawBatchSize < 1 || rawBatchSize > RateHistoryStore.MAX_PAGE_SIZE) {
            throw new IllegalArgumentException("the history raw batch is between 1 and "
                    + RateHistoryStore.MAX_PAGE_SIZE);
        }
        if (rawScanBudget < rawBatchSize) {
            throw new IllegalArgumentException("the history scan budget covers at least one raw batch");
        }
        this.rawBatchSize = rawBatchSize;
        this.rawScanBudget = rawScanBudget;
    }

    public PipelineMetricsHistory query(PipelineHistoryQuery query) {
        return execute(query).history();
    }

    /** Cost evidence retained for store-backed contract tests, never serialized on the public face. */
    record QueryCost(int storeReads, int rawDocumentsScanned, int outputPoints, int peakRawEntriesHeld) {
    }

    record QueryRun(PipelineMetricsHistory history, QueryCost cost) {
    }

    QueryRun execute(PipelineHistoryQuery request) {
        Normalized normalized = validate(request);
        Visibility visibility = requirePipeline(normalized.binding().pipelineId());
        QueryBinding unscoped = normalized.binding();
        normalized = new Normalized(new QueryBinding(unscoped.pipelineId(), unscoped.from(),
                unscoped.to(), unscoped.resolution(), unscoped.limit(), unscoped.tables(), visibility),
                normalized.tables(), normalized.cursor());

        Frozen frozen = freeze(normalized);
        EffectiveHistoryResolution effective = effectiveResolution(normalized.binding());
        if (!frozen.from().isBefore(frozen.to())) {
            PipelineMetricsHistory empty = response(normalized, frozen, effective,
                    Status.NO_RETAINED_SAMPLES, List.of(), List.of(), List.of(), null);
            return new QueryRun(empty, new QueryCost(0, 0, 0, 0));
        }

        Cost cost = new Cost();
        Entry predecessor = boundaryBefore(normalized, frozen, cost);
        QueryRun run = effective.raw()
                ? raw(normalized, frozen, effective, predecessor, cost)
                : aggregate(normalized, frozen, effective, predecessor, cost);
        if (!visibility.equals(requirePipeline(normalized.binding().pipelineId()))) {
            throw new TapstateException(MonitorError.INVALID_CURSOR,
                    Map.of("operation", "pipeline.metrics.history", "reason", "QUERY_MISMATCH"), null);
        }
        return run;
    }

    private QueryRun raw(Normalized normalized, Frozen frozen, EffectiveHistoryResolution effective,
            Entry predecessor, Cost cost) {
        QueryBinding binding = normalized.binding();
        Page page = history.readPageVisible(binding.pipelineId(), binding.visibility(),
                frozen.from(), frozen.to(), frozen.afterKey(),
                binding.limit());
        cost.page(page, predecessor == null ? 0 : 1);
        requireScanBudget(cost);

        if (page.entries().isEmpty() && frozen.afterKey() == null) {
            PipelineMetricsHistory empty = response(normalized, frozen, effective,
                    Status.NO_RETAINED_SAMPLES, List.of(), List.of(), List.of(), null);
            return measured(empty, cost);
        }

        Projection projection = rawProjection(page.entries(), predecessor,
                frozen.afterKey() == null ? StartReason.WINDOW_START : StartReason.CONTINUATION,
                frozen.from(), frozen.to(), normalized.tables());
        String next = null;
        if (page.hasMore() && !page.entries().isEmpty()) {
            Entry last = page.entries().getLast();
            next = cursors.issue(binding, frozen.from(), frozen.to(), frozen.cutoff(),
                    last.key(), last.sample().observedAt());
        }
        List<Segment> segments = segments(projection.points());
        List<Gap> gaps = relatedGaps(projection.points(), projection.gaps());
        List<Unavailable> unavailable = unavailable(segments, normalized.tables());
        PipelineMetricsHistory response = response(normalized, frozen, effective, Status.OK,
                segments, gaps, unavailable, next);
        return measured(response, cost);
    }

    private QueryRun aggregate(Normalized normalized, Frozen frozen, EffectiveHistoryResolution effective,
            Entry predecessor, Cost cost) {
        QueryBinding binding = normalized.binding();
        HistoryAggregator aggregator = new HistoryAggregator(frozen.from(), frozen.to(), frozen.resumeAt(),
                effective.duration(), sampleInterval, normalized.tables(),
                frozen.afterKey() == null ? StartReason.WINDOW_START : StartReason.CONTINUATION,
                binding.limit() + 1);
        aggregator.begin(predecessor);

        Key after = frozen.afterKey();
        boolean storeHasMore = true;
        int inWindowSamples = 0;
        while (storeHasMore && !aggregator.full()) {
            Page page = history.readPageVisible(binding.pipelineId(), binding.visibility(),
                    frozen.from(), frozen.to(), after, rawBatchSize);
            cost.page(page, predecessor == null ? 0 : 1);
            predecessor = null;
            requireScanBudget(cost);
            for (Entry entry : page.entries()) {
                aggregator.add(entry);
                inWindowSamples++;
                after = entry.key();
                if (aggregator.full()) {
                    break;
                }
            }
            storeHasMore = page.hasMore();
            if (page.entries().isEmpty()) {
                break;
            }
        }

        if (inWindowSamples == 0 && frozen.afterKey() == null) {
            PipelineMetricsHistory empty = response(normalized, frozen, effective,
                    Status.NO_RETAINED_SAMPLES, List.of(), List.of(), List.of(), null);
            return measured(empty, cost);
        }

        Entry successor = null;
        if (!aggregator.full() && !storeHasMore) {
            cost.storeReads++;
            Optional<Entry> found = history.successorVisible(binding.pipelineId(), binding.visibility(),
                    frozen.to());
            if (found.isPresent()) {
                successor = found.orElseThrow();
                cost.rawDocumentsScanned++;
                cost.peakRawEntriesHeld = Math.max(cost.peakRawEntriesHeld, 2);
                requireScanBudget(cost);
            }
        }

        HistoryAggregator.Projection all = aggregator.finish(successor);
        boolean hasMore = all.points().size() > binding.limit();
        List<Emitted> emitted = hasMore ? all.points().subList(0, binding.limit()) : all.points();
        String next = null;
        if (hasMore && !emitted.isEmpty()) {
            Emitted last = emitted.getLast();
            next = cursors.issue(binding, frozen.from(), frozen.to(), frozen.cutoff(),
                    last.resumeAfter(), last.resumeAt());
        }
        List<Segment> segments = segments(emitted);
        List<Gap> gaps = relatedGaps(emitted, all.gaps());
        List<Unavailable> unavailable = unavailable(segments, normalized.tables());
        PipelineMetricsHistory response = response(normalized, frozen, effective, Status.OK,
                segments, gaps, unavailable, next);
        cost.peakRawEntriesHeld = Math.max(cost.peakRawEntriesHeld, all.peakRawEntriesHeld());
        return measured(response, cost);
    }

    private Entry boundaryBefore(Normalized normalized, Frozen frozen, Cost cost) {
        QueryBinding binding = normalized.binding();
        if (frozen.afterKey() != null) {
            cost.storeReads++;
            Optional<Entry> exact = history.readVisible(binding.pipelineId(), binding.visibility(),
                    frozen.afterKey());
            if (exact.isPresent()) {
                cost.rawDocumentsScanned++;
                cost.peakRawEntriesHeld = 1;
                requireScanBudget(cost);
                return exact.orElseThrow();
            }
            cost.storeReads++;
            Optional<Entry> read = history.predecessorVisible(binding.pipelineId(), binding.visibility(),
                    frozen.resumeAt());
            read.ifPresent(ignored -> cost.rawDocumentsScanned++);
            Optional<Entry> fallback = retained(read, frozen);
            cost.peakRawEntriesHeld = read.isPresent() ? 1 : 0;
            requireScanBudget(cost);
            return fallback.orElse(null);
        }
        cost.storeReads++;
        Optional<Entry> read = history.predecessorVisible(binding.pipelineId(), binding.visibility(),
                frozen.from());
        read.ifPresent(ignored -> cost.rawDocumentsScanned++);
        Optional<Entry> predecessor = retained(read, frozen);
        cost.peakRawEntriesHeld = read.isPresent() ? 1 : 0;
        requireScanBudget(cost);
        return predecessor.orElse(null);
    }

    private static Optional<Entry> retained(Optional<Entry> boundary, Frozen frozen) {
        return boundary.filter(entry -> !entry.sample().observedAt().isBefore(frozen.cutoff()));
    }

    private Normalized validate(PipelineHistoryQuery query) {
        Objects.requireNonNull(query, "query");
        if (query.pipelineId().isBlank()) {
            throw malformed("pipelineId is required");
        }
        if (!query.from().isBefore(query.to())) {
            throw malformed("from must be before to");
        }
        Duration span = Duration.between(query.from(), query.to());
        if (span.compareTo(MAX_RANGE) > 0) {
            throw malformed("the history range is at most 15 days");
        }
        if (query.limit() < 1 || query.limit() > PipelineHistoryQuery.MAX_LIMIT) {
            throw malformed("limit is between 1 and " + PipelineHistoryQuery.MAX_LIMIT);
        }
        TreeSet<String> tables = new TreeSet<>();
        for (String table : query.tables()) {
            if (table == null || table.isBlank()) {
                throw malformed("table selectors are not blank");
            }
            tables.add(table);
        }
        if (2 + tables.size() > MAX_SERIES) {
            throw budget("SERIES", MAX_SERIES);
        }
        List<String> selected = List.copyOf(tables);
        QueryBinding binding = new QueryBinding(query.pipelineId(), query.from(), query.to(),
                query.resolution(), query.limit(), selected);
        return new Normalized(binding, selected, query.cursor());
    }

    private Frozen freeze(Normalized normalized) {
        if (normalized.cursor() != null) {
            State state = cursors.read(normalized.cursor(), normalized.binding());
            return new Frozen(state.effectiveFrom(), state.effectiveTo(), state.retentionCutoff(),
                    state.afterKey(), state.resumeAt());
        }
        Instant now = clock.instant();
        Instant cutoff = now.minus(history.retention());
        Instant effectiveTo = min(normalized.binding().to(), now);
        Instant effectiveFrom = max(normalized.binding().from(), cutoff);
        if (effectiveFrom.isAfter(effectiveTo)) {
            effectiveFrom = effectiveTo;
        }
        return new Frozen(effectiveFrom, effectiveTo, cutoff, null, effectiveFrom);
    }

    private Visibility requirePipeline(String pipelineId) {
        boolean exists = artifacts.get(pipelineId)
                .map(artifact -> PIPELINE_KIND.equals(artifact.kind()))
                .orElse(false);
        if (!exists) {
            throw new TapstateException(LifecycleError.UNKNOWN_PIPELINE, Map.of("pipeline", pipelineId), null);
        }
        return artifacts.historyVisibilityOf(pipelineId)
                .orElseThrow(() -> new TapstateException(LifecycleError.UNKNOWN_PIPELINE,
                        Map.of("pipeline", pipelineId), null));
    }

    private static EffectiveHistoryResolution effectiveResolution(QueryBinding binding) {
        return switch (binding.resolution()) {
            case RAW -> EffectiveHistoryResolution.PT1M;
            case PT5M -> EffectiveHistoryResolution.PT5M;
            case PT30M -> EffectiveHistoryResolution.PT30M;
            case PT1H -> EffectiveHistoryResolution.PT1H;
            case PT3H -> EffectiveHistoryResolution.PT3H;
            case PT6H -> EffectiveHistoryResolution.PT6H;
            case AUTO -> auto(Duration.between(binding.from(), binding.to()));
        };
    }

    private static EffectiveHistoryResolution auto(Duration span) {
        if (span.compareTo(Duration.ofHours(1)) <= 0) {
            return EffectiveHistoryResolution.PT1M;
        }
        if (span.compareTo(Duration.ofHours(6)) <= 0) {
            return EffectiveHistoryResolution.PT5M;
        }
        if (span.compareTo(Duration.ofDays(1)) <= 0) {
            return EffectiveHistoryResolution.PT30M;
        }
        if (span.compareTo(Duration.ofDays(3)) <= 0) {
            return EffectiveHistoryResolution.PT1H;
        }
        if (span.compareTo(Duration.ofDays(7)) <= 0) {
            return EffectiveHistoryResolution.PT3H;
        }
        return EffectiveHistoryResolution.PT6H;
    }

    private Projection rawProjection(List<Entry> entries, Entry predecessor, StartReason initialReason,
            Instant from, Instant to, List<String> tables) {
        List<Emitted> points = new ArrayList<>();
        List<EmittedGap> gaps = new ArrayList<>();
        Entry previous = predecessor;
        int segment = 0;
        StartReason reason = initialReason;
        for (Entry current : entries) {
            Point point;
            StartReason boundary = previous == null ? null : boundary(previous.sample(), current.sample());
            if (boundary != null) {
                segment++;
                reason = boundary;
                if (boundary == StartReason.GAP) {
                    Instant gapStart = max(from, previous.sample().observedAt());
                    Instant gapEnd = min(to, current.sample().observedAt());
                    if (gapStart.isBefore(gapEnd)) {
                        gaps.add(new EmittedGap(segment, new Gap(gapStart, gapEnd, GapReason.SAMPLE_GAP)));
                    }
                }
                point = new Point(current.sample().observedAt(), current.sample().observedAt(),
                        null, null, lag(current.sample(), tables));
            } else if (previous == null) {
                point = new Point(current.sample().observedAt(), current.sample().observedAt(),
                        null, null, lag(current.sample(), tables));
            } else {
                Rate records = rawRate(previous.sample(), current.sample(), HistoryAggregator.RECORDS_OUT);
                Rate bytes = rawRate(previous.sample(), current.sample(), HistoryAggregator.BYTES_OUT);
                Instant start = records == null && bytes == null
                        ? current.sample().observedAt() : previous.sample().observedAt();
                point = new Point(start, current.sample().observedAt(), records, bytes,
                        lag(current.sample(), tables));
            }
            points.add(new Emitted(point, segment, reason, current.key(), current.sample().observedAt()));
            previous = current;
        }
        return new Projection(points, gaps);
    }

    private StartReason boundary(RateSample left, RateSample right) {
        Duration elapsed = Duration.between(left.observedAt(), right.observedAt());
        if (!elapsed.isNegative() && !elapsed.isZero()
                && elapsed.compareTo(sampleInterval.multipliedBy(2)) >= 0) {
            return StartReason.GAP;
        }
        if (!Objects.equals(left.countingSince(), right.countingSince())
                || decreased(left, right, HistoryAggregator.RECORDS_OUT)
                || decreased(left, right, HistoryAggregator.BYTES_OUT)) {
            return StartReason.COUNTER_RESET;
        }
        return null;
    }

    private static boolean decreased(RateSample left, RateSample right, String counter) {
        Long before = left.counters().get(counter);
        Long after = right.counters().get(counter);
        return before != null && after != null && after < before;
    }

    private static Rate rawRate(RateSample left, RateSample right, String counter) {
        Long before = left.counters().get(counter);
        Long after = right.counters().get(counter);
        Duration elapsed = Duration.between(left.observedAt(), right.observedAt());
        if (elapsed.isZero() || elapsed.isNegative() || before == null || after == null
                || left.countingSince() == null || !left.countingSince().equals(right.countingSince())
                || after < before) {
            return null;
        }
        BigDecimal delta = BigDecimal.valueOf(after - before);
        long nanos = Math.addExact(Math.multiplyExact(elapsed.getSeconds(), 1_000_000_000L), elapsed.getNano());
        BigDecimal rate = delta.multiply(NANOS_PER_SECOND)
                .divide(BigDecimal.valueOf(nanos), 9, RoundingMode.HALF_EVEN);
        return new Rate(delta, rate, rate);
    }

    private static List<Lag> lag(RateSample sample, List<String> tables) {
        List<Lag> out = new ArrayList<>();
        for (String table : tables) {
            Long value = sample.lag().get(table);
            if (value != null) {
                out.add(new Lag(table, sample.observedAt(), value, value));
            }
        }
        return List.copyOf(out);
    }

    private static List<Segment> segments(List<Emitted> emitted) {
        List<Segment> out = new ArrayList<>();
        int offset = 0;
        while (offset < emitted.size()) {
            int segment = emitted.get(offset).segment();
            int end = offset + 1;
            while (end < emitted.size() && emitted.get(end).segment() == segment) {
                end++;
            }
            List<Point> points = emitted.subList(offset, end).stream().map(Emitted::point).toList();
            out.add(new Segment(points.getFirst().intervalStart(), points.getLast().intervalEnd(),
                    emitted.get(offset).startReason(), points));
            offset = end;
        }
        return List.copyOf(out);
    }

    private static List<Gap> relatedGaps(List<Emitted> points, List<EmittedGap> gaps) {
        Set<Integer> segments = new LinkedHashSet<>();
        points.forEach(point -> segments.add(point.segment()));
        return gaps.stream().filter(gap -> segments.contains(gap.segment())).map(EmittedGap::gap).toList();
    }

    private static List<Unavailable> unavailable(List<Segment> segments, List<String> tables) {
        List<Point> points = segments.stream().flatMap(segment -> segment.points().stream()).toList();
        if (points.isEmpty()) {
            return List.of();
        }
        List<Unavailable> unavailable = new ArrayList<>();
        if (points.stream().noneMatch(point -> point.recordsOut() != null)) {
            unavailable.add(new Unavailable(HistoryAggregator.RECORDS_OUT, null));
        }
        if (points.stream().noneMatch(point -> point.bytesOut() != null)) {
            unavailable.add(new Unavailable(HistoryAggregator.BYTES_OUT, null));
        }
        for (String table : tables) {
            boolean present = points.stream().flatMap(point -> point.lag().stream())
                    .anyMatch(lag -> table.equals(lag.table()));
            if (!present) {
                unavailable.add(new Unavailable("lag", table));
            }
        }
        return List.copyOf(unavailable);
    }

    private PipelineMetricsHistory response(Normalized normalized, Frozen frozen,
            EffectiveHistoryResolution resolution, Status status, List<Segment> segments,
            List<Gap> gaps, List<Unavailable> unavailable, String nextCursor) {
        QueryBinding binding = normalized.binding();
        return new PipelineMetricsHistory(binding.pipelineId(), binding.from(), binding.to(),
                frozen.from(), frozen.to(), frozen.cutoff(), resolution, status, Consistency.EVENTUAL,
                segments, gaps, unavailable, nextCursor);
    }

    private static QueryRun measured(PipelineMetricsHistory history, Cost cost) {
        int outputPoints = history.segments().stream().mapToInt(segment -> segment.points().size()).sum();
        return new QueryRun(history, new QueryCost(cost.storeReads, cost.rawDocumentsScanned,
                outputPoints, cost.peakRawEntriesHeld));
    }

    private void requireScanBudget(Cost cost) {
        if (cost.rawDocumentsScanned > rawScanBudget) {
            throw budget("RAW_SCAN", rawScanBudget);
        }
    }

    private static TapstateException malformed(String reason) {
        return new TapstateException(ControlError.MALFORMED_REQUEST, Map.of("reason", reason), null);
    }

    private static TapstateException budget(String name, int limit) {
        return new TapstateException(MonitorError.QUERY_BUDGET_EXCEEDED,
                Map.of("operation", "pipeline.metrics.history", "budget", name, "limit", limit), null);
    }

    private static Instant min(Instant left, Instant right) {
        return left.isBefore(right) ? left : right;
    }

    private static Instant max(Instant left, Instant right) {
        return left.isAfter(right) ? left : right;
    }

    private record Normalized(QueryBinding binding, List<String> tables, String cursor) {
    }

    private record Frozen(Instant from, Instant to, Instant cutoff, Key afterKey, Instant resumeAt) {
    }

    private record Projection(List<Emitted> points, List<EmittedGap> gaps) {
    }

    private static final class Cost {
        private int storeReads;
        private int rawDocumentsScanned;
        private int peakRawEntriesHeld;

        void page(Page page, int boundaryHeld) {
            storeReads++;
            rawDocumentsScanned += page.entries().size() + (page.hasMore() ? 1 : 0);
            peakRawEntriesHeld = Math.max(peakRawEntriesHeld, page.entries().size() + boundaryHeld);
        }
    }
}
