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
import io.tapstate.spi.store.HistoryRollupStore;
import io.tapstate.spi.store.HistoryRollupStore.Bucket;
import io.tapstate.spi.store.HistoryRollupStore.Fragment;
import io.tapstate.spi.store.HistoryRollupStore.Resolution;
import io.tapstate.spi.store.HistoryRollupStore.Scope;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Consumer;

/** The bounded, store-backed query service behind {@code pipeline.metrics.history}. */
public final class PipelineHistoryQueryService {

    public static final int DEFAULT_RAW_BATCH_SIZE = RateHistoryStore.MAX_PAGE_SIZE;
    public static final int DEFAULT_RAW_SCAN_BUDGET = 25_000;
    public static final int MAX_SERIES = 22;
    public static final Duration MAX_RANGE = Duration.ofDays(15);
    private static final int ROLLUP_READ_BATCH = 64;
    private static final int MAX_ROLLUP_READS = 128;
    private static final int MAX_RAW_FALLBACK_BUCKETS = 64;
    private static final int MAX_CACHED_ITEMS = 16_384;
    private static final int MAX_REFRESH_OFFERS_PER_QUERY = 64;

    private static final String PIPELINE_KIND = "pipeline";
    private static final BigDecimal NANOS_PER_SECOND = BigDecimal.valueOf(1_000_000_000L);

    private final ArtifactQueryService artifacts;
    private final RateHistoryStore history;
    private final HistoryRollupStore rollups;
    private final Consumer<HistoryRollupStore.Key> refreshHint;
    private final Consumer<RollupFallback> fallbackObserver;
    private final Duration sampleInterval;
    private final Clock clock;
    private final HistoryCursorCodec cursors;
    private final int rawBatchSize;
    private final int rawScanBudget;

    public PipelineHistoryQueryService(ArtifactQueryService artifacts, RateHistoryStore history,
            Duration sampleInterval, Clock clock, HistoryCursorCodec cursors) {
        this(artifacts, history, null, sampleInterval, clock, cursors,
                DEFAULT_RAW_BATCH_SIZE, DEFAULT_RAW_SCAN_BUDGET);
    }

    public PipelineHistoryQueryService(ArtifactQueryService artifacts, RateHistoryStore history,
            HistoryRollupStore rollups, Duration sampleInterval, Clock clock, HistoryCursorCodec cursors) {
        this(artifacts, history, rollups, ignored -> { }, sampleInterval, clock, cursors,
                DEFAULT_RAW_BATCH_SIZE, DEFAULT_RAW_SCAN_BUDGET);
    }

    public PipelineHistoryQueryService(ArtifactQueryService artifacts, RateHistoryStore history,
            HistoryRollupStore rollups, Consumer<HistoryRollupStore.Key> refreshHint,
            Duration sampleInterval, Clock clock, HistoryCursorCodec cursors) {
        this(artifacts, history, rollups, refreshHint, ignored -> { }, sampleInterval, clock, cursors,
                DEFAULT_RAW_BATCH_SIZE, DEFAULT_RAW_SCAN_BUDGET);
    }

    public PipelineHistoryQueryService(ArtifactQueryService artifacts, RateHistoryStore history,
            HistoryRollupStore rollups, Consumer<HistoryRollupStore.Key> refreshHint,
            Consumer<RollupFallback> fallbackObserver,
            Duration sampleInterval, Clock clock, HistoryCursorCodec cursors) {
        this(artifacts, history, rollups, refreshHint, fallbackObserver, sampleInterval, clock, cursors,
                DEFAULT_RAW_BATCH_SIZE, DEFAULT_RAW_SCAN_BUDGET);
    }

    PipelineHistoryQueryService(ArtifactQueryService artifacts, RateHistoryStore history,
            Duration sampleInterval, Clock clock, HistoryCursorCodec cursors,
            int rawBatchSize, int rawScanBudget) {
        this(artifacts, history, null, sampleInterval, clock, cursors, rawBatchSize, rawScanBudget);
    }

    PipelineHistoryQueryService(ArtifactQueryService artifacts, RateHistoryStore history,
            HistoryRollupStore rollups, Duration sampleInterval, Clock clock, HistoryCursorCodec cursors,
            int rawBatchSize, int rawScanBudget) {
        this(artifacts, history, rollups, ignored -> { }, sampleInterval, clock, cursors,
                rawBatchSize, rawScanBudget);
    }

    PipelineHistoryQueryService(ArtifactQueryService artifacts, RateHistoryStore history,
            HistoryRollupStore rollups, Consumer<HistoryRollupStore.Key> refreshHint,
            Duration sampleInterval, Clock clock, HistoryCursorCodec cursors,
            int rawBatchSize, int rawScanBudget) {
        this(artifacts, history, rollups, refreshHint, ignored -> { }, sampleInterval, clock, cursors,
                rawBatchSize, rawScanBudget);
    }

    PipelineHistoryQueryService(ArtifactQueryService artifacts, RateHistoryStore history,
            HistoryRollupStore rollups, Consumer<HistoryRollupStore.Key> refreshHint,
            Consumer<RollupFallback> fallbackObserver,
            Duration sampleInterval, Clock clock, HistoryCursorCodec cursors,
            int rawBatchSize, int rawScanBudget) {
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.history = Objects.requireNonNull(history, "history");
        this.rollups = rollups;
        this.refreshHint = Objects.requireNonNull(refreshHint, "refreshHint");
        this.fallbackObserver = Objects.requireNonNull(fallbackObserver, "fallbackObserver");
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

    /** One completed query that used raw history in place of a selected rollup resolution. */
    public record RollupFallback(Resolution resolution, int downDrilledBuckets, boolean fullRaw) {
        public RollupFallback {
            Objects.requireNonNull(resolution, "resolution");
            if (downDrilledBuckets < 0 || fullRaw && downDrilledBuckets != 0
                    || !fullRaw && downDrilledBuckets == 0) {
                throw new IllegalArgumentException("a rollup fallback is either full raw or counted raw buckets");
            }
        }
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
        QueryRun cached = effective.raw() ? null : cachedAggregate(normalized, frozen, effective, cost);
        QueryRun run;
        if (cached != null) {
            run = cached;
        } else {
            Boundary boundary = boundaryBefore(normalized, frozen, cost);
            run = effective.raw()
                    ? raw(normalized, frozen, effective, boundary, cost)
                    : aggregate(normalized, frozen, effective, boundary, cost);
        }
        if (!visibility.equals(requirePipeline(normalized.binding().pipelineId()))) {
            throw new TapstateException(MonitorError.INVALID_CURSOR,
                    Map.of("operation", "pipeline.metrics.history", "reason", "QUERY_MISMATCH"), null);
        }
        if (rollups != null && !effective.raw() && (cached == null || cost.rollupRawBuckets > 0)) {
            try {
                fallbackObserver.accept(new RollupFallback(Resolution.valueOf(effective.name()),
                        cached == null ? 0 : cost.rollupRawBuckets, cached == null));
            } catch (RuntimeException unavailable) {
                // Process health is best-effort and cannot fail an otherwise completed history read.
            }
        }
        return run;
    }

    private QueryRun cachedAggregate(Normalized normalized, Frozen frozen,
            EffectiveHistoryResolution effective, Cost cost) {
        if (rollups == null || (frozen.afterKey() != null && frozen.cachePosition() == null)) {
            return null;
        }
        QueryBinding binding = normalized.binding();
        Visibility visibility = binding.visibility();
        if (visibility.incarnationId() != null && visibility.includeLegacy()) {
            return null;
        }
        Resolution resolution = Resolution.valueOf(effective.name());
        Scope scope = visibility.incarnationId() == null
                ? Scope.legacy() : Scope.incarnation(visibility.incarnationId());
        Duration width = resolution.duration();
        Instant first = floor(frozen.from(), width);
        Instant end = ceil(frozen.to(), width);
        Map<Instant, Bucket> cached = new HashMap<>();
        int cachedItems = 0;
        Instant batchFrom = first;
        while (batchFrom.isBefore(end)) {
            Instant batchTo = min(end, batchFrom.plus(width.multipliedBy(ROLLUP_READ_BATCH)));
            if (++cost.storeReads > MAX_ROLLUP_READS) {
                return null;
            }
            List<Bucket> page;
            try {
                page = rollups.readRange(binding.pipelineId(), scope, resolution,
                        batchFrom, batchTo, ROLLUP_READ_BATCH);
            } catch (RuntimeException unavailable) {
                return null;
            }
            for (Bucket bucket : page) {
                cachedItems += 1 + bucket.fragments().size() + bucket.gaps().size();
                if (cachedItems > MAX_CACHED_ITEMS) {
                    return null;
                }
                if (!bucket.key().pipelineId().equals(binding.pipelineId())
                        || !bucket.key().scope().equals(scope)
                        || bucket.key().resolution() != resolution
                        || bucket.key().bucketStart().isBefore(batchFrom)
                        || !bucket.key().bucketStart().isBefore(batchTo)
                        || cached.putIfAbsent(bucket.key().bucketStart(), bucket) != null) {
                    return null;
                }
            }
            batchFrom = batchTo;
        }
        if (cached.isEmpty()) {
            int hinted = 0;
            for (Instant at = first; at.isBefore(end) && hinted < MAX_REFRESH_OFFERS_PER_QUERY;
                    at = at.plus(width), hinted++) {
                offerRefresh(new HistoryRollupStore.Key(binding.pipelineId(), scope, resolution, at));
            }
            return null;
        }

        List<BucketSlice> slices = new ArrayList<>();
        int fallbackBuckets = 0;
        int hinted = 0;
        Instant checkedAt = clock.instant();
        HistoryCursorCodec.CachePosition resume = frozen.cachePosition();
        for (Instant at = first; at.isBefore(end); at = at.plus(width)) {
            Instant sliceFrom = max(at, frozen.from());
            Instant sliceTo = min(at.plus(width), frozen.to());
            Bucket bucket = cached.get(at);
            if (hinted < MAX_REFRESH_OFFERS_PER_QUERY
                    && (bucket == null || !bucket.validUntil().isAfter(checkedAt)
                            || bucket.inWindowSamples() < 0 || ambiguousCounterBoundary(bucket))) {
                offerRefresh(new HistoryRollupStore.Key(binding.pipelineId(), scope, resolution, at));
                hinted++;
            }
            boolean full = at.equals(sliceFrom) && at.plus(width).equals(sliceTo);
            if (!full || bucket == null || !bucket.usableAt(checkedAt)
                    || bucket.inWindowSamples() < 0
                    || bucket.inWindowSamples() > 0 && bucket.fragments().isEmpty()
                    || ambiguousCounterBoundary(bucket)
                    || resume != null && resume.isRawFallback()
                            && resume.bucketStart().equals(at)) {
                bucket = null;
                fallbackBuckets++;
            }
            slices.add(new BucketSlice(at, sliceFrom, sliceTo, bucket));
        }
        if (fallbackBuckets > MAX_RAW_FALLBACK_BUCKETS) {
            return null;
        }
        if (resume != null) {
            if (resume.bucketStart().isBefore(first) || !resume.bucketStart().isBefore(end)) {
                return null;
            }
            if (!resume.isRawFallback()) {
                Bucket anchor = cached.get(resume.bucketStart());
                if (anchor == null || !anchor.usableAt(checkedAt) || ambiguousCounterBoundary(anchor)
                        || !anchor.computedAt().equals(resume.computedAt())
                        || resume.fragmentIndex() >= anchor.fragments().size()
                        || !Objects.equals(anchor.fragments().get(resume.fragmentIndex()).resumeAfter(),
                                frozen.afterKey())
                        || !Objects.equals(anchor.fragments().get(resume.fragmentIndex()).resumeAt(),
                                frozen.resumeAt())) {
                    return null;
                }
            }
        }

        boolean retainedSample = slices.stream().anyMatch(slice ->
                slice.bucket() != null && slice.bucket().inWindowSamples() > 0);
        List<Emitted> output = new ArrayList<>();
        List<Gap> observedGaps = new ArrayList<>();
        List<HistoryCursorCodec.CachePosition> positions = new ArrayList<>();
        boolean passedAnchor = resume == null || resume.isRawFallback();
        int globalSegment = 0;
        int precedingLocalSegment = -1;
        Instant precedingBucket = null;
        boolean hasPriorPoint = resume != null;
        if (resume != null) {
            precedingBucket = resume.bucketStart();
            precedingLocalSegment = resume.isRawFallback() ? 0 : cached.get(resume.bucketStart()).fragments()
                    .get(resume.fragmentIndex()).segment();
        }
        for (BucketSlice slice : slices) {
            if (resume != null && slice.bucketStart().isBefore(resume.bucketStart())) {
                continue;
            }
            SliceProjection projection = slice.bucket() == null
                    ? rawSlice(normalized, frozen, effective, slice, cost)
                    : cachedSlice(slice.bucket(), normalized.tables());
            retainedSample |= projection.inWindowSamples() > 0;
            for (HistoryRollupStore.Gap gap : projection.gaps()) {
                Instant start = max(frozen.resumeAt(), gap.intervalStart());
                if (start.isBefore(gap.intervalEnd())) {
                    observedGaps.add(new Gap(start, gap.intervalEnd(),
                            GapReason.valueOf(gap.reason().name())));
                }
            }
            if (output.size() > binding.limit()) {
                if (retainedSample) {
                    break;
                }
                continue;
            }
            for (SourcePoint source : projection.points()) {
                if (!passedAnchor) {
                    if (resume.isRawFallback()
                            ? source.position() == null
                                    && Objects.equals(source.resumeAfter(), frozen.afterKey())
                                    && Objects.equals(source.resumeAt(), frozen.resumeAt())
                            : source.position() != null && source.position().equals(resume)) {
                        passedAnchor = true;
                    }
                    continue;
                }
                boolean boundary = false;
                if (hasPriorPoint) {
                    boolean sameBucket = slice.bucketStart().equals(precedingBucket);
                    boundary = sameBucket
                            ? source.localSegment() != precedingLocalSegment
                            : source.localSegment() > 0 || source.startReason() == StartReason.GAP
                                    || source.startReason() == StartReason.COUNTER_RESET;
                    if (boundary) {
                        globalSegment++;
                    }
                } else {
                    globalSegment = source.localSegment();
                }
                StartReason reason = hasPriorPoint && !boundary
                        ? StartReason.CONTINUATION
                        : !hasPriorPoint && source.localSegment() == 0
                                && source.startReason() == StartReason.CONTINUATION
                                ? StartReason.WINDOW_START : source.startReason();
                Key anchor = source.resumeAfter() == null
                        ? new Key(source.point().intervalEnd(), "cache-unanchored")
                        : source.resumeAfter();
                output.add(new Emitted(source.point(), globalSegment, reason,
                        anchor, source.resumeAt() == null ? source.point().intervalEnd() : source.resumeAt()));
                positions.add(source.position() == null
                        ? HistoryCursorCodec.CachePosition.rawFallback(slice.bucketStart())
                        : source.position());
                hasPriorPoint = true;
                precedingBucket = slice.bucketStart();
                precedingLocalSegment = source.localSegment();
                if (output.size() > binding.limit()) {
                    break;
                }
            }
            if (output.size() > binding.limit() && retainedSample) {
                break;
            }
        }
        if (!passedAnchor) {
            return null;
        }
        if (cached.values().stream().anyMatch(bucket -> bucket.usableAt(checkedAt)
                && !clock.instant().isBefore(bucket.validUntil()))) {
            return null;
        }
        if (!retainedSample && resume == null) {
            return measured(response(normalized, frozen, effective,
                    Status.NO_RETAINED_SAMPLES, List.of(), List.of(), List.of(), null), cost);
        }
        boolean hasMore = output.size() > binding.limit();
        List<Emitted> emitted = hasMore ? output.subList(0, binding.limit()) : output;
        String next = null;
        if (hasMore && !emitted.isEmpty()) {
            Emitted last = emitted.getLast();
            HistoryCursorCodec.CachePosition position = positions.get(binding.limit() - 1);
            if (position.isRawFallback()) {
                next = cursors.issue(binding, frozen.from(), frozen.to(), frozen.cutoff(),
                        last.resumeAfter(), last.resumeAt(), position);
            } else {
                Fragment fragment = cached.get(position.bucketStart()).fragments().get(position.fragmentIndex());
                if (fragment.resumeAfter() == null || fragment.resumeAt() == null) {
                    return null;
                }
                next = cursors.issue(binding, frozen.from(), frozen.to(), frozen.cutoff(),
                        last.resumeAfter(), last.resumeAt(), position);
            }
        }
        List<Segment> segments = segments(emitted);
        List<Gap> related = cachedGapsForPage(segments, observedGaps);
        return measured(response(normalized, frozen, effective, Status.OK,
                segments, related, unavailable(segments, normalized.tables()), next), cost);
    }

    private void offerRefresh(HistoryRollupStore.Key key) {
        try {
            refreshHint.accept(key);
        } catch (RuntimeException unavailable) {
            // A hint is disposable; the same request still uses bounded raw history as its fallback.
        }
    }

    private static List<Gap> cachedGapsForPage(List<Segment> segments, List<Gap> observed) {
        if (observed.isEmpty()) {
            return List.of();
        }
        List<Gap> ordered = observed.stream()
                .sorted(java.util.Comparator.comparing(Gap::intervalStart)
                        .thenComparing(Gap::intervalEnd))
                .toList();
        List<Gap> merged = new ArrayList<>();
        for (Gap gap : ordered) {
            if (!merged.isEmpty()) {
                Gap last = merged.getLast();
                if (last.reason() == gap.reason()
                        && !last.intervalEnd().isBefore(gap.intervalStart())) {
                    merged.set(merged.size() - 1, new Gap(last.intervalStart(),
                            max(last.intervalEnd(), gap.intervalEnd()), last.reason()));
                    continue;
                }
            }
            merged.add(gap);
        }
        return merged.stream().filter(gap -> segments.stream()
                .anyMatch(segment -> segment.startReason() == StartReason.GAP
                        && !segment.intervalStart().isBefore(gap.intervalEnd())))
                .toList();
    }

    /** Explicit uncovered counter intervals cannot qualify an unknown continuation boundary. */
    private static boolean ambiguousCounterBoundary(Bucket bucket) {
        for (Fragment fragment : bucket.fragments()) {
            long span = Duration.between(fragment.intervalStart(), fragment.intervalEnd()).toNanos();
            if (fragment.startReason() == HistoryRollupStore.StartReason.CONTINUATION
                            && fragment.recordsOut() == null && fragment.bytesOut() == null
                    || fragment.recordsOutStats() != null
                            && fragment.recordsOutStats().coveredNanos() != span
                    || fragment.bytesOutStats() != null
                            && fragment.bytesOutStats().coveredNanos() != span) {
                return true;
            }
        }
        return false;
    }

    private SliceProjection cachedSlice(Bucket bucket, List<String> tables) {
        List<SourcePoint> points = new ArrayList<>();
        for (int index = 0; index < bucket.fragments().size(); index++) {
            Fragment fragment = bucket.fragments().get(index);
            List<Lag> selectedLag = fragment.lag().stream()
                    .filter(lag -> tables.contains(lag.table()))
                    .map(lag -> new Lag(lag.table(), lag.observedAt(), lag.last(), lag.max()))
                    .toList();
            Point point = new Point(fragment.intervalStart(), fragment.intervalEnd(),
                    cachedRate(fragment.recordsOut()), cachedRate(fragment.bytesOut()), selectedLag);
            HistoryCursorCodec.CachePosition position = new HistoryCursorCodec.CachePosition(
                    bucket.key().bucketStart(), index, bucket.computedAt());
            points.add(new SourcePoint(point, fragment.segment(),
                    StartReason.valueOf(fragment.startReason().name()),
                    fragment.resumeAfter(), fragment.resumeAt(), position));
        }
        return new SliceProjection(bucket.inWindowSamples(), points, bucket.gaps());
    }

    private static Rate cachedRate(HistoryRollupStore.Rate value) {
        return value == null ? null : new Rate(value.delta(), value.averageRate(), value.maxRate());
    }

    private SliceProjection rawSlice(Normalized normalized, Frozen frozen,
            EffectiveHistoryResolution effective, BucketSlice slice, Cost cost) {
        cost.rollupRawBuckets++;
        QueryBinding binding = normalized.binding();
        HistoryCursorCodec.CachePosition position = frozen.cachePosition();
        boolean resuming = position != null && position.isRawFallback()
                && position.bucketStart().equals(slice.bucketStart());
        Entry predecessor;
        HistoryCounterCheckpoint checkpoint;
        if (resuming) {
            Boundary boundary = boundaryBefore(normalized, frozen, cost);
            predecessor = boundary.predecessor();
            checkpoint = boundary.checkpoint();
        } else {
            cost.storeReads++;
            Optional<Entry> boundary = history.predecessorVisible(binding.pipelineId(), binding.visibility(),
                    slice.from());
            boundary.ifPresent(ignored -> cost.rawDocumentsScanned++);
            predecessor = retained(boundary, frozen).orElse(null);
            requireScanBudget(cost);
            checkpoint = checkpointBefore(binding, frozen, predecessor, cost);
        }
        HistoryAggregator aggregator = new HistoryAggregator(slice.from(), slice.to(),
                resuming ? frozen.resumeAt() : slice.from(),
                effective.duration(), sampleInterval, normalized.tables(),
                !resuming && slice.from().equals(frozen.from())
                        ? StartReason.WINDOW_START : StartReason.CONTINUATION,
                binding.limit() + 1);
        aggregator.begin(predecessor, checkpoint);
        Key after = resuming ? frozen.afterKey() : null;
        int count = 0;
        boolean storeHasMore;
        do {
            Page page = history.readPageVisible(binding.pipelineId(), binding.visibility(),
                    slice.from(), slice.to(), after, rawBatchSize);
            cost.page(page, predecessor == null && count == 0 ? 0 : 1);
            requireScanBudget(cost);
            for (Entry entry : page.entries()) {
                aggregator.add(entry);
                count++;
                after = entry.key();
                if (aggregator.full()) {
                    break;
                }
            }
            storeHasMore = page.hasMore();
            if (page.entries().isEmpty() || aggregator.full()) {
                break;
            }
        } while (storeHasMore);
        Entry successor = null;
        if ((count > 0 || resuming) && !aggregator.full() && !storeHasMore) {
            cost.storeReads++;
            successor = history.successorVisible(binding.pipelineId(), binding.visibility(),
                    slice.to()).orElse(null);
            if (successor != null) {
                cost.rawDocumentsScanned++;
                requireScanBudget(cost);
            }
        }
        HistoryAggregator.Projection projection = aggregator.finish(successor);
        List<SourcePoint> points = projection.points().stream()
                .map(emitted -> new SourcePoint(emitted.point(), emitted.segment(),
                        emitted.startReason(), emitted.resumeAfter(), emitted.resumeAt(), null))
                .toList();
        List<HistoryRollupStore.Gap> gaps = projection.gaps().stream()
                .map(gap -> new HistoryRollupStore.Gap(gap.segment(), gap.gap().intervalStart(),
                        gap.gap().intervalEnd(),
                        HistoryRollupStore.GapReason.valueOf(gap.gap().reason().name())))
                .toList();
        return new SliceProjection(count, points, gaps);
    }

    private static Instant floor(Instant at, Duration width) {
        long seconds = width.toSeconds();
        return Instant.ofEpochSecond(Math.floorDiv(at.getEpochSecond(), seconds) * seconds);
    }

    private static Instant ceil(Instant at, Duration width) {
        Instant floor = floor(at, width);
        return floor.equals(at) ? floor : floor.plus(width);
    }

    private QueryRun raw(Normalized normalized, Frozen frozen, EffectiveHistoryResolution effective,
            Boundary boundary, Cost cost) {
        QueryBinding binding = normalized.binding();
        Entry predecessor = boundary.predecessor();
        HistoryCounterCheckpoint checkpoint = boundary.checkpoint();
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
                frozen.from(), frozen.to(), normalized.tables(), checkpoint);
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
            Boundary boundary, Cost cost) {
        QueryBinding binding = normalized.binding();
        Entry predecessor = boundary.predecessor();
        HistoryAggregator aggregator = new HistoryAggregator(frozen.from(), frozen.to(), frozen.resumeAt(),
                effective.duration(), sampleInterval, normalized.tables(),
                frozen.afterKey() == null ? StartReason.WINDOW_START : StartReason.CONTINUATION,
                binding.limit() + 1);
        aggregator.begin(predecessor, boundary.checkpoint());

        Key after = frozen.afterKey();
        boolean storeHasMore = true;
        int inWindowSamples = 0;
        while (storeHasMore && !aggregator.full()) {
            Page page = history.readPageVisible(binding.pipelineId(), binding.visibility(),
                    frozen.from(), frozen.to(), after, rawBatchSize);
            cost.page(page, predecessor == null && inWindowSamples == 0 ? 0 : 1);
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

    private HistoryCounterCheckpoint checkpointBefore(QueryBinding binding, Frozen frozen,
            Entry predecessor, Cost cost) {
        if (predecessor == null) { return new HistoryCounterCheckpoint(binding.pipelineId()); }
        return HistoryCounterCheckpoint.replay(history, binding.visibility(), frozen.cutoff(), predecessor,
                sampleInterval, rawBatchSize, page -> {
                    cost.page(page, 1);
                    requireScanBudget(cost);
                });
    }

    private Boundary boundaryBefore(Normalized normalized, Frozen frozen, Cost cost) {
        QueryBinding binding = normalized.binding();
        if (frozen.afterKey() != null) {
            cost.storeReads++;
            Optional<Entry> exact = history.readVisible(binding.pipelineId(), binding.visibility(),
                    frozen.afterKey());
            if (exact.isPresent()) {
                cost.rawDocumentsScanned++;
                cost.peakRawEntriesHeld = 1;
                requireScanBudget(cost);
                Entry predecessor = exact.orElseThrow();
                return new Boundary(predecessor, checkpointBefore(binding, frozen, predecessor, cost));
            }
            return recoverBeforeExpiredAnchor(binding, frozen, cost);
        }
        cost.storeReads++;
        Optional<Entry> read = history.predecessorVisible(binding.pipelineId(), binding.visibility(),
                frozen.from());
        read.ifPresent(ignored -> cost.rawDocumentsScanned++);
        Optional<Entry> predecessor = retained(read, frozen);
        cost.peakRawEntriesHeld = read.isPresent() ? 1 : 0;
        requireScanBudget(cost);
        Entry entry = predecessor.orElse(null);
        return new Boundary(entry, checkpointBefore(binding, frozen, entry, cost));
    }

    private Boundary recoverBeforeExpiredAnchor(QueryBinding binding, Frozen frozen, Cost cost) {
        HistoryCounterCheckpoint checkpoint = new HistoryCounterCheckpoint(binding.pipelineId());
        Entry predecessor = null;
        Key after = null;
        Instant end = frozen.afterKey().observedAt().plusMillis(1);
        while (frozen.cutoff().isBefore(end)) {
            Page page = history.readPageVisible(binding.pipelineId(), binding.visibility(),
                    frozen.cutoff(), end, after, rawBatchSize);
            cost.page(page, predecessor == null ? 0 : 1);
            requireScanBudget(cost);
            for (Entry entry : page.entries()) {
                int byTime = entry.key().observedAt().compareTo(frozen.afterKey().observedAt());
                if (byTime > 0 || byTime == 0 && entry.key().internalKey()
                        .compareTo(frozen.afterKey().internalKey()) > 0) {
                    return new Boundary(predecessor, checkpoint);
                }
                checkpoint.observe(entry, sampleInterval);
                predecessor = entry;
            }
            if (!page.hasMore()) { break; }
            after = page.lastKey().orElseThrow();
        }
        return new Boundary(predecessor, checkpoint);
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
                    state.afterKey(), state.resumeAt(), state.cachePosition());
        }
        Instant now = clock.instant();
        Instant cutoff = now.minus(history.retention());
        Instant effectiveTo = min(normalized.binding().to(), now);
        Instant effectiveFrom = max(normalized.binding().from(), cutoff);
        if (effectiveFrom.isAfter(effectiveTo)) {
            effectiveFrom = effectiveTo;
        }
        return new Frozen(effectiveFrom, effectiveTo, cutoff, null, effectiveFrom, null);
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
            Instant from, Instant to, List<String> tables, HistoryCounterCheckpoint checkpoint) {
        List<Emitted> points = new ArrayList<>();
        List<EmittedGap> gaps = new ArrayList<>();
        Entry previous = predecessor;
        int segment = 0;
        StartReason reason = initialReason;
        for (Entry current : entries) {
            Point point;
            boolean reset = checkpoint.observe(current, sampleInterval);
            StartReason boundary = previous == null ? null : boundary(previous, current, reset);
            if (boundary != null) {
                segment++;
                reason = boundary;
                if (boundary == StartReason.GAP) {
                    Instant gapStart = max(from, gapStart(previous, current));
                    Instant gapEnd = min(to, current.sample().observedAt());
                    if (gapStart.isBefore(gapEnd)) {
                        gaps.add(new EmittedGap(segment, new Gap(gapStart, gapEnd, GapReason.SAMPLE_GAP)));
                    }
                }
                point = new Point(current.sample().observedAt(), current.sample().observedAt(),
                        null, null, lag(current.sample(), tables));
            } else if (previous == null) {
                if (current.gapFrom() != null) {
                    reason = StartReason.GAP;
                    Instant gapStart = max(from, current.gapFrom());
                    Instant gapEnd = min(to, current.sample().observedAt());
                    if (gapStart.isBefore(gapEnd)) {
                        gaps.add(new EmittedGap(segment, new Gap(gapStart, gapEnd, GapReason.SAMPLE_GAP)));
                    }
                }
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

    private StartReason boundary(Entry previous, Entry current, boolean reset) {
        RateSample left = previous.sample();
        RateSample right = current.sample();
        Duration elapsed = Duration.between(left.observedAt(), right.observedAt());
        if (current.gapFrom() != null) {
            return StartReason.GAP;
        }
        if (!elapsed.isNegative() && !elapsed.isZero()
                && elapsed.compareTo(sampleInterval.multipliedBy(2)) >= 0) {
            return StartReason.GAP;
        }
        if (reset
                || decreased(left, right, HistoryAggregator.RECORDS_OUT)
                || decreased(left, right, HistoryAggregator.BYTES_OUT)) {
            return StartReason.COUNTER_RESET;
        }
        if (previous.scope().isPresent() && current.scope().isPresent()
                && !previous.scope().equals(current.scope())) {
            return StartReason.CONTINUATION;
        }
        return null;
    }

    private static Instant gapStart(Entry previous, Entry current) {
        if (current.gapFrom() != null && !previous.scope().equals(current.scope())) {
            return current.gapFrom();
        }
        return previous.sample().observedAt();
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

    private record Boundary(Entry predecessor, HistoryCounterCheckpoint checkpoint) {
    }

    private record Frozen(Instant from, Instant to, Instant cutoff, Key afterKey, Instant resumeAt,
            HistoryCursorCodec.CachePosition cachePosition) {
    }

    private record BucketSlice(Instant bucketStart, Instant from, Instant to, Bucket bucket) {
    }

    private record SourcePoint(Point point, int localSegment, StartReason startReason,
            Key resumeAfter, Instant resumeAt, HistoryCursorCodec.CachePosition position) {
    }

    private record SliceProjection(int inWindowSamples, List<SourcePoint> points,
            List<HistoryRollupStore.Gap> gaps) {
    }

    private record Projection(List<Emitted> points, List<EmittedGap> gaps) {
    }

    private static final class Cost {
        private int storeReads;
        private int rawDocumentsScanned;
        private int peakRawEntriesHeld;
        private int rollupRawBuckets;

        void page(Page page, int boundaryHeld) {
            storeReads++;
            rawDocumentsScanned += page.entries().size() + (page.hasMore() ? 1 : 0);
            peakRawEntriesHeld = Math.max(peakRawEntriesHeld, page.entries().size() + boundaryHeld);
        }
    }
}
