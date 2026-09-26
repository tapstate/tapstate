package io.tapstate.app;

import io.tapstate.control.core.HistoryAggregator;
import io.tapstate.control.core.HistoryAggregator.Emitted;
import io.tapstate.control.core.HistoryAggregator.EmittedGap;
import io.tapstate.control.core.PipelineMetricsHistory;
import io.tapstate.spi.store.HistoryRollupStore;
import io.tapstate.spi.store.HistoryRollupStore.Bucket;
import io.tapstate.spi.store.HistoryRollupStore.Fragment;
import io.tapstate.spi.store.HistoryRollupStore.Gap;
import io.tapstate.spi.store.HistoryRollupStore.Key;
import io.tapstate.spi.store.HistoryRollupStore.Lag;
import io.tapstate.spi.store.HistoryRollupStore.Rate;
import io.tapstate.spi.store.HistoryRollupStore.Resolution;
import io.tapstate.spi.store.HistoryRollupStore.Scope;
import io.tapstate.spi.store.RateHistoryStore;
import io.tapstate.spi.store.RateHistoryStore.Entry;
import io.tapstate.spi.store.RateHistoryStore.Page;
import io.tapstate.spi.store.RateHistoryStore.Visibility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Builds disposable fixed-resolution buckets on one low-priority, fixed-batch worker. Each scope and
 * resolution has its own contiguous position; after a restart persisted keys recover that position.
 * Expired buckets are refreshed only on bounded hints, leaving reads to descend when a hint is lost.
 */
final class HistoryRollupWorker implements AutoCloseable {

    static final int DEFAULT_BATCH_SIZE = 16;
    static final Duration DEFAULT_INTERVAL = Duration.ofSeconds(5);
    static final int MAX_REFRESH_HINTS = 64;
    static final int MAX_RAW_ENTRIES_PER_BUCKET = 4096;
    private static final Duration EMPTY_POLL_INTERVAL = Duration.ofMinutes(1);

    private static final Logger LOG = LoggerFactory.getLogger(HistoryRollupWorker.class);
    private static final List<Resolution> LEVELS = List.of(Resolution.values());
    private static final BigDecimal NANOS_PER_SECOND = BigDecimal.valueOf(1_000_000_000L);
    private static final int INTERNAL_SCALE = 30;

    private enum BuildResult { WRITTEN, NO_INPUT, OWNER_LOST }

    record Work(String pipelineId, Scope scope) {
        Work {
            Objects.requireNonNull(pipelineId, "pipelineId");
            Objects.requireNonNull(scope, "scope");
            if (pipelineId.isBlank()) {
                throw new IllegalArgumentException("a rollup pipeline id is not blank");
            }
        }
    }

    private record LevelWork(Work work, Resolution resolution) { }

    private final RateHistoryStore raw;
    private final HistoryRollupStore rollups;
    private final Clock clock;
    private final Duration sampleInterval;
    private final int batchSize;
    private final Supplier<List<Work>> current;
    private final Predicate<Work> permitted;
    private final ScheduledExecutorService scheduler;
    private final Map<LevelWork, Instant> nextBucket = new HashMap<>();
    private final Map<LevelWork, Instant> emptyRetryAt = new HashMap<>();
    private final LinkedHashSet<Key> refreshHints = new LinkedHashSet<>();
    private int nextWorkIndex;

    HistoryRollupWorker(RateHistoryStore raw, HistoryRollupStore rollups, Clock clock,
            Duration sampleInterval, int batchSize, Duration interval,
            Supplier<List<Work>> current, Predicate<Work> permitted, boolean schedule) {
        this.raw = Objects.requireNonNull(raw, "raw");
        this.rollups = Objects.requireNonNull(rollups, "rollups");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.sampleInterval = Objects.requireNonNull(sampleInterval, "sampleInterval");
        this.current = Objects.requireNonNull(current, "current");
        this.permitted = Objects.requireNonNull(permitted, "permitted");
        Objects.requireNonNull(interval, "interval");
        if (sampleInterval.isZero() || sampleInterval.isNegative() || batchSize < 1
                || batchSize > HistoryRollupStore.MAX_PAGE_SIZE
                || interval.isZero() || interval.isNegative() || interval.toMillis() < 1) {
            throw new IllegalArgumentException("rollup worker budgets are positive and bounded");
        }
        this.batchSize = batchSize;
        if (schedule) {
            scheduler = Executors.newSingleThreadScheduledExecutor(task -> {
                Thread thread = new Thread(task, "tapstate-history-rollup");
                thread.setDaemon(true);
                thread.setPriority(Thread.MIN_PRIORITY);
                return thread;
            });
            scheduler.scheduleWithFixedDelay(this::runOneBatch, interval.toMillis(),
                    interval.toMillis(), TimeUnit.MILLISECONDS);
        } else {
            scheduler = null;
        }
    }

    /** One global batch, with at most one key per scope before moving to the next. */
    synchronized void runOneBatch() {
        List<Work> currentWork;
        try {
            currentWork = List.copyOf(new LinkedHashSet<>(current.get()));
        } catch (RuntimeException failure) {
            LOG.warn("Could not enumerate history rollup work; retrying", failure);
            return;
        }
        if (currentWork.isEmpty()) {
            nextBucket.clear();
            emptyRetryAt.clear();
            refreshHints.clear();
            return;
        }
        List<LevelWork> live = new ArrayList<>();
        for (Resolution resolution : LEVELS) {
            for (Work work : currentWork) {
                live.add(new LevelWork(work, resolution));
            }
        }
        Set<LevelWork> liveSet = new HashSet<>(live);
        nextBucket.keySet().retainAll(liveSet);
        emptyRetryAt.keySet().retainAll(liveSet);
        refreshHints.removeIf(key -> !liveSet.contains(new LevelWork(
                new Work(key.pipelineId(), key.scope()), key.resolution())));
        int idle = 0;
        Set<LevelWork> failedThisPass = new HashSet<>();
        for (int attempts = 0; attempts < batchSize && idle < live.size();) {
            LevelWork level = live.get(Math.floorMod(nextWorkIndex++, live.size()));
            if (failedThisPass.contains(level)) {
                idle++;
                continue;
            }
            try {
                if (!permitted.test(level.work()) || !processOne(level)) {
                    idle++;
                } else {
                    attempts++;
                    idle = 0;
                }
            } catch (RuntimeException failure) {
                attempts++;
                idle++;
                failedThisPass.add(level);
                LOG.warn("Could not build a closed history rollup bucket for pipeline {}; retrying",
                        level.work().pipelineId(), failure);
            }
        }
    }

    /** A lost hint never extends cache freshness; callers still descend when the old entry expires. */
    synchronized boolean requestRefresh(Key key) {
        Objects.requireNonNull(key, "key");
        Instant now = clock.instant();
        if (key.bucketEnd().isAfter(now)
                || key.bucketStart().isBefore(ceil(now.minus(raw.retention()), key.resolution()))) {
            return false;
        }
        if (refreshHints.contains(key)) {
            return true;
        }
        if (refreshHints.size() >= MAX_REFRESH_HINTS) {
            return false;
        }
        return refreshHints.add(key);
    }

    private boolean processOne(LevelWork level) {
        Work work = level.work();
        Resolution resolution = level.resolution();
        Key hinted = refreshHints.stream()
                .filter(key -> key.pipelineId().equals(work.pipelineId()) && key.scope().equals(work.scope())
                        && key.resolution() == resolution)
                .findFirst().orElse(null);
        if (hinted != null) {
            if (hinted.bucketStart().isBefore(ceil(clock.instant().minus(raw.retention()), resolution))) {
                refreshHints.remove(hinted);
                return true;
            }
            BuildResult result = upsert(hinted);
            if (result != BuildResult.OWNER_LOST) {
                refreshHints.remove(hinted);
            }
            if (result == BuildResult.WRITTEN) {
                emptyRetryAt.remove(level);
            }
            return result != BuildResult.OWNER_LOST;
        }

        Instant retryAt = emptyRetryAt.get(level);
        if (retryAt != null && clock.instant().isBefore(retryAt)) {
            return false;
        }

        Instant from = nextBucket.get(level);
        if (from == null) {
            Instant cutoff = ceil(clock.instant().minus(raw.retention()), resolution);
            Page first = raw.readPageVisible(work.pipelineId(), visibility(work.scope()),
                    cutoff, clock.instant(), null, 1);
            if (first.entries().isEmpty()) {
                return false;
            }
            from = floor(first.entries().getFirst().key().observedAt(), resolution);
            nextBucket.put(level, from);
        }
        Instant cutoff = ceil(clock.instant().minus(raw.retention()), resolution);
        if (from.isBefore(cutoff)) {
            from = cutoff;
            nextBucket.put(level, from);
        }
        Key key = new Key(work.pipelineId(), work.scope(), resolution, from);
        if (key.bucketEnd().isAfter(clock.instant())) {
            return false;
        }
        if (rollups.read(key).isEmpty()) {
            BuildResult built = upsert(key);
            if (built == BuildResult.NO_INPUT) {
                emptyRetryAt.put(level, clock.instant().plus(EMPTY_POLL_INTERVAL));
                return false;
            }
            if (built == BuildResult.OWNER_LOST) {
                return false;
            }
        }
        emptyRetryAt.remove(level);
        nextBucket.put(level, key.bucketEnd());
        return true;
    }

    private BuildResult upsert(Key key) {
        Bucket cascaded = fromChildren(key);
        if (cascaded != null) {
            if (!permitted.test(new Work(key.pipelineId(), key.scope()))) {
                return BuildResult.OWNER_LOST;
            }
            rollups.upsert(cascaded);
            return BuildResult.WRITTEN;
        }
        return upsertFromRaw(key);
    }

    private BuildResult upsertFromRaw(Key key) {
        Instant readStartedAt = clock.instant().truncatedTo(ChronoUnit.MILLIS);
        Visibility visibility = visibility(key.scope());
        Entry predecessor = raw.predecessorVisible(key.pipelineId(), visibility, key.bucketStart())
                .filter(entry -> !entry.key().observedAt().isBefore(readStartedAt.minus(raw.retention())))
                .orElse(null);
        List<Entry> entries = new ArrayList<>();
        TreeSet<String> tables = new TreeSet<>();
        RateHistoryStore.Key after = null;
        boolean tooLarge = false;
        while (true) {
            Page page = raw.readPageVisible(key.pipelineId(), visibility,
                    key.bucketStart(), key.bucketEnd(), after, RateHistoryStore.MAX_PAGE_SIZE);
            for (Entry entry : page.entries()) {
                entries.add(entry);
                tables.addAll(entry.sample().lag().keySet());
                if (entries.size() > MAX_RAW_ENTRIES_PER_BUCKET
                        || tables.size() > HistoryRollupStore.MAX_LAGS_PER_FRAGMENT) {
                    tooLarge = true;
                    break;
                }
            }
            if (tooLarge || !page.hasMore()) {
                break;
            }
            after = page.lastKey().orElseThrow();
        }

        List<Fragment> fragments = List.of();
        List<Gap> gaps = List.of();
        if (!tooLarge) {
            Entry successor = raw.successorVisible(key.pipelineId(), visibility, key.bucketEnd()).orElse(null);
            if (entries.isEmpty() && successor == null) {
                return BuildResult.NO_INPUT;
            }
            HistoryAggregator aggregator = new HistoryAggregator(key.bucketStart(), key.bucketEnd(),
                    key.bucketStart(), key.resolution().duration(), sampleInterval, List.copyOf(tables),
                    predecessor == null ? PipelineMetricsHistory.StartReason.WINDOW_START
                            : PipelineMetricsHistory.StartReason.CONTINUATION,
                    HistoryRollupStore.MAX_FRAGMENTS + 1);
            aggregator.begin(predecessor);
            for (Entry entry : entries) {
                aggregator.add(entry);
                if (aggregator.full()) {
                    tooLarge = true;
                    break;
                }
            }
            if (!tooLarge) {
                HistoryAggregator.Projection projection = aggregator.finish(successor);
                tooLarge = projection.points().size() > HistoryRollupStore.MAX_FRAGMENTS
                        || projection.gaps().size() > HistoryRollupStore.MAX_GAPS;
                if (!tooLarge) {
                    try {
                        fragments = projection.points().stream().map(HistoryRollupWorker::fragment).toList();
                        gaps = projection.gaps().stream().map(HistoryRollupWorker::gap).toList();
                    } catch (IllegalArgumentException unrepresentable) {
                        tooLarge = true;
                    }
                }
            }
        }
        Instant computedAt = clock.instant().truncatedTo(ChronoUnit.MILLIS);
        Instant validUntil = readStartedAt.plus(HistoryRollupStore.MAX_CACHE_AGE);
        if (!computedAt.isBefore(validUntil) || computedAt.isBefore(key.bucketEnd())) {
            throw new IllegalStateException("rollup inputs expired or the clock moved before bucket close");
        }
        if (!permitted.test(new Work(key.pipelineId(), key.scope()))) {
            return BuildResult.OWNER_LOST;
        }
        rollups.upsert(new Bucket(key, computedAt, readStartedAt, validUntil, tooLarge,
                tooLarge ? List.of() : fragments, tooLarge ? List.of() : gaps, entries.size()));
        return BuildResult.WRITTEN;
    }

    /** A missing, stale or complex child is recomputed from bounded raw input for this coarse bucket. */
    private Bucket fromChildren(Key key) {
        Resolution finer = finer(key.resolution());
        if (finer == null) {
            return null;
        }
        Instant readAt = clock.instant().truncatedTo(ChronoUnit.MILLIS);
        List<Fragment> parts = new ArrayList<>();
        Instant earliestRead = null;
        Instant deadline = null;
        int samples = 0;
        for (Instant at = key.bucketStart(); at.isBefore(key.bucketEnd());
                at = at.plus(finer.duration())) {
            Bucket child = rollups.read(new Key(key.pipelineId(), key.scope(), finer, at)).orElse(null);
            if (child == null || !child.usableAt(readAt) || child.inWindowSamples() < 0
                    || !child.gaps().isEmpty() || child.fragments().size() > 1) {
                return null;
            }
            earliestRead = earliestRead == null || child.inputReadStartedAt().isBefore(earliestRead)
                    ? child.inputReadStartedAt() : earliestRead;
            deadline = deadline == null || child.validUntil().isBefore(deadline)
                    ? child.validUntil() : deadline;
            samples = Math.addExact(samples, child.inWindowSamples());
            for (Fragment fragment : child.fragments()) {
                if (fragment.segment() != 0
                        || fragment.startReason() == HistoryRollupStore.StartReason.GAP
                        || fragment.startReason() == HistoryRollupStore.StartReason.COUNTER_RESET
                        || (fragment.recordsOut() != null && fragment.recordsOutStats() == null)
                        || (fragment.bytesOut() != null && fragment.bytesOutStats() == null)
                        || fragment.resumeAfter() == null) {
                    return null;
                }
                parts.add(fragment);
            }
        }
        Instant computedAt = clock.instant().truncatedTo(ChronoUnit.MILLIS);
        if (parts.isEmpty() || deadline == null || !computedAt.isBefore(deadline)) {
            return null;
        }
        try {
            Fragment merged = merge(parts);
            return new Bucket(key, computedAt, earliestRead, deadline, false,
                    List.of(merged), List.of(), samples);
        } catch (IllegalArgumentException | ArithmeticException unrepresentable) {
            return null;
        }
    }

    private static Resolution finer(Resolution resolution) {
        return switch (resolution) {
            case PT5M -> null;
            case PT30M -> Resolution.PT5M;
            case PT1H -> Resolution.PT30M;
            case PT3H -> Resolution.PT1H;
            case PT6H -> Resolution.PT3H;
        };
    }

    private static Fragment merge(List<Fragment> parts) {
        Fragment first = parts.getFirst();
        Fragment last = parts.getLast();
        Instant start = first.intervalStart();
        Instant end = first.intervalEnd();
        HistoryRollupStore.CounterStats records = null;
        HistoryRollupStore.CounterStats bytes = null;
        Map<String, Lag> lag = new LinkedHashMap<>();
        for (Fragment part : parts) {
            if (part.intervalStart().isBefore(start)) {
                start = part.intervalStart();
            }
            if (part.intervalEnd().isAfter(end)) {
                end = part.intervalEnd();
            }
            records = combine(records, part.recordsOutStats());
            bytes = combine(bytes, part.bytesOutStats());
            for (Lag reading : part.lag()) {
                lag.merge(reading.table(), reading, (earlier, later) -> new Lag(earlier.table(),
                        later.observedAt().isAfter(earlier.observedAt())
                                ? later.observedAt() : earlier.observedAt(),
                        later.observedAt().isAfter(earlier.observedAt()) ? later.last() : earlier.last(),
                        Math.max(earlier.max(), later.max())));
            }
        }
        return new Fragment(0, first.startReason(), start, end,
                projectedRate(records), projectedRate(bytes),
                lag.values().stream().sorted(java.util.Comparator.comparing(Lag::table)).toList(),
                records, bytes, last.resumeAfter(), last.resumeAt());
    }

    private static HistoryRollupStore.CounterStats combine(HistoryRollupStore.CounterStats left,
            HistoryRollupStore.CounterStats right) {
        if (right == null) {
            return left;
        }
        if (left == null) {
            return right;
        }
        return new HistoryRollupStore.CounterStats(left.delta().add(right.delta()),
                Math.addExact(left.coveredNanos(), right.coveredNanos()),
                left.maxRate().max(right.maxRate()));
    }

    private static Rate projectedRate(HistoryRollupStore.CounterStats stats) {
        if (stats == null) {
            return null;
        }
        PipelineMetricsHistory.Rate wire = new PipelineMetricsHistory.Rate(stats.delta(),
                stats.delta().multiply(NANOS_PER_SECOND)
                        .divide(BigDecimal.valueOf(stats.coveredNanos()), INTERNAL_SCALE, RoundingMode.HALF_EVEN),
                stats.maxRate());
        return rate(wire);
    }

    private static Fragment fragment(Emitted emitted) {
        PipelineMetricsHistory.Point point = emitted.point();
        return new Fragment(emitted.segment(),
                HistoryRollupStore.StartReason.valueOf(emitted.startReason().name()),
                point.intervalStart(), point.intervalEnd(), rate(point.recordsOut()), rate(point.bytesOut()),
                point.lag().stream().map(lag -> new Lag(lag.table(), lag.observedAt(),
                        lag.last(), lag.max())).toList(),
                stats(emitted.recordsOutStats()), stats(emitted.bytesOutStats()),
                emitted.resumeAfter(), emitted.resumeAt());
    }

    private static HistoryRollupStore.CounterStats stats(HistoryAggregator.CounterStats stats) {
        return stats == null ? null : new HistoryRollupStore.CounterStats(
                stats.delta(), stats.coveredNanos(), stats.maxRate());
    }

    private static Rate rate(PipelineMetricsHistory.Rate rate) {
        return rate == null ? null : new Rate(rate.delta(), rate.averageRate(), rate.maxRate());
    }

    private static Gap gap(EmittedGap emitted) {
        PipelineMetricsHistory.Gap gap = emitted.gap();
        return new Gap(emitted.segment(), gap.intervalStart(), gap.intervalEnd(),
                HistoryRollupStore.GapReason.valueOf(gap.reason().name()));
    }

    private static Visibility visibility(Scope scope) {
        return scope.incarnationId().map(id -> new Visibility(id, false))
                .orElseGet(() -> new Visibility(null, true));
    }

    private static Instant floor(Instant at, Resolution resolution) {
        long seconds = resolution.duration().toSeconds();
        return Instant.ofEpochSecond(Math.floorDiv(at.getEpochSecond(), seconds) * seconds);
    }

    private static Instant ceil(Instant at, Resolution resolution) {
        Instant floor = floor(at, resolution);
        return floor.equals(at) ? floor : floor.plus(resolution.duration());
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }
}
