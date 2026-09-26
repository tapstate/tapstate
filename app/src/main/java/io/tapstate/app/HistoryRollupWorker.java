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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
 * Builds disposable five-minute buckets on one low-priority, fixed-batch worker. A successful upsert
 * advances only an in-process cursor; after a restart the worker walks deterministic persisted keys from
 * the first retained sample and skips completed buckets. Expired buckets are refreshed only on bounded
 * hints, leaving the query face to descend to raw history when a hint is lost.
 */
final class HistoryRollupWorker implements AutoCloseable {

    static final int DEFAULT_BATCH_SIZE = 16;
    static final Duration DEFAULT_INTERVAL = Duration.ofSeconds(5);
    static final int MAX_REFRESH_HINTS = 64;
    static final int MAX_RAW_ENTRIES_PER_BUCKET = 4096;
    private static final Duration EMPTY_POLL_INTERVAL = Duration.ofMinutes(1);

    private static final Logger LOG = LoggerFactory.getLogger(HistoryRollupWorker.class);
    private static final Resolution RESOLUTION = Resolution.PT5M;

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

    private final RateHistoryStore raw;
    private final HistoryRollupStore rollups;
    private final Clock clock;
    private final Duration sampleInterval;
    private final int batchSize;
    private final Supplier<List<Work>> current;
    private final Predicate<Work> permitted;
    private final ScheduledExecutorService scheduler;
    private final Map<Work, Instant> nextBucket = new HashMap<>();
    private final Map<Work, Instant> emptyRetryAt = new HashMap<>();
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
        List<Work> live;
        try {
            live = List.copyOf(new LinkedHashSet<>(current.get()));
        } catch (RuntimeException failure) {
            LOG.warn("Could not enumerate history rollup work; retrying", failure);
            return;
        }
        if (live.isEmpty()) {
            nextBucket.clear();
            emptyRetryAt.clear();
            refreshHints.clear();
            return;
        }
        Set<Work> liveSet = new HashSet<>(live);
        nextBucket.keySet().retainAll(liveSet);
        emptyRetryAt.keySet().retainAll(liveSet);
        refreshHints.removeIf(key -> !liveSet.contains(new Work(key.pipelineId(), key.scope())));
        int idle = 0;
        for (int attempts = 0; attempts < batchSize && idle < live.size();) {
            Work work = live.get(Math.floorMod(nextWorkIndex++, live.size()));
            try {
                if (!permitted.test(work) || !processOne(work)) {
                    idle++;
                } else {
                    attempts++;
                    idle = 0;
                }
            } catch (RuntimeException failure) {
                attempts++;
                idle = 0;
                LOG.warn("Could not build a closed history rollup bucket for pipeline {}; retrying",
                        work.pipelineId(), failure);
            }
        }
    }

    /** A lost hint never extends cache freshness; callers still descend when the old entry expires. */
    synchronized boolean requestRefresh(Key key) {
        Objects.requireNonNull(key, "key");
        Instant now = clock.instant();
        if (key.resolution() != RESOLUTION || key.bucketEnd().isAfter(now)
                || key.bucketStart().isBefore(ceil(now.minus(raw.retention())))) {
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

    private boolean processOne(Work work) {
        Key hinted = refreshHints.stream()
                .filter(key -> key.pipelineId().equals(work.pipelineId()) && key.scope().equals(work.scope()))
                .findFirst().orElse(null);
        if (hinted != null) {
            if (hinted.bucketStart().isBefore(ceil(clock.instant().minus(raw.retention())))) {
                refreshHints.remove(hinted);
                return true;
            }
            BuildResult result = upsert(hinted);
            if (result != BuildResult.OWNER_LOST) {
                refreshHints.remove(hinted);
            }
            if (result == BuildResult.WRITTEN) {
                emptyRetryAt.remove(work);
            }
            return result != BuildResult.OWNER_LOST;
        }

        Instant retryAt = emptyRetryAt.get(work);
        if (retryAt != null && clock.instant().isBefore(retryAt)) {
            return false;
        }

        Instant from = nextBucket.get(work);
        if (from == null) {
            Instant cutoff = ceil(clock.instant().minus(raw.retention()));
            Page first = raw.readPageVisible(work.pipelineId(), visibility(work.scope()),
                    cutoff, clock.instant(), null, 1);
            if (first.entries().isEmpty()) {
                return false;
            }
            from = floor(first.entries().getFirst().key().observedAt());
            nextBucket.put(work, from);
        }
        Instant cutoff = ceil(clock.instant().minus(raw.retention()));
        if (from.isBefore(cutoff)) {
            from = cutoff;
            nextBucket.put(work, from);
        }
        Key key = new Key(work.pipelineId(), work.scope(), RESOLUTION, from);
        if (key.bucketEnd().isAfter(clock.instant())) {
            return false;
        }
        if (rollups.read(key).isEmpty()) {
            BuildResult built = upsert(key);
            if (built == BuildResult.NO_INPUT) {
                emptyRetryAt.put(work, clock.instant().plus(EMPTY_POLL_INTERVAL));
                return false;
            }
            if (built == BuildResult.OWNER_LOST) {
                return false;
            }
        }
        emptyRetryAt.remove(work);
        nextBucket.put(work, key.bucketEnd());
        return true;
    }

    private BuildResult upsert(Key key) {
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
                    key.bucketStart(), RESOLUTION.duration(), sampleInterval, List.copyOf(tables),
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
                tooLarge ? List.of() : fragments, tooLarge ? List.of() : gaps));
        return BuildResult.WRITTEN;
    }

    private static Fragment fragment(Emitted emitted) {
        PipelineMetricsHistory.Point point = emitted.point();
        return new Fragment(emitted.segment(),
                HistoryRollupStore.StartReason.valueOf(emitted.startReason().name()),
                point.intervalStart(), point.intervalEnd(), rate(point.recordsOut()), rate(point.bytesOut()),
                point.lag().stream().map(lag -> new Lag(lag.table(), lag.observedAt(),
                        lag.last(), lag.max())).toList());
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

    private static Instant floor(Instant at) {
        long seconds = RESOLUTION.duration().toSeconds();
        return Instant.ofEpochSecond(Math.floorDiv(at.getEpochSecond(), seconds) * seconds);
    }

    private static Instant ceil(Instant at) {
        Instant floor = floor(at);
        return floor.equals(at) ? floor : floor.plus(RESOLUTION.duration());
    }

    @Override
    public void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }
}
