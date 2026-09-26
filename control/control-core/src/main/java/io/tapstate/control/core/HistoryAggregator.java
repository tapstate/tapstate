package io.tapstate.control.core;

import io.tapstate.control.core.PipelineMetricsHistory.Gap;
import io.tapstate.control.core.PipelineMetricsHistory.GapReason;
import io.tapstate.control.core.PipelineMetricsHistory.Lag;
import io.tapstate.control.core.PipelineMetricsHistory.Point;
import io.tapstate.control.core.PipelineMetricsHistory.Rate;
import io.tapstate.control.core.PipelineMetricsHistory.StartReason;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.RateHistoryStore.Entry;
import io.tapstate.spi.store.RateHistoryStore.Key;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The one query-time rate and bucket implementation. It accepts stable-ordered raw entries a batch at a
 * time and retains only one predecessor, one bucket accumulator and the caller's bounded output prefix.
 */
public final class HistoryAggregator {

    public static final String RECORDS_OUT = "records.out";
    public static final String BYTES_OUT = "bytes.out";

    private static final int INTERNAL_SCALE = 30;
    private static final BigDecimal NANOS_PER_SECOND = BigDecimal.valueOf(1_000_000_000L);

    /** One completed point and the stateless position from which a later page recomputes. */
    public record Emitted(Point point, int segment, StartReason startReason, Key resumeAfter,
            Instant resumeAt) {
        public Emitted {
            Objects.requireNonNull(point, "point");
            Objects.requireNonNull(startReason, "startReason");
            Objects.requireNonNull(resumeAfter, "resumeAfter");
            Objects.requireNonNull(resumeAt, "resumeAt");
        }
    }

    /** A gap belongs to the segment that starts after it. */
    public record EmittedGap(int segment, Gap gap) {
        public EmittedGap {
            Objects.requireNonNull(gap, "gap");
        }
    }

    public record Projection(List<Emitted> points, List<EmittedGap> gaps, int peakRawEntriesHeld) {
        public Projection {
            points = List.copyOf(points);
            gaps = List.copyOf(gaps);
        }
    }

    private final Instant from;
    private final Instant to;
    private final Instant resumeFrom;
    private final Duration resolution;
    private final Duration gapThreshold;
    private final List<String> tables;
    private final int outputCeiling;
    private final List<Emitted> emitted = new ArrayList<>();
    private final List<EmittedGap> gaps = new ArrayList<>();

    private Entry previous;
    private Bucket active;
    private int segment;
    private StartReason segmentReason;

    public HistoryAggregator(Instant from, Instant to, Instant resumeFrom, Duration resolution,
            Duration sampleInterval, List<String> tables, StartReason initialReason, int outputCeiling) {
        this.from = Objects.requireNonNull(from, "from");
        this.to = Objects.requireNonNull(to, "to");
        this.resumeFrom = Objects.requireNonNull(resumeFrom, "resumeFrom");
        this.resolution = Objects.requireNonNull(resolution, "resolution");
        Objects.requireNonNull(sampleInterval, "sampleInterval");
        this.tables = List.copyOf(Objects.requireNonNull(tables, "tables"));
        this.segmentReason = Objects.requireNonNull(initialReason, "initialReason");
        this.outputCeiling = outputCeiling;
        if (!from.isBefore(to) || resumeFrom.isBefore(from) || resumeFrom.isAfter(to)) {
            throw new IllegalArgumentException("aggregate bounds are ordered inside the effective window");
        }
        if (resolution.isZero() || resolution.isNegative() || resolution.toNanosPart() != 0) {
            throw new IllegalArgumentException("an aggregate resolution is a positive whole number of seconds");
        }
        if (sampleInterval.isZero() || sampleInterval.isNegative()) {
            throw new IllegalArgumentException("a sample interval is positive");
        }
        if (outputCeiling < 1) {
            throw new IllegalArgumentException("an output ceiling is positive");
        }
        this.gapThreshold = sampleInterval.multipliedBy(2);
    }

    /** Supplies the predecessor without outputting it. */
    public void begin(Entry predecessor) {
        if (previous != null) {
            throw new IllegalStateException("a history aggregation predecessor is supplied once");
        }
        previous = predecessor;
    }

    /** Adds one in-window sample. Calls may span any number of store pages. */
    public void add(Entry current) {
        Objects.requireNonNull(current, "current");
        if (previous != null && compare(previous.key(), current.key()) >= 0) {
            throw new IllegalStateException("rate-history entries are not in stable ascending order");
        }
        if (previous == null) {
            addLag(current);
        } else {
            processPair(previous, current, true);
        }
        previous = current;
    }

    /**
     * Completes the projection. A right-boundary successor contributes only the clipped counter interval;
     * its lag and key are never output or treated as consumed.
     */
    public Projection finish(Entry successor) {
        if (!full() && successor != null && previous != null) {
            if (compare(previous.key(), successor.key()) >= 0) {
                throw new IllegalStateException("a history successor does not follow the last sample");
            }
            processPair(previous, successor, false);
        }
        if (!full() && active != null && previous != null) {
            flush(previous.key(), min(to, max(resumeFrom, active.end)));
        }
        return new Projection(emitted, gaps, 2);
    }

    /** True once the retained prefix contains the caller's page plus one proof of a following point. */
    public boolean full() {
        return emitted.size() >= outputCeiling;
    }

    private void processPair(Entry left, Entry right, boolean includeRightLag) {
        RateSample a = left.sample();
        RateSample b = right.sample();
        Duration elapsed = Duration.between(a.observedAt(), b.observedAt());
        if (elapsed.isNegative()) {
            throw new IllegalStateException("rate-history time moved backwards");
        }

        StartReason boundary = boundary(left, right, elapsed);
        if (boundary != null) {
            if (active != null) {
                flush(left.key(), min(to, max(resumeFrom, active.end)));
            }
            segment++;
            segmentReason = boundary;
            if (boundary == StartReason.GAP) {
                Instant gapStart = max(resumeFrom, a.observedAt());
                Instant gapEnd = min(to, b.observedAt());
                if (gapStart.isBefore(gapEnd)) {
                    gaps.add(new EmittedGap(segment, new Gap(gapStart, gapEnd, GapReason.SAMPLE_GAP)));
                }
            }
            if (includeRightLag && !full()) {
                addLag(right);
            }
            return;
        }

        Instant intervalStart = max(resumeFrom, max(from, a.observedAt()));
        Instant intervalEnd = min(to, b.observedAt());
        if (intervalStart.isBefore(intervalEnd)) {
            CounterInterval records = counter(a, b, RECORDS_OUT, elapsed);
            CounterInterval bytes = counter(a, b, BYTES_OUT, elapsed);
            if (records != null || bytes != null) {
                addInterval(left.key(), intervalStart, intervalEnd, records, bytes);
            }
        }
        if (includeRightLag && !full()) {
            addLag(right);
        }
    }

    private StartReason boundary(Entry previous, Entry current, Duration elapsed) {
        RateSample left = previous.sample();
        RateSample right = current.sample();
        if (!elapsed.isZero() && elapsed.compareTo(gapThreshold) >= 0) {
            return StartReason.GAP;
        }
        if (!Objects.equals(left.countingSince(), right.countingSince())
                || decreased(left, right, RECORDS_OUT)
                || decreased(left, right, BYTES_OUT)) {
            return StartReason.COUNTER_RESET;
        }
        if (previous.scope().isPresent() && current.scope().isPresent()
                && !previous.scope().equals(current.scope())) {
            return StartReason.CONTINUATION;
        }
        return null;
    }

    private static boolean decreased(RateSample left, RateSample right, String name) {
        Long before = left.counters().get(name);
        Long after = right.counters().get(name);
        return before != null && after != null && after < before;
    }

    private static CounterInterval counter(RateSample left, RateSample right, String name, Duration elapsed) {
        Long before = left.counters().get(name);
        Long after = right.counters().get(name);
        if (elapsed.isZero() || before == null || after == null
                || left.countingSince() == null || !left.countingSince().equals(right.countingSince())
                || after < before) {
            return null;
        }
        long nanos = nanos(elapsed);
        BigDecimal delta = BigDecimal.valueOf(after - before);
        BigDecimal rate = delta.multiply(NANOS_PER_SECOND)
                .divide(BigDecimal.valueOf(nanos), INTERNAL_SCALE, RoundingMode.HALF_EVEN);
        return new CounterInterval(delta, nanos, rate);
    }

    private void addInterval(Key leftKey, Instant intervalStart, Instant intervalEnd,
            CounterInterval records, CounterInterval bytes) {
        Instant cursor = intervalStart;
        long fullNanos = nanos(Duration.between(intervalStart, intervalEnd));
        while (cursor.isBefore(intervalEnd) && !full()) {
            Instant bucketStart = bucketStart(cursor);
            Instant bucketEnd = bucketStart.plus(resolution);
            Instant sliceEnd = min(intervalEnd, bucketEnd);
            if (active != null && (active.segment != segment || !active.bucketStart.equals(bucketStart))) {
                flush(leftKey, cursor);
                if (full()) {
                    return;
                }
            }
            if (active == null) {
                active = new Bucket(segment, segmentReason, bucketStart);
            }
            long sliceNanos = nanos(Duration.between(cursor, sliceEnd));
            active.addRates(cursor, sliceEnd, records, bytes, sliceNanos, fullNanos);
            cursor = sliceEnd;
            if (cursor.equals(bucketEnd)) {
                flush(leftKey, cursor);
            }
        }
    }

    private void addLag(Entry entry) {
        Instant at = entry.sample().observedAt();
        if (at.isBefore(resumeFrom) || at.isBefore(from) || !at.isBefore(to) || full()) {
            return;
        }
        Instant bucketStart = bucketStart(at);
        if (active != null && (active.segment != segment || !active.bucketStart.equals(bucketStart))) {
            Key marker = previous == null ? entry.key() : previous.key();
            flush(marker, at);
            if (full()) {
                return;
            }
        }
        if (active == null) {
            active = new Bucket(segment, segmentReason, bucketStart);
        }
        active.addFrame(at);
        active.addLag(at, entry.sample().lag(), tables);
    }

    private void flush(Key resumeAfter, Instant resumeAt) {
        if (active == null || !active.hasData()) {
            active = null;
            return;
        }
        if (emitted.size() < outputCeiling) {
            emitted.add(new Emitted(active.point(), active.segment, active.reason, resumeAfter, resumeAt));
        }
        active = null;
    }

    private Instant bucketStart(Instant value) {
        long seconds = resolution.toSeconds();
        long aligned = Math.floorDiv(value.getEpochSecond(), seconds) * seconds;
        return Instant.ofEpochSecond(aligned);
    }

    private static int compare(Key left, Key right) {
        int byTime = left.observedAt().compareTo(right.observedAt());
        return byTime != 0 ? byTime : left.internalKey().compareTo(right.internalKey());
    }

    private static long nanos(Duration duration) {
        return Math.addExact(Math.multiplyExact(duration.getSeconds(), 1_000_000_000L), duration.getNano());
    }

    private static Instant min(Instant left, Instant right) {
        return left.isBefore(right) ? left : right;
    }

    private static Instant max(Instant left, Instant right) {
        return left.isAfter(right) ? left : right;
    }

    private record CounterInterval(BigDecimal delta, long nanos, BigDecimal rate) {
    }

    private static final class CounterBucket {
        private BigDecimal delta = BigDecimal.ZERO;
        private long nanos;
        private BigDecimal maxRate;

        void add(CounterInterval interval, long sliceNanos, long clippedIntervalNanos) {
            BigDecimal clippedDelta = interval.delta()
                    .multiply(BigDecimal.valueOf(clippedIntervalNanos))
                    .divide(BigDecimal.valueOf(interval.nanos()), INTERNAL_SCALE, RoundingMode.HALF_EVEN);
            BigDecimal sliceDelta = clippedDelta
                    .multiply(BigDecimal.valueOf(sliceNanos))
                    .divide(BigDecimal.valueOf(clippedIntervalNanos), INTERNAL_SCALE, RoundingMode.HALF_EVEN);
            delta = delta.add(sliceDelta);
            nanos = Math.addExact(nanos, sliceNanos);
            maxRate = maxRate == null || interval.rate().compareTo(maxRate) > 0 ? interval.rate() : maxRate;
        }

        Rate rate() {
            if (nanos == 0) {
                return null;
            }
            BigDecimal average = delta.multiply(NANOS_PER_SECOND)
                    .divide(BigDecimal.valueOf(nanos), INTERNAL_SCALE, RoundingMode.HALF_EVEN);
            return new Rate(delta, average, maxRate);
        }
    }

    private static final class LagBucket {
        private Instant observedAt;
        private long last;
        private long max = Long.MIN_VALUE;

        void add(Instant at, long value) {
            observedAt = at;
            last = value;
            max = Math.max(max, value);
        }

        Lag finish(String table) {
            return new Lag(table, observedAt, last, max);
        }
    }

    private static final class Bucket {
        private final int segment;
        private final StartReason reason;
        private final Instant bucketStart;
        private final CounterBucket records = new CounterBucket();
        private final CounterBucket bytes = new CounterBucket();
        private final Map<String, LagBucket> lag = new LinkedHashMap<>();
        private Instant start;
        private Instant end;
        private boolean frame;

        Bucket(int segment, StartReason reason, Instant bucketStart) {
            this.segment = segment;
            this.reason = reason;
            this.bucketStart = bucketStart;
        }

        void addRates(Instant from, Instant to, CounterInterval recordsInterval,
                CounterInterval bytesInterval, long sliceNanos, long clippedIntervalNanos) {
            touch(from, to);
            if (recordsInterval != null) {
                records.add(recordsInterval, sliceNanos, clippedIntervalNanos);
            }
            if (bytesInterval != null) {
                bytes.add(bytesInterval, sliceNanos, clippedIntervalNanos);
            }
        }

        void addLag(Instant at, Map<String, Long> readings, List<String> selectedTables) {
            for (String table : selectedTables) {
                Long value = readings.get(table);
                if (value != null) {
                    lag.computeIfAbsent(table, ignored -> new LagBucket()).add(at, value);
                    touch(at, at);
                }
            }
        }

        void addFrame(Instant at) {
            frame = true;
            touch(at, at);
        }

        boolean hasData() {
            return frame || records.nanos != 0 || bytes.nanos != 0 || !lag.isEmpty();
        }

        Point point() {
            List<Lag> readings = lag.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> entry.getValue().finish(entry.getKey()))
                    .toList();
            return new Point(start, end, records.rate(), bytes.rate(), readings);
        }

        private void touch(Instant from, Instant to) {
            start = start == null || from.isBefore(start) ? from : start;
            end = end == null || to.isAfter(end) ? to : end;
        }
    }
}
