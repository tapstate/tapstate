package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.List;

/** Predeclared middle-cohort observed output progress, never a capacity or saturation estimate. */
final class BenchmarkSteadyOutputWindow {
    private BenchmarkSteadyOutputWindow() { }
    static final int BINS = 10;
    static final double MAX_HALF_TREND = 0.05;
    record Reading(long startedAtNanos, long endedAtNanos, long completedDeliveries,
                   List<Long> fixedBins, double halfTrend, double recordsPerSecond) {
        Reading { fixedBins = List.copyOf(fixedBins); }
    }

    static Reading read(List<Long> times) {
        if (times.size() < 10_000 || times.size() > 96_000) { throw new AssertionError("fixed steady cohort lacks its required bounded delivery count"); }
        long first = times.getFirst(), last = times.getLast();
        long duration = last - first;
        if (duration <= 0) { throw new AssertionError("fixed output cohort has no positive observed interval"); }
        return readWindow(times, first, last, true);
    }

    private static Reading readWindow(List<Long> times, long first, long last, boolean qualify) {
        long duration = last - first;
        long[] bins = new long[BINS];
        long previous = Long.MIN_VALUE;
        long completed = 0;
        // Every operation in the same open-lower, closed-upper interval is counted once.
        for (int i = 0; i < times.size(); i++) {
            long at = times.get(i);
            if (at < previous) { throw new AssertionError("fixed output cohort timeline moved backward"); }
            previous = at;
            if (at <= first || at > last) { continue; }
            int bin = (int) Math.min(BINS - 1, Math.floor(((double) (at - first) * BINS) / duration));
            bins[bin]++;
            completed++;
        }
        if (completed < 10_000) { throw new AssertionError("common output interval has fewer than ten thousand completed deliveries"); }
        long early = 0, late = 0;
        List<Long> counts = new ArrayList<>();
        for (int i = 0; i < BINS; i++) {
            counts.add(bins[i]);
            if (i < BINS / 2) { early += bins[i]; } else { late += bins[i]; }
        }
        double trend = Math.abs((double) late / early - 1);
        var reading = new Reading(first, last, completed, counts, trend, completed * 1e9 / duration);
        if (qualify) { requireSteady(reading); }
        return reading;
    }

    static Reading readServerOperations(List<Long> wallMillis) {
        if (wallMillis.stream().anyMatch(java.util.Objects::isNull)) { throw new AssertionError("target operation timeline is unavailable"); }
        validateWallSamples(wallMillis);
        // Logical oplog order is validated by the observer. Concurrent writes independently sample
        // server wall dates, so temporal bins use every original timestamp in chronological order.
        var nanos = wallMillis.stream().sorted().map(value -> Math.multiplyExact(value, 1_000_000L)).toList();
        return read(nanos);
    }

    static Reading readOperationCohort(List<Long> wallMillis, List<Long> observedNanos) {
        if (wallMillis.size() != observedNanos.size() || observedNanos.stream().anyMatch(java.util.Objects::isNull)) {
            throw new AssertionError("operation and observed delivery cohorts differ");
        }
        for (int i=1;i<observedNanos.size();i++) {
            if (observedNanos.get(i)<observedNanos.get(i-1)) { throw new AssertionError("local observed delivery cohort moved backward"); }
        }
        return readServerOperations(wallMillis);
    }

    static Reading readCommonOperations(List<List<Long>> streams) {
        return readCommonOperations(streams, true);
    }

    static Reading readCommonOperations(List<List<Long>> streams, boolean qualify) {
        var merged = mergeValidatedOperationStreams(streams);
        if (merged.size() < 10_000 || merged.size() > 96_000 || streams.stream().anyMatch(List::isEmpty)) {
            throw new AssertionError("common operation streams lack their bounded cohorts");
        }
        var interval = commonIntervalMillis(streams);
        long first = interval.first(), last = interval.last();
        return readWindow(merged.stream().map(value -> Math.multiplyExact(value, 1_000_000L)).toList(),
                Math.multiplyExact(first, 1_000_000L), Math.multiplyExact(last, 1_000_000L), qualify);
    }

    static void requireSteady(Reading reading) {
        if (reading.fixedBins().stream().anyMatch(count -> count == 0)) {
            throw new AssertionError("fixed output cohort contains an idle time bin");
        }
        long early = reading.fixedBins().subList(0, BINS / 2).stream().mapToLong(Long::longValue).sum();
        long late = reading.fixedBins().subList(BINS / 2, BINS).stream().mapToLong(Long::longValue).sum();
        // Integer counts preserve the inclusive five-percent boundary without rounding it upward.
        if (Math.multiplyExact(Math.abs(late - early), 20) > early) {
            throw new AssertionError("fixed output cohort has excessive first-to-last-half trend");
        }
    }

    static List<Long> mergeValidatedOperationStreams(List<List<Long>> streams) {
        if (streams.isEmpty() || streams.size() > 2) { throw new AssertionError("operation stream set differs from fixed targets"); }
        var merged = new ArrayList<Long>();
        for (List<Long> stream : streams) {
            validateWallSamples(stream);
            merged.addAll(stream);
        }
        merged.sort(Long::compare); return List.copyOf(merged);
    }

    record ServerInterval(long first, long last) { }

    static ServerInterval commonIntervalMillis(List<List<Long>> streams) {
        if (streams.isEmpty() || streams.size() > 2 || streams.stream().anyMatch(List::isEmpty)) {
            throw new AssertionError("operation stream set differs from fixed targets");
        }
        long first = Long.MIN_VALUE, last = Long.MAX_VALUE;
        for (List<Long> stream : streams) {
            validateWallSamples(stream);
            long minimum = Long.MAX_VALUE, maximum = Long.MIN_VALUE;
            for (Long value : stream) {
                if (value == null) { throw new AssertionError("target operation clock is missing before stream merge"); }
                minimum = Math.min(minimum, value); maximum = Math.max(maximum, value);
            }
            first = Math.max(first, minimum); last = Math.min(last, maximum);
        }
        if (last <= first) { throw new AssertionError("target output streams have no common interval"); }
        return new ServerInterval(first, last);
    }

    private static void validateWallSamples(List<Long> stream) {
        var samples = new BenchmarkTargetClock.WallSamples();
        for (Long current : stream) {
            if (!samples.accept(current)) {
                throw new AssertionError("target operation clock is missing or moved backward beyond clock uncertainty"
                        + "; highWaterMillis=" + samples.highWaterMillis() + "; currentWallMillis=" + current
                        + "; uncertaintyMillis=" + BenchmarkTargetClock.ENDPOINT_RESOLUTION_ERROR_MILLIS);
            }
        }
    }
}
