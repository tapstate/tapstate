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
        long[] bins = new long[BINS];
        long previous = first;
        // The first observed event defines the open lower boundary; all later events are counted once.
        for (int i = 1; i < times.size(); i++) {
            long at = times.get(i);
            if (at < previous) { throw new AssertionError("fixed output cohort timeline moved backward"); }
            previous = at;
            int bin = (int) Math.min(BINS - 1, Math.floor(((double) (at - first) * BINS) / duration));
            bins[bin]++;
        }
        long early = 0, late = 0;
        List<Long> counts = new ArrayList<>();
        for (int i = 0; i < BINS; i++) {
            if (bins[i] == 0) { throw new AssertionError("fixed output cohort contains an idle time bin"); }
            counts.add(bins[i]);
            if (i < BINS / 2) { early += bins[i]; } else { late += bins[i]; }
        }
        double trend = Math.abs((double) late / early - 1);
        // Integer counts preserve the inclusive five-percent boundary without rounding it upward.
        if (Math.multiplyExact(Math.abs(late - early), 20) > early) {
            throw new AssertionError("fixed output cohort has excessive first-to-last-half trend");
        }
        return new Reading(first, last, times.size() - 1L, counts, trend, (times.size() - 1L) * 1e9 / duration);
    }

    static Reading readServerOperations(List<Long> wallMillis) {
        if (wallMillis.stream().anyMatch(java.util.Objects::isNull)) { throw new AssertionError("target operation timeline is unavailable"); }
        var nanos = wallMillis.stream().map(value -> Math.multiplyExact(value, 1_000_000L)).toList();
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

    static List<Long> mergeValidatedOperationStreams(List<List<Long>> streams) {
        if (streams.isEmpty() || streams.size() > 2) { throw new AssertionError("operation stream set differs from fixed targets"); }
        var merged = new ArrayList<Long>();
        for (List<Long> stream : streams) {
            for (int i=0;i<stream.size();i++) {
                if (stream.get(i)==null || i>0 && stream.get(i)<stream.get(i-1)) {
                    throw new AssertionError("target operation clock is missing or moved backward before stream merge");
                }
            }
            merged.addAll(stream);
        }
        merged.sort(Long::compare); return List.copyOf(merged);
    }
}
