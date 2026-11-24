package io.tapstate.e2e;

import java.util.HashSet;
import java.util.List;

/** Qualification for event-correlated latency, independent of aggregate runtime telemetry. */
final class LiveLatencyEligibility {
    private LiveLatencyEligibility() {
    }

    record Sample(String sourceEvent, long latencyNanos) {
    }

    record Reading(String status, int expected, int valid, double coverage, String source,
                   Long p50Nanos, Long p95Nanos, Long p99Nanos, String reason) {
    }

    static Reading assess(int expected, List<Sample> samples, boolean comparableClocks, int minimum, double requiredCoverage) {
        return assess(expected, samples, comparableClocks, minimum, minimum, requiredCoverage);
    }

    static Reading assess(int expected, List<Sample> samples, boolean comparableClocks, int minimumP95,
            int minimumP99, double requiredCoverage) {
        if (expected < 0 || minimumP95 < 1 || minimumP99 < minimumP95 || !(requiredCoverage > 0 && requiredCoverage <= 1)) {
            throw new IllegalArgumentException("Invalid live latency qualification inputs");
        }
        var identities = new HashSet<String>();
        for (var sample : samples) {
            if (sample.sourceEvent() == null || sample.latencyNanos() < 0 || !identities.add(sample.sourceEvent())) {
                throw new IllegalArgumentException("Latency evidence must contain distinct, correlated source events");
            }
        }
        int valid = samples.size();
        if (valid > expected) {
            throw new IllegalArgumentException("More correlated samples than expected source events");
        }
        double coverage = expected == 0 ? 0 : (double) valid / expected;
        if (!comparableClocks || valid < minimumP95 || coverage < requiredCoverage) {
            String reason = !comparableClocks ? "Window-level event correlation and a comparable clock are required"
                    : valid < minimumP95 ? "Too few valid latency samples" : "Confirmation coverage below threshold";
            return new Reading("insufficient-evidence", expected, valid, coverage, null, null, null, null, reason);
        }
        long[] times = samples.stream().mapToLong(Sample::latencyNanos).sorted().toArray();
        return new Reading("eligible", expected, valid, coverage, "correlated-source-to-target-v1",
                nearestRank(times, 0.50), nearestRank(times, 0.95),
                valid >= minimumP99 ? nearestRank(times, 0.99) : null, null);
    }

    private static long nearestRank(long[] sorted, double fraction) {
        return sorted[(int) Math.ceil(sorted.length * fraction) - 1];
    }
}
