package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * APP process CPU per fixed-cohort row, recomputed by the controller from each phase's retained raw resource
 * trace and its independently reconstructed cohort returns. CPU is enclosed by the inner and outer resource
 * reads around the common return window; both ends are divided by the cohort row count.
 */
final class BenchmarkReturnCpuPerRow {
    private static final int MAX_ATTEMPTS = 4096;

    private BenchmarkReturnCpuPerRow() { }

    record Phase(long cpuNanosLower, long cpuNanosUpper, long cohortRows) { }

    static Phase phase(Map<String, Object> retainedResources, List<BenchmarkReturnTimeBounds.Delivery> cohort,
            Map<String, Object> retainedEnclosure) {
        require(cohort != null && !cohort.isEmpty(), "complete cohort returns are missing");
        var recomputed = BenchmarkReturnResourceBounds.evidence(summary(retainedResources), cohort);
        require(JsonReader.parse(JsonWriter.write(recomputed)).equals(retainedEnclosure),
                "retained phase CPU enclosure differs from the raw resource trace");
        require("RECORDED_COUNTER_ENCLOSURE".equals(recomputed.get("state")),
                "phase CPU enclosure is unavailable: " + recomputed.get("reason"));
        long lower = (Long) recomputed.get("cpuNanosLower"), upper = (Long) recomputed.get("cpuNanosUpper");
        return new Phase(lower, upper, cohort.size());
    }

    static BenchmarkReturnIntervalComparison.Bounds perRow(List<Phase> phases) {
        require(phases != null && !phases.isEmpty(), "measured phase CPU enclosures are missing");
        long lower = 0, upper = 0, rows = 0;
        for (var phase : phases) {
            require(phase.cpuNanosLower() >= 0 && phase.cpuNanosUpper() >= phase.cpuNanosLower() && phase.cohortRows() > 0,
                    "phase CPU enclosure is invalid");
            lower = Math.addExact(lower, phase.cpuNanosLower()); upper = Math.addExact(upper, phase.cpuNanosUpper());
            rows = Math.addExact(rows, phase.cohortRows());
        }
        require(upper > 0, "the cohort window recorded no APP CPU");
        var count = BigDecimal.valueOf(rows);
        return new BenchmarkReturnIntervalComparison.Bounds(
                BigDecimal.valueOf(lower).divide(count, new MathContext(34, RoundingMode.FLOOR)).stripTrailingZeros(),
                BigDecimal.valueOf(upper).divide(count, new MathContext(34, RoundingMode.CEILING)).stripTrailingZeros());
    }

    /** Rebuilds the sampler summary exactly as the fork wrote it; an unreadable trace stays unreadable. */
    static BenchmarkResourceSampler.Summary summary(Map<String, Object> resources) {
        var sampling = BenchmarkReturnExternalAcquirer.object(resources.get("sampling"));
        var attempts = new ArrayList<BenchmarkResourceSampler.Attempt>();
        for (Object raw : BenchmarkReturnExternalAcquirer.list(sampling.get("attempts"), MAX_ATTEMPTS)) {
            var attempt = BenchmarkReturnExternalAcquirer.object(raw);
            var reading = BenchmarkReturnExternalAcquirer.object(attempt.get("reading"));
            Object failure = attempt.get("failureType");
            require(failure == null || failure instanceof String, "resource read failure type is malformed");
            attempts.add(new BenchmarkResourceSampler.Attempt(number(attempt, "index"), number(attempt, "startedAtNanos"),
                    number(attempt, "completedAtNanos"),
                    BenchmarkResourceSampler.Outcome.valueOf(BenchmarkReturnExternalAcquirer.text(attempt.get("outcome"), 32)),
                    (String) failure, "UNAVAILABLE".equals(reading.get("state")) ? null
                            : new BenchmarkProcessProbe.Snapshot(optional(reading.get("cpuNanos")), optional(reading.get("heapUsedBytes")),
                                    optional(reading.get("rssBytes")), optional(reading.get("gcCollectionMillis")))));
        }
        var diagnostics = new BenchmarkResourceSampler.SamplingDiagnostics(
                BenchmarkReturnExternalAcquirer.text(sampling.get("state"), 64), number(sampling, "attemptCount"),
                number(sampling, "failureCount"), number(sampling, "totalDurationNanos"), number(sampling, "maxDurationNanos"),
                number(sampling, "omittedAttempts"), attempts, Math.toIntExact(number(sampling, "retainedAttemptLimit")));
        return new BenchmarkResourceSampler.Summary(number(resources, "cpuNanos"), number(resources, "gcCollectionMillis"),
                number(resources, "peakHeapBytes"), number(resources, "peakRssBytes"),
                Math.toIntExact(number(resources, "sampleCount")), Optional.of(diagnostics));
    }

    private static OptionalLong optional(Object value) {
        if (value == null) { return OptionalLong.empty(); }
        require(value instanceof Integer || value instanceof Long, "resource reading is not an integer");
        return OptionalLong.of(((Number) value).longValue());
    }
    private static long number(Map<String, Object> object, String field) {
        Object value = object.get(field);
        require(value instanceof Integer || value instanceof Long, "resource field is not an integer: " + field);
        return ((Number) value).longValue();
    }
    private static void require(boolean condition, String reason) {
        if (!condition) { throw new AssertionError("return CPU per row " + reason); }
    }
}
