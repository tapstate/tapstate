package io.tapstate.e2e;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/** Pure pooling of phase interval inputs; the caller must separately prove method and phase acceptance. */
final class BenchmarkReturnIntervalMetrics {
    private static final int MAX_PHASES = 6, MAX_COHORT_ROWS = 512 * 512;
    private BenchmarkReturnIntervalMetrics() { }

    record Phase(String id, long completedLower, long completedUpper, long durationLower, long durationUpper,
                 List<BenchmarkCausalClock.Interval> latencies) {
        Phase {
            var ticket = BenchmarkReturnComponentSourceWork.begin(BenchmarkReturnComponentSourceWork.Kind.METRIC_PHASE);
            try {
            require(id != null && id.length() <= 128 && !id.isBlank() && completedLower >= 10000
                    && completedUpper >= completedLower && completedUpper <= MAX_COHORT_ROWS
                    && durationLower > 0 && durationUpper >= durationLower,
                    "phase count, duration or identity is outside the prior domain");
            require(latencies != null && !latencies.isEmpty() && latencies.size() <= MAX_COHORT_ROWS
                    && completedUpper <= latencies.size(),
                    "phase latency roster exceeds its prior cap");
            // Guard before the immutable copy; negative lower latency endpoints remain evidence.
            var boundedLatencies = latencies;
            latencies = BenchmarkReturnWorkBudget.opaqueRequest(BenchmarkReturnWorkBudget.current(),
                    BenchmarkReturnWorkBudget.Activity.COPY, () -> List.copyOf(boundedLatencies));
            BenchmarkReturnWorkBudget.add(BenchmarkReturnWorkBudget.Activity.COPY, BenchmarkReturnWorkBudget.Work.pass(latencies.size()));
            BenchmarkReturnWorkBudget.done(ticket);
            } catch (RuntimeException | Error primary) { BenchmarkReturnWorkBudget.failed(ticket, primary); throw primary; }
        }
    }

    record Fork(BenchmarkReturnTimeBounds.Rate throughput, BenchmarkCausalClock.Interval p99,
                long completedLower, long completedUpper, long durationLower, long durationUpper,
                int fullCohortRows, List<String> phases) {
        Fork {
            var ticket = BenchmarkReturnComponentSourceWork.begin(BenchmarkReturnComponentSourceWork.Kind.METRIC_RESULT);
            try {
                require(phases != null && phases.size() <= MAX_PHASES, "result phase roster exceeds prior cap");
                phases = List.copyOf(phases); BenchmarkReturnWorkBudget.done(ticket);
            } catch (RuntimeException | Error primary) { BenchmarkReturnWorkBudget.failed(ticket, primary); throw primary; }
        }
    }

    static Fork aggregate(List<Phase> phases) {
        return BenchmarkReturnComponentSourceWork.entered(BenchmarkReturnComponentSourceWork.Kind.METRIC_POOL,
                () -> aggregateBounded(phases));
    }
    private static Fork aggregateBounded(List<Phase> phases) {
        require(phases != null, "bounded measured phases are required");
        int declaredPhases = phases.size();
        require(declaredPhases > 0 && declaredPhases <= MAX_PHASES, "bounded measured phases are required");
        var captured = new ArrayList<Phase>(declaredPhases);
        var ids = new HashSet<String>(); int rows = 0;
        long countLower = 0, countUpper = 0, durationLower = 0, durationUpper = 0;
        // Check total material before allocating or traversing the pooled latency rows.
        for (int index = 0; index < declaredPhases; index++) {
            var phase = phases.get(index);
            require(phase != null && ids.add(phase.id()), "a measured phase is missing or duplicated");
            rows = Math.addExact(rows, phase.latencies().size());
            require(rows <= MAX_COHORT_ROWS, "pooled cohort exceeds the independently registered row cap");
            countLower = Math.addExact(countLower, phase.completedLower()); countUpper = Math.addExact(countUpper, phase.completedUpper());
            durationLower = Math.addExact(durationLower, phase.durationLower()); durationUpper = Math.addExact(durationUpper, phase.durationUpper());
            captured.add(phase);
        }
        require(phases.size() == declaredPhases, "the declared phase roster changed during capture");
        require(rows >= 10000, "pooled p99 needs the complete minimum cohort");
        var pooled = new ArrayList<BenchmarkCausalClock.Interval>(rows);
        for (var phase : captured) pooled.addAll(phase.latencies());
        var p99 = BenchmarkReturnTimeBounds.p99(pooled);
        var lowerNumerator = BigDecimal.valueOf(countLower).multiply(BigDecimal.valueOf(1000000000L));
        var upperNumerator = BigDecimal.valueOf(countUpper).multiply(BigDecimal.valueOf(1000000000L));
        double rateLower = Math.nextDown(lowerNumerator.divide(BigDecimal.valueOf(durationUpper),
                new MathContext(17, RoundingMode.FLOOR)).doubleValue());
        double rateUpper = Math.nextUp(upperNumerator.divide(BigDecimal.valueOf(durationLower),
                new MathContext(17, RoundingMode.CEILING)).doubleValue());
        require(Double.isFinite(rateLower) && Double.isFinite(rateUpper) && rateLower > 0 && rateLower <= rateUpper,
                "pooled rate enclosure is invalid");
        return new Fork(new BenchmarkReturnTimeBounds.Rate(rateLower, rateUpper), p99,
                countLower, countUpper, durationLower, durationUpper, rows, captured.stream().map(Phase::id).toList());
    }

    private static void require(boolean condition, String reason) {
        if (!condition) throw new AssertionError("return interval metrics " + reason);
    }
}
