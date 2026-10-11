package io.tapstate.e2e;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Conservative arithmetic for return-metric intervals; method and correctness proof remain separate. */
final class BenchmarkReturnIntervalComparison {
    private static final MathContext LOWER = new MathContext(40, RoundingMode.FLOOR);
    private static final MathContext UPPER = new MathContext(40, RoundingMode.CEILING);
    private static final double LEGACY_TOLERANCE = 1e-12;

    private BenchmarkReturnIntervalComparison() { }

    enum State { PASS, FAIL, UNKNOWN }

    record Bounds(BigDecimal lower, BigDecimal upper) {
        Bounds {
            var ticket = BenchmarkReturnComponentSourceWork.begin(BenchmarkReturnComponentSourceWork.Kind.COMPARISON_INPUT);
            try {
            scalar(lower); scalar(upper);
            require(lower.signum() >= 0 && lower.compareTo(upper) <= 0, "invalid nonnegative bounds");
            BenchmarkReturnWorkBudget.done(ticket);
            } catch (RuntimeException | Error primary) { BenchmarkReturnWorkBudget.failed(ticket, primary); throw primary; }
        }
        static Bounds point(long value) { return new Bounds(BigDecimal.valueOf(value), BigDecimal.valueOf(value)); }
        static Bounds point(double value) {
            require(Double.isFinite(value) && value >= 0, "point is not finite and nonnegative");
            return new Bounds(BigDecimal.valueOf(value), BigDecimal.valueOf(value));
        }
    }

    /** P99 already covers all original measured-phase cohort rows in this fork. */
    record Fork(PipelineBenchmarkComparison.Arm arm, Bounds throughput, Bounds p99,
                long completedCohortRows, long peakHeapBytes, long peakRssBytes) {
        Fork {
            var ticket = BenchmarkReturnComponentSourceWork.begin(BenchmarkReturnComponentSourceWork.Kind.COMPARISON_INPUT);
            try {
            require(arm != null && throughput != null && p99 != null && completedCohortRows >= 10000
                    && throughput.lower().signum() > 0 && p99.lower().signum() > 0
                    && peakHeapBytes > 0 && peakRssBytes > 0, "fork metric or memory input is incomplete");
            BenchmarkReturnWorkBudget.done(ticket);
            } catch (RuntimeException | Error primary) { BenchmarkReturnWorkBudget.failed(ticket, primary); throw primary; }
        }
    }

    record Statistics(Bounds median, Bounds relativeMad) { }
    record Summary(Statistics throughput, Statistics p99, long peakHeapBytes, long peakRssBytes) { }
    record Pair(Summary baseline, Summary candidate) { }
    record Evaluation(Map<PipelineBenchmarkComparison.Workload, Pair> workloads,
                      List<String> failures, List<String> unresolved) {
        Evaluation {
            var ticket = BenchmarkReturnComponentSourceWork.begin(BenchmarkReturnComponentSourceWork.Kind.COMPARISON_INPUT);
            try {
                require(workloads != null && workloads.size() <= 3 && failures != null && failures.size() <= 24
                        && unresolved != null && unresolved.size() <= 24, "evaluation projection exceeds prior roster");
                workloads = Map.copyOf(workloads); failures = List.copyOf(failures); unresolved = List.copyOf(unresolved);
                BenchmarkReturnWorkBudget.done(ticket);
            } catch (RuntimeException | Error primary) { BenchmarkReturnWorkBudget.failed(ticket, primary); throw primary; }
        }
        State state() { return !failures.isEmpty() ? State.FAIL : unresolved.isEmpty() ? State.PASS : State.UNKNOWN; }
    }

    static Evaluation evaluateObservabilityCost(Map<PipelineBenchmarkComparison.Workload, List<Fork>> runs) {
        return BenchmarkReturnComponentSourceWork.entered(BenchmarkReturnComponentSourceWork.Kind.COMPARISON,
                () -> evaluateBounded(runs));
    }
    private static Evaluation evaluateBounded(Map<PipelineBenchmarkComparison.Workload, List<Fork>> runs) {
        require(runs != null && runs.keySet().equals(Set.of(PipelineBenchmarkComparison.Workload.values())),
                "all three workloads are required");
        var summaries = new EnumMap<PipelineBenchmarkComparison.Workload, Pair>(PipelineBenchmarkComparison.Workload.class);
        var failures = new ArrayList<String>(); var unresolved = new ArrayList<String>();
        var order = PipelineBenchmarkComparison.schedule();
        require(order.size() == 10, "the original alternating schedule differs");
        for (var workload : PipelineBenchmarkComparison.Workload.values()) {
            var forks = runs.get(workload);
            require(forks != null && forks.size() == order.size(), "five interleaved forks per arm are required");
            var baseline = new ArrayList<Fork>(5); var candidate = new ArrayList<Fork>(5);
            for (int index = 0; index < order.size(); index++) {
                var fork = forks.get(index);
                require(fork != null && fork.arm() == order.get(index), "fork order or arm differs");
                (fork.arm() == PipelineBenchmarkComparison.Arm.A ? baseline : candidate).add(fork);
            }
            var a = summary(baseline); var b = summary(candidate); summaries.put(workload, new Pair(a, b));
            if (forks.stream().allMatch(fork -> point(fork.throughput()) && point(fork.p99()))) {
                // Preserve the original IEEE-754 operation order at point inputs, including its
                // tolerance boundary. Exact decimal algebra is not an equivalent legacy bridge.
                legacyPointChecks(workload, baseline, candidate, failures);
                continue;
            }
            for (var arm : PipelineBenchmarkComparison.Arm.values()) {
                var current = arm == PipelineBenchmarkComparison.Arm.A ? a : b;
                fpAtMost(workload + " " + arm + " throughput noise exceeds 3%", current.throughput().relativeMad().lower().doubleValue(),
                        current.throughput().relativeMad().upper().doubleValue(), 0.03 + LEGACY_TOLERANCE, failures, unresolved);
                fpAtMost(workload + " " + arm + " delivery p99 noise exceeds 5%", current.p99().relativeMad().lower().doubleValue(),
                        current.p99().relativeMad().upper().doubleValue(), 0.05 + LEGACY_TOLERANCE, failures, unresolved);
            }
            var at = a.throughput().median(); var bt = b.throughput().median();
            double lossLower = Math.nextDown(1 - Math.nextUp(bt.upper().doubleValue() / at.lower().doubleValue()));
            double lossUpper = Math.nextUp(1 - Math.nextDown(bt.lower().doubleValue() / at.upper().doubleValue()));
            fpAtMost(workload + " throughput regresses more than 5%", lossLower, lossUpper,
                    0.05 + LEGACY_TOLERANCE, failures, unresolved);
            var ap = a.p99().median(); var bp = b.p99().median();
            double growthLower = Math.nextDown(Math.nextDown(bp.lower().doubleValue() / ap.upper().doubleValue()) - 1);
            double growthUpper = Math.nextUp(Math.nextUp(bp.upper().doubleValue() / ap.lower().doubleValue()) - 1);
            fpAtMost(workload + " delivery p99 regresses more than 5%", growthLower, growthUpper,
                    0.05 + LEGACY_TOLERANCE, failures, unresolved);
            memoryChecks(workload, a, b, failures);
        }
        return new Evaluation(summaries, failures, unresolved);
    }

    /** The rectangular enclosure retains shared-median uncertainty; it cannot turn uncertainty into PASS. */
    static Statistics statistics(List<Bounds> values) {
        return BenchmarkReturnComponentSourceWork.entered(BenchmarkReturnComponentSourceWork.Kind.REAL_STATISTICS,
                () -> statisticsBounded(values));
    }
    private static Statistics statisticsBounded(List<Bounds> values) {
        require(values != null && values.size() == 5 && values.stream().allMatch(java.util.Objects::nonNull),
                "exactly five bounded values are required");
        var median = median(values);
        require(median.lower().signum() > 0, "the median denominator is not positive");
        var deviations = new ArrayList<Bounds>(5);
        for (var value : values) {
            var lower = BigDecimal.ZERO.max(median.lower().subtract(value.upper())).max(value.lower().subtract(median.upper()));
            var upper = value.lower().subtract(median.upper()).abs().max(value.upper().subtract(median.lower()).abs());
            deviations.add(new Bounds(lower, upper));
        }
        return new Statistics(median, ratio(median(deviations), median));
    }

    private static Summary summary(List<Fork> forks) {
        require(forks.size() == 5, "exactly five forks per arm are required");
        return new Summary(floatingStatistics(forks.stream().map(Fork::throughput).toList()),
                floatingStatistics(forks.stream().map(Fork::p99).toList()),
                forks.stream().mapToLong(Fork::peakHeapBytes).max().orElseThrow(),
                forks.stream().mapToLong(Fork::peakRssBytes).max().orElseThrow());
    }

    private static boolean point(Bounds value) { return value.lower().compareTo(value.upper()) == 0; }

    static Statistics floatingStatistics(List<Bounds> values) {
        return BenchmarkReturnComponentSourceWork.entered(BenchmarkReturnComponentSourceWork.Kind.FLOAT_STATISTICS,
                () -> floatingStatisticsBounded(values));
    }
    private static Statistics floatingStatisticsBounded(List<Bounds> values) {
        require(values != null && values.size() == 5 && values.stream().allMatch(java.util.Objects::nonNull),
                "exactly five bounded values are required");
        if (values.stream().allMatch(BenchmarkReturnIntervalComparison::point)) {
            var points = values.stream().mapToDouble(value -> value.lower().doubleValue()).sorted().toArray();
            double median = points[2];
            double mad = java.util.Arrays.stream(points).map(value -> Math.abs(value - median)).sorted().toArray()[2] / median;
            return new Statistics(Bounds.point(median), Bounds.point(mad));
        }
        double[] lower = values.stream().mapToDouble(value -> outward(value.lower(), false)).sorted().toArray();
        double[] upper = values.stream().mapToDouble(value -> outward(value.upper(), true)).sorted().toArray();
        double ml = lower[2], mu = upper[2];
        require(ml > 0, "floating median denominator is not positive");
        double[] dl = new double[5], du = new double[5];
        for (int index = 0; index < 5; index++) {
            double vl = outward(values.get(index).lower(), false), vu = outward(values.get(index).upper(), true);
            dl[index] = Math.max(0, Math.max(downDifference(ml, vu), downDifference(vl, mu)));
            du[index] = Math.max(upAbsoluteDifference(vl, mu), upAbsoluteDifference(vu, ml));
        }
        java.util.Arrays.sort(dl); java.util.Arrays.sort(du);
        double nl = dl[2] == 0 ? 0 : Math.max(0, Math.nextDown(dl[2] / mu));
        double nu = du[2] == 0 ? 0 : Math.nextUp(du[2] / ml);
        return new Statistics(fpBounds(ml, mu), fpBounds(nl, nu));
    }

    private static double outward(BigDecimal value, boolean upper) {
        double converted = value.doubleValue(); require(Double.isFinite(converted), "decimal is outside finite floating range");
        int direction = new BigDecimal(converted).compareTo(value);
        return upper && direction < 0 ? Math.nextUp(converted) : !upper && direction > 0 ? Math.nextDown(converted) : converted;
    }
    private static double downDifference(double a, double b) { return a == b ? 0 : Math.nextDown(a - b); }
    private static double upAbsoluteDifference(double a, double b) { return a == b ? 0 : Math.nextUp(Math.abs(a - b)); }
    private static Bounds fpBounds(double lower, double upper) {
        require(Double.isFinite(lower) && Double.isFinite(upper), "floating arithmetic overflowed");
        return new Bounds(BigDecimal.valueOf(lower), BigDecimal.valueOf(upper));
    }

    private static void legacyPointChecks(PipelineBenchmarkComparison.Workload workload, List<Fork> baseline,
            List<Fork> candidate, List<String> failures) {
        var a = summary(baseline); var b = summary(candidate);
        for (var arm : PipelineBenchmarkComparison.Arm.values()) {
            var current = arm == PipelineBenchmarkComparison.Arm.A ? a : b;
            if (current.throughput().relativeMad().lower().doubleValue() > 0.03 + LEGACY_TOLERANCE)
                failures.add(workload + " " + arm + " throughput noise exceeds 3%");
            if (current.p99().relativeMad().lower().doubleValue() > 0.05 + LEGACY_TOLERANCE)
                failures.add(workload + " " + arm + " delivery p99 noise exceeds 5%");
        }
        if (1 - b.throughput().median().lower().doubleValue() / a.throughput().median().lower().doubleValue() > 0.05 + LEGACY_TOLERANCE)
            failures.add(workload + " throughput regresses more than 5%");
        if (b.p99().median().lower().doubleValue() / a.p99().median().lower().doubleValue() - 1 > 0.05 + LEGACY_TOLERANCE)
            failures.add(workload + " delivery p99 regresses more than 5%");
        memoryChecks(workload, a, b, failures);
    }
    private static void memoryChecks(PipelineBenchmarkComparison.Workload workload, Summary a, Summary b, List<String> failures) {
        if ((double) b.peakHeapBytes() / a.peakHeapBytes() - 1 > 0.10 + LEGACY_TOLERANCE)
            failures.add(workload + " peak heap regresses more than 10%");
        if ((double) b.peakRssBytes() / a.peakRssBytes() - 1 > 0.10 + LEGACY_TOLERANCE)
            failures.add(workload + " peak RSS regresses more than 10%");
    }

    private static void fpAtMost(String name, double lower, double upper, double limit,
            List<String> failures, List<String> unresolved) {
        require(Double.isFinite(lower) && Double.isFinite(upper) && lower <= upper, "floating gate enclosure is invalid");
        if (lower > limit) failures.add(name); else if (upper > limit) unresolved.add(name);
    }

    private static Bounds median(List<Bounds> values) {
        return new Bounds(values.stream().map(Bounds::lower).sorted().toList().get(2),
                values.stream().map(Bounds::upper).sorted().toList().get(2));
    }

    private static Bounds ratio(Bounds numerator, Bounds denominator) {
        require(denominator.lower().signum() > 0, "ratio denominator is not positive");
        return new Bounds(numerator.lower().divide(denominator.upper(), LOWER), numerator.upper().divide(denominator.lower(), UPPER));
    }

    private static void scalar(BigDecimal value) {
        // Guard exponent and coefficient before compare/subtract/divide can expand their material.
        require(value != null && value.precision() <= 64 && Math.abs((long) value.scale()) <= 64,
                "decimal material exceeds its prior bound");
    }
    private static void require(boolean condition, String reason) {
        if (!condition) { throw new AssertionError("return interval comparison " + reason); }
    }
}
