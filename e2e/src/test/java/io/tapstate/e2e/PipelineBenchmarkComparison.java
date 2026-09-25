package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/** Pure comparison of fixed, same-machine pipeline benchmark forks. */
final class PipelineBenchmarkComparison {

    private static final int FORKS_PER_ARM = 5;
    private static final int MIN_COMPLETED_DELIVERIES = 10_000;
    private static final double THROUGHPUT_NOISE_LIMIT = 0.03;
    private static final double P99_NOISE_LIMIT = 0.05;
    private static final double IMPROVEMENT_FLOOR = 0.10;
    private static final double REGRESSION_LIMIT = 0.10;
    private static final double OBSERVABILITY_REGRESSION_LIMIT = 0.05;
    private static final double ROUNDING_TOLERANCE = 1e-12;
    private static final List<Arm> ORDER = List.of(
            Arm.A, Arm.B, Arm.B, Arm.A, Arm.A, Arm.B, Arm.B, Arm.A, Arm.A, Arm.B);

    private PipelineBenchmarkComparison() {
    }

    enum Arm { A, B }

    enum Workload { COPY, STATELESS, STATEFUL }

    enum PrimaryMetric { THROUGHPUT, DELIVERY_P99 }

    record Fork(Arm arm, double recordsOutPerSecond, long[] deliveryNanos, long peakHeapBytes, long peakRssBytes) {
        Fork {
            deliveryNanos = deliveryNanos.clone();
        }
    }

    record Summary(double throughput, double throughputRelativeMad, double deliveryP99Nanos,
                   double p99RelativeMad, long peakHeapBytes, long peakRssBytes) {}

    record Pair(Summary baseline, Summary candidate) {}

    record Evaluation(Map<Workload, Pair> workloads, List<String> failures) {
        Evaluation {
            workloads = Map.copyOf(workloads);
            failures = List.copyOf(failures);
        }

        boolean passed() {
            return failures.isEmpty();
        }
    }

    static List<Arm> schedule() {
        return ORDER;
    }

    static Evaluation evaluate(Map<Workload, List<Fork>> runs, Workload target, PrimaryMetric primary) {
        return evaluate(runs, target, primary, REGRESSION_LIMIT, true);
    }

    /** The added observation path is priced before any business optimization is credited. */
    static Evaluation evaluateObservabilityCost(Map<Workload, List<Fork>> runs) {
        return evaluate(runs, null, null, OBSERVABILITY_REGRESSION_LIMIT, false);
    }

    private static Evaluation evaluate(Map<Workload, List<Fork>> runs, Workload target, PrimaryMetric primary,
                                       double performanceRegressionLimit, boolean requireImprovement) {
        if (runs == null || !runs.keySet().equals(java.util.Set.of(Workload.values()))) {
            throw new AssertionError("benchmark requires all three workloads");
        }
        if (requireImprovement && (target == null || primary == null)) {
            throw new AssertionError("optimization requires a target and a primary metric");
        }
        Map<Workload, Pair> summaries = new EnumMap<>(Workload.class);
        List<String> failures = new ArrayList<>();
        for (Workload workload : Workload.values()) {
            List<Fork> forks = runs.get(workload);
            if (forks == null || forks.size() != ORDER.size()) {
                throw new AssertionError(workload + " requires five interleaved forks per arm");
            }
            List<Double> baselineThroughput = new ArrayList<>(FORKS_PER_ARM);
            List<Double> candidateThroughput = new ArrayList<>(FORKS_PER_ARM);
            List<Double> baselineP99 = new ArrayList<>(FORKS_PER_ARM);
            List<Double> candidateP99 = new ArrayList<>(FORKS_PER_ARM);
            long baselineHeap = 0;
            long candidateHeap = 0;
            long baselineRss = 0;
            long candidateRss = 0;
            for (int index = 0; index < ORDER.size(); index++) {
                Fork fork = forks.get(index);
                Arm expected = ORDER.get(index);
                if (fork == null || fork.arm() != expected) {
                    throw new AssertionError(workload + " fork " + index + " must run arm " + expected);
                }
                if (!(fork.recordsOutPerSecond() > 0) || !Double.isFinite(fork.recordsOutPerSecond())
                        || fork.peakHeapBytes() <= 0 || fork.peakRssBytes() <= 0) {
                    throw new AssertionError(workload + " fork " + index + " has invalid throughput or memory");
                }
                double p99 = deliveryP99(fork.deliveryNanos(), workload, index);
                if (expected == Arm.A) {
                    baselineThroughput.add(fork.recordsOutPerSecond());
                    baselineP99.add(p99);
                    baselineHeap = Math.max(baselineHeap, fork.peakHeapBytes());
                    baselineRss = Math.max(baselineRss, fork.peakRssBytes());
                } else {
                    candidateThroughput.add(fork.recordsOutPerSecond());
                    candidateP99.add(p99);
                    candidateHeap = Math.max(candidateHeap, fork.peakHeapBytes());
                    candidateRss = Math.max(candidateRss, fork.peakRssBytes());
                }
            }
            Summary baseline = summary(baselineThroughput, baselineP99, baselineHeap, baselineRss);
            Summary candidate = summary(candidateThroughput, candidateP99, candidateHeap, candidateRss);
            summaries.put(workload, new Pair(baseline, candidate));
            checkNoise(workload, Arm.A, baseline, failures);
            checkNoise(workload, Arm.B, candidate, failures);
            checkRegression(workload, baseline, candidate, performanceRegressionLimit, failures);
        }

        if (requireImprovement) {
            Pair selected = summaries.get(target);
            double gain = primary == PrimaryMetric.THROUGHPUT
                    ? selected.candidate().throughput() / selected.baseline().throughput() - 1
                    : 1 - selected.candidate().deliveryP99Nanos() / selected.baseline().deliveryP99Nanos();
            double baselineNoise = primary == PrimaryMetric.THROUGHPUT
                    ? selected.baseline().throughputRelativeMad() : selected.baseline().p99RelativeMad();
            if (gain + ROUNDING_TOLERANCE < Math.max(IMPROVEMENT_FLOOR, 2 * baselineNoise)) {
                failures.add(target + " target improvement below 10% or twice baseline noise");
            }
        }
        return new Evaluation(summaries, failures);
    }

    private static double deliveryP99(long[] durations, Workload workload, int index) {
        if (durations.length < MIN_COMPLETED_DELIVERIES) {
            throw new AssertionError(workload + " fork " + index + " has fewer than 10,000 completed deliveries");
        }
        long[] sorted = durations.clone();
        Arrays.sort(sorted);
        if (sorted[0] <= 0) {
            throw new AssertionError(workload + " fork " + index + " has an invalid delivery duration");
        }
        return sorted[(int) Math.ceil(sorted.length * 0.99) - 1];
    }

    private static Summary summary(List<Double> throughput, List<Double> p99, long heap, long rss) {
        double throughputMedian = median(throughput);
        double p99Median = median(p99);
        return new Summary(throughputMedian, relativeMad(throughput, throughputMedian), p99Median,
                relativeMad(p99, p99Median), heap, rss);
    }

    private static double median(List<Double> values) {
        double[] sorted = values.stream().mapToDouble(Double::doubleValue).sorted().toArray();
        return sorted[sorted.length / 2];
    }

    private static double relativeMad(List<Double> values, double median) {
        return median(values.stream().map(value -> Math.abs(value - median)).toList()) / median;
    }

    private static void checkNoise(Workload workload, Arm arm, Summary summary, List<String> failures) {
        if (summary.throughputRelativeMad() > THROUGHPUT_NOISE_LIMIT + ROUNDING_TOLERANCE) {
            failures.add(workload + " " + arm + " throughput noise exceeds 3%");
        }
        if (summary.p99RelativeMad() > P99_NOISE_LIMIT + ROUNDING_TOLERANCE) {
            failures.add(workload + " " + arm + " delivery p99 noise exceeds 5%");
        }
    }

    private static void checkRegression(Workload workload, Summary baseline, Summary candidate,
                                        double performanceRegressionLimit,
                                        List<String> failures) {
        int performanceLimitPercent = (int) Math.round(performanceRegressionLimit * 100);
        if (1 - candidate.throughput() / baseline.throughput()
                > performanceRegressionLimit + ROUNDING_TOLERANCE) {
            failures.add(workload + " throughput regresses more than " + performanceLimitPercent + "%");
        }
        if (candidate.deliveryP99Nanos() / baseline.deliveryP99Nanos() - 1
                > performanceRegressionLimit + ROUNDING_TOLERANCE) {
            failures.add(workload + " delivery p99 regresses more than " + performanceLimitPercent + "%");
        }
        if ((double) candidate.peakHeapBytes() / baseline.peakHeapBytes() - 1
                > REGRESSION_LIMIT + ROUNDING_TOLERANCE) {
            failures.add(workload + " peak heap regresses more than 10%");
        }
        if ((double) candidate.peakRssBytes() / baseline.peakRssBytes() - 1
                > REGRESSION_LIMIT + ROUNDING_TOLERANCE) {
            failures.add(workload + " peak RSS regresses more than 10%");
        }
    }
}
