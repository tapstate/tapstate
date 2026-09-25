package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The benchmark verdict uses every fixed fork, including an unfavorable one. */
class PipelineBenchmarkComparisonTest {

    private static final long TEN_MILLIS = 10_000_000L;

    @Test
    void theOrderIsFixedAndAReorderedRunIsRefused() {
        assertThat(PipelineBenchmarkComparison.schedule()).containsExactly(
                PipelineBenchmarkComparison.Arm.A, PipelineBenchmarkComparison.Arm.B,
                PipelineBenchmarkComparison.Arm.B, PipelineBenchmarkComparison.Arm.A,
                PipelineBenchmarkComparison.Arm.A, PipelineBenchmarkComparison.Arm.B,
                PipelineBenchmarkComparison.Arm.B, PipelineBenchmarkComparison.Arm.A,
                PipelineBenchmarkComparison.Arm.A, PipelineBenchmarkComparison.Arm.B);

        Map<PipelineBenchmarkComparison.Workload, List<PipelineBenchmarkComparison.Fork>> runs = validRuns();
        Collections.swap(runs.get(PipelineBenchmarkComparison.Workload.COPY), 0, 1);
        assertThatThrownBy(() -> evaluate(runs))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("COPY fork 0 must run arm A");
    }

    @Test
    void aForkBelowTenThousandCompletedDeliveriesCannotReportP99() {
        Map<PipelineBenchmarkComparison.Workload, List<PipelineBenchmarkComparison.Fork>> runs = validRuns();
        List<PipelineBenchmarkComparison.Fork> copy = runs.get(PipelineBenchmarkComparison.Workload.COPY);
        PipelineBenchmarkComparison.Fork old = copy.getFirst();
        copy.set(0, new PipelineBenchmarkComparison.Fork(old.arm(), old.recordsOutPerSecond(),
                durations(TEN_MILLIS, 9_999), old.peakHeapBytes(), old.peakRssBytes()));

        assertThatThrownBy(() -> evaluate(runs))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("fewer than 10,000 completed deliveries");
    }

    @Test
    void noisyBaselineAndNoisyCandidateAreBothRejected() {
        Map<PipelineBenchmarkComparison.Workload, List<PipelineBenchmarkComparison.Fork>> noisyBaseline = validRuns();
        setThroughput(noisyBaseline, PipelineBenchmarkComparison.Workload.COPY,
                PipelineBenchmarkComparison.Arm.A, 80, 90, 100, 110, 120);
        PipelineBenchmarkComparison.Evaluation baseline = evaluate(noisyBaseline);
        assertThat(baseline.passed()).isFalse();
        assertThat(baseline.failures()).contains("COPY A throughput noise exceeds 3%");
        assertThat(baseline.workloads().get(PipelineBenchmarkComparison.Workload.COPY)
                .baseline().throughputRelativeMad()).isEqualTo(0.10);

        Map<PipelineBenchmarkComparison.Workload, List<PipelineBenchmarkComparison.Fork>> noisyCandidate = validRuns();
        setP99(noisyCandidate, PipelineBenchmarkComparison.Workload.COPY,
                PipelineBenchmarkComparison.Arm.B, 8, 9, 10, 11, 12);
        PipelineBenchmarkComparison.Evaluation candidate = evaluate(noisyCandidate);
        assertThat(candidate.passed()).isFalse();
        assertThat(candidate.failures()).contains("COPY B delivery p99 noise exceeds 5%");
        assertThat(candidate.workloads().get(PipelineBenchmarkComparison.Workload.COPY)
                .candidate().p99RelativeMad()).isEqualTo(0.10);
    }

    @Test
    void aNinePercentGainDoesNotMeetTheTargetFloor() {
        Map<PipelineBenchmarkComparison.Workload, List<PipelineBenchmarkComparison.Fork>> runs = validRuns();
        setThroughput(runs, PipelineBenchmarkComparison.Workload.COPY,
                PipelineBenchmarkComparison.Arm.B, 109, 109, 109, 109, 109);

        PipelineBenchmarkComparison.Evaluation result = evaluate(runs);
        assertThat(result.passed()).isFalse();
        assertThat(result.failures()).contains("COPY target improvement below 10% or twice baseline noise");
    }

    @Test
    void targetOtherMetricAndNonTargetRegressionsRejectAnOtherwiseFastCandidate() {
        Map<PipelineBenchmarkComparison.Workload, List<PipelineBenchmarkComparison.Fork>> runs = validRuns();
        setP99(runs, PipelineBenchmarkComparison.Workload.COPY,
                PipelineBenchmarkComparison.Arm.B, 12, 12, 12, 12, 12);
        setThroughput(runs, PipelineBenchmarkComparison.Workload.STATELESS,
                PipelineBenchmarkComparison.Arm.B, 89, 89, 89, 89, 89);
        setMemory(runs, PipelineBenchmarkComparison.Workload.STATELESS,
                PipelineBenchmarkComparison.Arm.B, 1_110, 1_150);

        PipelineBenchmarkComparison.Evaluation result = evaluate(runs);
        assertThat(result.passed()).isFalse();
        assertThat(result.failures()).contains(
                "COPY delivery p99 regresses more than 10%",
                "STATELESS throughput regresses more than 10%",
                "STATELESS peak heap regresses more than 10%",
                "STATELESS peak RSS regresses more than 10%");
    }

    @Test
    void stableGainPassesAndP99UsesTheNearestRankWithinEachFork() {
        Map<PipelineBenchmarkComparison.Workload, List<PipelineBenchmarkComparison.Fork>> runs = validRuns();
        for (PipelineBenchmarkComparison.Arm arm : PipelineBenchmarkComparison.Arm.values()) {
            setTailLatency(runs, PipelineBenchmarkComparison.Workload.COPY, arm);
        }

        PipelineBenchmarkComparison.Evaluation result = evaluate(runs);
        assertThat(result.passed()).isTrue();
        PipelineBenchmarkComparison.Pair copy = result.workloads().get(PipelineBenchmarkComparison.Workload.COPY);
        assertThat(copy.baseline().throughput()).isEqualTo(100);
        assertThat(copy.candidate().throughput()).isEqualTo(112);
        assertThat(copy.baseline().deliveryP99Nanos()).isEqualTo(20_000_000);
        assertThat(copy.candidate().deliveryP99Nanos()).isEqualTo(20_000_000);
    }

    @Test
    void aStableP99TargetImprovementCanPassWhileThroughputFallsWithinItsLimit() {
        Map<PipelineBenchmarkComparison.Workload, List<PipelineBenchmarkComparison.Fork>> runs = validRuns();
        setThroughput(runs, PipelineBenchmarkComparison.Workload.COPY,
                PipelineBenchmarkComparison.Arm.B, 100, 100, 100, 100, 100);
        setThroughput(runs, PipelineBenchmarkComparison.Workload.STATEFUL,
                PipelineBenchmarkComparison.Arm.B, 95, 95, 95, 95, 95);
        setP99(runs, PipelineBenchmarkComparison.Workload.STATEFUL,
                PipelineBenchmarkComparison.Arm.B, 8.8, 8.8, 8.8, 8.8, 8.8);

        PipelineBenchmarkComparison.Evaluation result = PipelineBenchmarkComparison.evaluate(
                runs, PipelineBenchmarkComparison.Workload.STATEFUL,
                PipelineBenchmarkComparison.PrimaryMetric.DELIVERY_P99);
        assertThat(result.passed()).isTrue();
    }

    @Test
    void observationCostGateNeedsNoBusinessGainButRejectsFivePercentRegressions() {
        Map<PipelineBenchmarkComparison.Workload, List<PipelineBenchmarkComparison.Fork>> withinBudget = validRuns();
        setThroughput(withinBudget, PipelineBenchmarkComparison.Workload.COPY,
                PipelineBenchmarkComparison.Arm.B, 95, 95, 95, 95, 95);
        setP99(withinBudget, PipelineBenchmarkComparison.Workload.COPY,
                PipelineBenchmarkComparison.Arm.B, 10.5, 10.5, 10.5, 10.5, 10.5);
        assertThat(PipelineBenchmarkComparison.evaluateObservabilityCost(withinBudget).passed()).isTrue();

        setThroughput(withinBudget, PipelineBenchmarkComparison.Workload.STATELESS,
                PipelineBenchmarkComparison.Arm.B, 94, 94, 94, 94, 94);
        setP99(withinBudget, PipelineBenchmarkComparison.Workload.STATEFUL,
                PipelineBenchmarkComparison.Arm.B, 10.6, 10.6, 10.6, 10.6, 10.6);
        PipelineBenchmarkComparison.Evaluation exceeded =
                PipelineBenchmarkComparison.evaluateObservabilityCost(withinBudget);
        assertThat(exceeded.passed()).isFalse();
        assertThat(exceeded.failures()).contains(
                "STATELESS throughput regresses more than 5%",
                "STATEFUL delivery p99 regresses more than 5%");
    }

    private static PipelineBenchmarkComparison.Evaluation evaluate(
            Map<PipelineBenchmarkComparison.Workload, List<PipelineBenchmarkComparison.Fork>> runs) {
        return PipelineBenchmarkComparison.evaluate(runs, PipelineBenchmarkComparison.Workload.COPY,
                PipelineBenchmarkComparison.PrimaryMetric.THROUGHPUT);
    }

    private static Map<PipelineBenchmarkComparison.Workload, List<PipelineBenchmarkComparison.Fork>> validRuns() {
        Map<PipelineBenchmarkComparison.Workload, List<PipelineBenchmarkComparison.Fork>> runs =
                new EnumMap<>(PipelineBenchmarkComparison.Workload.class);
        for (PipelineBenchmarkComparison.Workload workload : PipelineBenchmarkComparison.Workload.values()) {
            List<PipelineBenchmarkComparison.Fork> forks = new ArrayList<>();
            for (PipelineBenchmarkComparison.Arm arm : PipelineBenchmarkComparison.schedule()) {
                double throughput = workload == PipelineBenchmarkComparison.Workload.COPY
                        && arm == PipelineBenchmarkComparison.Arm.B ? 112 : 100;
                forks.add(new PipelineBenchmarkComparison.Fork(
                        arm, throughput, durations(TEN_MILLIS, 10_000), 1_000, 1_000));
            }
            runs.put(workload, forks);
        }
        return runs;
    }

    private static void setThroughput(
            Map<PipelineBenchmarkComparison.Workload, List<PipelineBenchmarkComparison.Fork>> runs,
            PipelineBenchmarkComparison.Workload workload, PipelineBenchmarkComparison.Arm arm, double... values) {
        replaceArm(runs, workload, arm, (old, ordinal) -> new PipelineBenchmarkComparison.Fork(
                old.arm(), values[ordinal], old.deliveryNanos(), old.peakHeapBytes(), old.peakRssBytes()));
    }

    private static void setP99(
            Map<PipelineBenchmarkComparison.Workload, List<PipelineBenchmarkComparison.Fork>> runs,
            PipelineBenchmarkComparison.Workload workload, PipelineBenchmarkComparison.Arm arm, double... millis) {
        replaceArm(runs, workload, arm, (old, ordinal) -> new PipelineBenchmarkComparison.Fork(
                old.arm(), old.recordsOutPerSecond(), durations((long) (millis[ordinal] * 1_000_000), 10_000),
                old.peakHeapBytes(), old.peakRssBytes()));
    }

    private static void setMemory(
            Map<PipelineBenchmarkComparison.Workload, List<PipelineBenchmarkComparison.Fork>> runs,
            PipelineBenchmarkComparison.Workload workload, PipelineBenchmarkComparison.Arm arm, long heap, long rss) {
        replaceArm(runs, workload, arm, (old, ordinal) -> new PipelineBenchmarkComparison.Fork(
                old.arm(), old.recordsOutPerSecond(), old.deliveryNanos(), heap, rss));
    }

    private static void setTailLatency(
            Map<PipelineBenchmarkComparison.Workload, List<PipelineBenchmarkComparison.Fork>> runs,
            PipelineBenchmarkComparison.Workload workload, PipelineBenchmarkComparison.Arm arm) {
        long[] durations = durations(TEN_MILLIS, 10_000);
        Arrays.fill(durations, 9_899, durations.length, 20_000_000L);
        replaceArm(runs, workload, arm, (old, ordinal) -> new PipelineBenchmarkComparison.Fork(
                old.arm(), old.recordsOutPerSecond(), durations, old.peakHeapBytes(), old.peakRssBytes()));
    }

    private static long[] durations(long nanos, int count) {
        long[] durations = new long[count];
        Arrays.fill(durations, nanos);
        return durations;
    }

    private static void replaceArm(
            Map<PipelineBenchmarkComparison.Workload, List<PipelineBenchmarkComparison.Fork>> runs,
            PipelineBenchmarkComparison.Workload workload, PipelineBenchmarkComparison.Arm arm,
            java.util.function.BiFunction<PipelineBenchmarkComparison.Fork, Integer,
                    PipelineBenchmarkComparison.Fork> replacement) {
        List<PipelineBenchmarkComparison.Fork> forks = runs.get(workload);
        int ordinal = 0;
        for (int index = 0; index < forks.size(); index++) {
            PipelineBenchmarkComparison.Fork old = forks.get(index);
            if (old.arm() == arm) {
                forks.set(index, replacement.apply(old, ordinal++));
            }
        }
    }
}
