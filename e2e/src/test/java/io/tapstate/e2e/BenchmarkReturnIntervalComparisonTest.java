package io.tapstate.e2e;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnIntervalComparisonTest {
    @Test void an_interval_containing_the_failed_legacy_tolerance_boundary_cannot_pass() {
        var input = runs(0);
        input.put(PipelineBenchmarkComparison.Workload.STATELESS,
                input.get(PipelineBenchmarkComparison.Workload.STATELESS).stream().map(fork ->
                        new BenchmarkReturnIntervalComparison.Fork(fork.arm(), fork.arm() == PipelineBenchmarkComparison.Arm.A
                                ? bounds(1000, 1000) : new BenchmarkReturnIntervalComparison.Bounds(
                                        new BigDecimal("949.999999999"), new BigDecimal("950")),
                                bounds(1000, 1000), 10000, 1000, 1000)).toList());
        var result = BenchmarkReturnIntervalComparison.evaluateObservabilityCost(input);
        assertThat(result.state()).isEqualTo(BenchmarkReturnIntervalComparison.State.UNKNOWN);
        assertThat(result.unresolved()).contains("STATELESS throughput regresses more than 5%");
    }

    @Test void floating_statistic_enclosures_cover_endpoint_and_interior_grid_assignments() {
        var inputs = List.of(bounds(90, 110), bounds(95, 105), bounds(99, 101), bounds(103, 115), bounds(80, 100));
        var result = BenchmarkReturnIntervalComparison.floatingStatistics(inputs);
        for (int assignment = 0; assignment < 243; assignment++) {
            int cursor = assignment; double[] values = new double[5];
            for (int index = 0; index < 5; index++) {
                int selected = cursor % 3; cursor /= 3;
                var row = inputs.get(index);
                values[index] = (selected == 0 ? row.lower() : selected == 2 ? row.upper()
                        : row.lower().add(row.upper()).divide(BigDecimal.valueOf(2))).doubleValue();
            }
            java.util.Arrays.sort(values); double median = values[2];
            double mad = java.util.Arrays.stream(values).map(value -> Math.abs(value - median)).sorted().toArray()[2] / median;
            assertThat(result.median().lower().doubleValue()).isLessThanOrEqualTo(median);
            assertThat(result.median().upper().doubleValue()).isGreaterThanOrEqualTo(median);
            assertThat(result.relativeMad().lower().doubleValue()).isLessThanOrEqualTo(mad);
            assertThat(result.relativeMad().upper().doubleValue()).isGreaterThanOrEqualTo(mad);
        }
    }

    @Test void point_metrics_preserve_existing_cost_decisions_including_rounding_tolerance() {
        for (int scenario = 0; scenario < 9; scenario++) {
            var bounded = runs(scenario); var scalar = new EnumMap<PipelineBenchmarkComparison.Workload,
                    List<PipelineBenchmarkComparison.Fork>>(PipelineBenchmarkComparison.Workload.class);
            bounded.forEach((workload, forks) -> scalar.put(workload, forks.stream().map(fork -> {
                long[] rows = new long[10000]; java.util.Arrays.fill(rows, fork.p99().lower().longValueExact());
                return new PipelineBenchmarkComparison.Fork(fork.arm(), fork.throughput().lower().doubleValue(), rows,
                        fork.peakHeapBytes(), fork.peakRssBytes());
            }).toList()));
            var prior = PipelineBenchmarkComparison.evaluateObservabilityCost(scalar);
            var current = BenchmarkReturnIntervalComparison.evaluateObservabilityCost(bounded);
            assertThat(current.unresolved()).isEmpty();
            assertThat(current.state() == BenchmarkReturnIntervalComparison.State.PASS).isEqualTo(prior.passed());
            assertThat(current.failures()).containsExactlyInAnyOrderElementsOf(prior.failures());
        }
    }

    @Test void shared_median_and_mad_enclose_every_endpoint_assignment() {
        var inputs = List.of(bounds(90, 110), bounds(95, 105), bounds(99, 101), bounds(103, 115), bounds(80, 100));
        var result = BenchmarkReturnIntervalComparison.statistics(inputs);
        for (int mask = 0; mask < 32; mask++) {
            var values = new ArrayList<BigDecimal>(5);
            for (int index = 0; index < 5; index++) values.add((mask & (1 << index)) == 0 ? inputs.get(index).lower() : inputs.get(index).upper());
            values.sort(BigDecimal::compareTo); var median = values.get(2);
            var deviations = values.stream().map(value -> value.subtract(median).abs()).sorted().toList();
            var mad = deviations.get(2);
            assertThat(result.median().lower()).isLessThanOrEqualTo(median);
            assertThat(result.median().upper()).isGreaterThanOrEqualTo(median);
            assertThat(result.relativeMad().lower().multiply(median)).isLessThanOrEqualTo(mad);
            assertThat(result.relativeMad().upper().multiply(median)).isGreaterThanOrEqualTo(mad);
        }
    }

    @Test void uncertain_noise_and_regression_remain_unknown() {
        var input = runs(0); var changed = input.get(PipelineBenchmarkComparison.Workload.STATELESS).stream()
                .map(fork -> new BenchmarkReturnIntervalComparison.Fork(fork.arm(), fork.arm() == PipelineBenchmarkComparison.Arm.A
                        ? bounds(990, 1010) : bounds(930, 960), bounds(990, 1010), 10000, 1000, 1000)).toList();
        input.put(PipelineBenchmarkComparison.Workload.STATELESS, changed);
        var result = BenchmarkReturnIntervalComparison.evaluateObservabilityCost(input);
        assertThat(result.state()).isEqualTo(BenchmarkReturnIntervalComparison.State.UNKNOWN);
        assertThat(result.failures()).isEmpty();
        assertThat(result.unresolved()).contains("STATELESS throughput regresses more than 5%");
    }

    @Test void proven_regression_is_failed_even_when_noise_is_uncertain() {
        var input = runs(0); input.put(PipelineBenchmarkComparison.Workload.COPY,
                input.get(PipelineBenchmarkComparison.Workload.COPY).stream().map(fork -> new BenchmarkReturnIntervalComparison.Fork(
                        fork.arm(), fork.arm() == PipelineBenchmarkComparison.Arm.A ? bounds(990, 1010) : bounds(800, 900),
                        bounds(990, 1010), 10000, 1000, 1000)).toList());
        var result = BenchmarkReturnIntervalComparison.evaluateObservabilityCost(input);
        assertThat(result.state()).isEqualTo(BenchmarkReturnIntervalComparison.State.FAIL);
        assertThat(result.failures()).contains("COPY throughput regresses more than 5%");
        assertThat(result.unresolved()).isNotEmpty();
    }

    @Test void incomplete_workloads_or_forks_and_changed_order_are_rejected() {
        var missing = runs(0); missing.remove(PipelineBenchmarkComparison.Workload.STATEFUL);
        assertThatThrownBy(() -> BenchmarkReturnIntervalComparison.evaluateObservabilityCost(missing)).hasMessageContaining("all three workloads");
        var shortRun = runs(0); shortRun.put(PipelineBenchmarkComparison.Workload.COPY, shortRun.get(PipelineBenchmarkComparison.Workload.COPY).subList(0, 9));
        assertThatThrownBy(() -> BenchmarkReturnIntervalComparison.evaluateObservabilityCost(shortRun)).hasMessageContaining("five interleaved forks");
        var reordered = runs(0); var rows = new ArrayList<>(reordered.get(PipelineBenchmarkComparison.Workload.COPY));
        java.util.Collections.swap(rows, 0, 1); reordered.put(PipelineBenchmarkComparison.Workload.COPY, rows);
        assertThatThrownBy(() -> BenchmarkReturnIntervalComparison.evaluateObservabilityCost(reordered)).hasMessageContaining("fork order");
    }

    @Test void tiny_scientific_decimal_is_rejected_before_material_expansion() {
        assertThatThrownBy(() -> new BenchmarkReturnIntervalComparison.Bounds(new BigDecimal("1e-1000000000"), BigDecimal.ONE))
                .hasMessageContaining("prior bound");
        assertThatThrownBy(() -> BenchmarkReturnIntervalComparison.Bounds.point(Double.NaN)).hasMessageContaining("finite");
        assertThatThrownBy(() -> new BenchmarkReturnIntervalComparison.Fork(PipelineBenchmarkComparison.Arm.A,
                bounds(1, 2), bounds(1, 2), 9999, 100, 100)).hasMessageContaining("incomplete");
    }

    private static EnumMap<PipelineBenchmarkComparison.Workload, List<BenchmarkReturnIntervalComparison.Fork>> runs(int scenario) {
        var result = new EnumMap<PipelineBenchmarkComparison.Workload, List<BenchmarkReturnIntervalComparison.Fork>>(PipelineBenchmarkComparison.Workload.class);
        for (var workload : PipelineBenchmarkComparison.Workload.values()) {
            var forks = new ArrayList<BenchmarkReturnIntervalComparison.Fork>(); int a = 0, b = 0;
            for (var arm : PipelineBenchmarkComparison.schedule()) {
                int forkIndex = arm == PipelineBenchmarkComparison.Arm.A ? a++ : b++;
                double rate = arm == PipelineBenchmarkComparison.Arm.B && scenario == 1 ? 949 : 1000;
                long p99 = arm == PipelineBenchmarkComparison.Arm.B && scenario == 2 ? 1051 : 1000;
                if (scenario == 3 && arm == PipelineBenchmarkComparison.Arm.A) rate = new double[]{900, 950, 1000, 1050, 1100}[forkIndex];
                if (scenario == 4 && arm == PipelineBenchmarkComparison.Arm.B) p99 = new long[]{880, 940, 1000, 1060, 1120}[forkIndex];
                long heap = arm == PipelineBenchmarkComparison.Arm.B && scenario == 5 ? 1101 : 1000;
                long rss = arm == PipelineBenchmarkComparison.Arm.B && scenario == 6 ? 1101 : 1000;
                if (scenario == 7 && arm == PipelineBenchmarkComparison.Arm.B) rate = 950 - 0.0000000005;
                if (scenario == 8 && arm == PipelineBenchmarkComparison.Arm.B) rate = 949.999999999;
                forks.add(new BenchmarkReturnIntervalComparison.Fork(arm, BenchmarkReturnIntervalComparison.Bounds.point(rate),
                        BenchmarkReturnIntervalComparison.Bounds.point(p99), 10000, heap, rss));
            }
            result.put(workload, forks);
        }
        return result;
    }
    private static BenchmarkReturnIntervalComparison.Bounds bounds(long lower, long upper) {
        return new BenchmarkReturnIntervalComparison.Bounds(BigDecimal.valueOf(lower), BigDecimal.valueOf(upper));
    }
}
