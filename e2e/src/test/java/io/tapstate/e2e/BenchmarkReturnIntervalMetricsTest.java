package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnIntervalMetricsTest {
    @Test void mutable_input_phase_substitution_cannot_change_the_validated_latency_population() {
        var original = phase("original", 10000, 10000, 1000000000L, 1000000000L, rows(10000, 1, 2));
        var replacement = phase("replacement", 20000, 20000, 2000000000L, 2000000000L, rows(20000, 1000, 1001));
        var reads = new java.util.concurrent.atomic.AtomicInteger();
        var input = new java.util.AbstractList<BenchmarkReturnIntervalMetrics.Phase>() {
            @Override public int size() { return 1; }
            @Override public BenchmarkReturnIntervalMetrics.Phase get(int index) {
                return reads.getAndIncrement() == 0 ? original : replacement;
            }
        };
        var result = BenchmarkReturnIntervalMetrics.aggregate(input);
        assertThat(result.p99()).isEqualTo(new BenchmarkCausalClock.Interval(1, 2));
        assertThat(result.phases()).containsExactly("original");
        assertThat(result.fullCohortRows()).isEqualTo(10000);
        assertThat(reads).hasValue(1);
    }

    @Test void fork_rate_uses_summed_counts_over_summed_duration_without_averaging_phase_rates() {
        var result = BenchmarkReturnIntervalMetrics.aggregate(List.of(
                phase("one", 10000, 10000, 1000000000L, 1000000000L, rows(10000, 10, 11)),
                phase("two", 10000, 10000, 9000000000L, 9000000000L, rows(10000, 20, 21))));
        assertThat(result.completedLower()).isEqualTo(20000); assertThat(result.durationLower()).isEqualTo(10000000000L);
        assertThat(result.throughput().lowerRecordsPerSecond()).isLessThanOrEqualTo(2000);
        assertThat(result.throughput().upperRecordsPerSecond()).isGreaterThanOrEqualTo(2000).isLessThan(2001);
    }

    @Test void fork_p99_pools_the_original_rows_without_averaging_or_taking_the_maximum_phase_p99() {
        var small = new ArrayList<>(rows(10000, 1, 2));
        for (int index = 9800; index < 10000; index++) small.set(index, new BenchmarkCausalClock.Interval(1000, 1001));
        var result = BenchmarkReturnIntervalMetrics.aggregate(List.of(
                phase("small", 10000, 10000, 1000000000L, 1000000000L, small),
                phase("large", 90000, 90000, 9000000000L, 9000000000L, rows(90000, 1, 2))));
        assertThat(BenchmarkReturnTimeBounds.p99(small)).isEqualTo(new BenchmarkCausalClock.Interval(1000, 1001));
        assertThat(result.p99()).isEqualTo(new BenchmarkCausalClock.Interval(1, 2));
        assertThat(result.fullCohortRows()).isEqualTo(100000);
    }

    @Test void count_and_duration_uncertainty_are_enclosed_and_negative_latency_is_retained() {
        var negative = rows(12000, -1, 1);
        var first = phase("one", 10000, 11000, 1000000000L, 1100000000L, negative);
        var result = BenchmarkReturnIntervalMetrics.aggregate(List.of(first));
        assertThat(result.throughput().lowerRecordsPerSecond()).isLessThanOrEqualTo(10000.0 / 1.1);
        assertThat(result.throughput().upperRecordsPerSecond()).isGreaterThanOrEqualTo(11000);
        assertThat(result.p99()).isEqualTo(new BenchmarkCausalClock.Interval(-1, 1));
        assertThat(first.latencies().getFirst().lowerNanos()).isEqualTo(-1);
        assertThatThrownBy(() -> first.latencies().clear()).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void duplicate_phase_and_checked_duration_overflow_refuse_without_repairing_inputs() {
        var first = phase("one", 10000, 10000, 1000000000L, 1000000000L, rows(10000, 1, 2));
        assertThatThrownBy(() -> BenchmarkReturnIntervalMetrics.aggregate(List.of(first, first))).hasMessageContaining("duplicated");
        var huge = phase("huge", 10000, 10000, Long.MAX_VALUE, Long.MAX_VALUE, rows(10000, 1, 2));
        assertThatThrownBy(() -> BenchmarkReturnIntervalMetrics.aggregate(List.of(first, huge))).isInstanceOf(ArithmeticException.class);
    }

    @Test void phase_row_cap_refuses_before_copying_unbounded_input_and_pooled_cap_remains_distinct() {
        var reads = new java.util.concurrent.atomic.AtomicInteger();
        var oversized = new java.util.AbstractList<BenchmarkCausalClock.Interval>() {
            @Override public int size() { return 512 * 512 + 1; }
            @Override public BenchmarkCausalClock.Interval get(int index) {
                reads.incrementAndGet(); throw new AssertionError("unbounded row read before prior guard");
            }
        };
        assertThatThrownBy(() -> phase("oversized", 10000, 10000, 1, 1, oversized))
                .hasMessageContaining("phase latency roster");
        assertThat(reads).hasValue(0);
        var one = phase("one", 10000, 10000, 1, 1, rows(200000, 1, 2));
        var two = phase("two", 10000, 10000, 1, 1, rows(100000, 1, 2));
        assertThatThrownBy(() -> BenchmarkReturnIntervalMetrics.aggregate(List.of(one, two))).hasMessageContaining("pooled cohort");
    }

    private static BenchmarkReturnIntervalMetrics.Phase phase(String id, long cl, long cu, long dl, long du,
            List<BenchmarkCausalClock.Interval> rows) {
        return new BenchmarkReturnIntervalMetrics.Phase(id, cl, cu, dl, du, rows);
    }
    private static List<BenchmarkCausalClock.Interval> rows(int count, long lower, long upper) {
        return java.util.Collections.nCopies(count, new BenchmarkCausalClock.Interval(lower, upper));
    }
}
