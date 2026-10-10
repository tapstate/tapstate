package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnTimeBoundsTest {
    @Test void quantile_bounds_cover_values_that_take_opposite_ends_of_their_intervals() {
        List<BenchmarkCausalClock.Interval> values = IntStream.rangeClosed(1, 10_000)
                .mapToObj(value -> new BenchmarkCausalClock.Interval(value, value + 200L)).toList();
        assertThat(BenchmarkReturnTimeBounds.p99(values)).isEqualTo(new BenchmarkCausalClock.Interval(9_900, 10_100));
    }

    @Test void ninety_ninth_percentile_uses_the_frozen_nearest_rank_and_keeps_outliers() {
        List<BenchmarkCausalClock.Interval> values = new ArrayList<>();
        for (int i = 0; i < 9_899; i++) { values.add(new BenchmarkCausalClock.Interval(1, 2)); }
        for (int i = 0; i < 101; i++) { values.add(new BenchmarkCausalClock.Interval(100, 150)); }
        assertThat(BenchmarkReturnTimeBounds.p99(values)).isEqualTo(new BenchmarkCausalClock.Interval(100, 150));
        assertThatThrownBy(() -> BenchmarkReturnTimeBounds.p99(values.subList(0, 9_999)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("10000");
    }

    @Test void overlapping_return_bounds_cannot_manufacture_a_positive_rate_span() {
        var span = BenchmarkReturnTimeBounds.span(List.of(new BenchmarkCausalClock.Interval(100, 120),
                new BenchmarkCausalClock.Interval(110, 130)));
        assertThat(span).isEqualTo(new BenchmarkCausalClock.Interval(-10, 30));
        assertThatThrownBy(() -> BenchmarkReturnTimeBounds.throughput(10_000, span))
                .isInstanceOf(AssertionError.class).hasMessageContaining("positive duration");
    }

    @Test void throughput_retains_both_uncertainty_extremes() {
        var rate = BenchmarkReturnTimeBounds.throughput(10_000,
                new BenchmarkCausalClock.Interval(1_000_000_000, 2_000_000_000));
        assertThat(rate.lowerRecordsPerSecond()).isLessThanOrEqualTo(5_000).isGreaterThan(4_999.999999);
        assertThat(rate.upperRecordsPerSecond()).isGreaterThanOrEqualTo(10_000).isLessThan(10_000.000001);
        assertThatThrownBy(() -> BenchmarkReturnTimeBounds.throughput(0,
                new BenchmarkCausalClock.Interval(1, 2))).isInstanceOf(AssertionError.class);
    }

    @Test void rate_rounding_cannot_narrow_an_exact_large_integer_duration_bound() {
        long duration = 9_007_199_254_740_993L;
        var rate = BenchmarkReturnTimeBounds.throughput(192_000, new BenchmarkCausalClock.Interval(duration, duration + 1));
        var numerator = java.math.BigDecimal.valueOf(192_000_000_000_000L);
        assertThat(java.math.BigDecimal.valueOf(rate.lowerRecordsPerSecond())
                .multiply(java.math.BigDecimal.valueOf(duration + 1))).isLessThanOrEqualTo(numerator);
        assertThat(java.math.BigDecimal.valueOf(rate.upperRecordsPerSecond())
                .multiply(java.math.BigDecimal.valueOf(duration))).isGreaterThanOrEqualTo(numerator);
    }

    @Test void the_last_callback_exit_narrows_a_return_bound_without_becoming_a_physical_commit_point() {
        var owner = new BenchmarkCausalClock.Identity(17, 1000);
        var clock = new BenchmarkCausalClock(owner, List.of(
                new BenchmarkCausalClock.Sample(0, owner, 100, 110, 1000),
                new BenchmarkCausalClock.Sample(1, owner, 200, 210, 2000),
                new BenchmarkCausalClock.Sample(2, owner, 300, 310, 3000)));
        var target = BenchmarkWorkloadDefinitions.byId("copy").phase("cdc-update").targets().getFirst();
        var row = new BenchmarkWriteReturnExpectations.Association(target, "1", 0, 150, 160,
                1, 1, "pdk.state.bench_copy.sink", "orders", 1500, 2200, 2500, 2, true);
        var delivery = BenchmarkReturnTimeBounds.map(List.of(row), owner, clock).getFirst();
        assertThat(delivery.capturedPoint()).isEqualTo(new BenchmarkCausalClock.Interval(200, 310));
        assertThat(delivery.literalReturn()).isEqualTo(new BenchmarkCausalClock.Interval(200, 310));
        assertThat(delivery.latency()).isEqualTo(new BenchmarkCausalClock.Interval(50, 160));
        var uncertain = new BenchmarkWriteReturnExpectations.Association(target, "1", 0, 250, 260,
                1, 1, "pdk.state.bench_copy.sink", "orders", 1500, 2200, 2500, 2, true);
        assertThat(BenchmarkReturnTimeBounds.map(List.of(uncertain), owner, clock).getFirst().latency())
                .isEqualTo(new BenchmarkCausalClock.Interval(-50, 60));
    }

    @Test void a_wholly_pre_issue_return_and_a_reused_runtime_are_rejected() {
        var owner = new BenchmarkCausalClock.Identity(17, 1000);
        var clock = new BenchmarkCausalClock(owner, List.of(
                new BenchmarkCausalClock.Sample(0, owner, 100, 110, 1000),
                new BenchmarkCausalClock.Sample(1, owner, 200, 210, 2000)));
        var target = BenchmarkWorkloadDefinitions.byId("copy").phase("cdc-update").targets().getFirst();
        var row = new BenchmarkWriteReturnExpectations.Association(target, "1", 0, 300, 310,
                1, 1, "pdk.state.bench_copy.sink", "orders", 1200, 1300, 1500, 2, true);
        assertThatThrownBy(() -> BenchmarkReturnTimeBounds.map(List.of(row), owner, clock))
                .isInstanceOf(AssertionError.class).hasMessageContaining("source issue");
        assertThatThrownBy(() -> BenchmarkReturnTimeBounds.map(List.of(row),
                new BenchmarkCausalClock.Identity(17, 1001), clock)).isInstanceOf(AssertionError.class);
        var backwards = new BenchmarkWriteReturnExpectations.Association(target, "1", 0, 150, 160,
                1, 1, "pdk.state.bench_copy.sink", "orders", 1500, 1400, 1600, 2, true);
        assertThatThrownBy(() -> BenchmarkReturnTimeBounds.map(List.of(backwards), owner, clock))
                .isInstanceOf(AssertionError.class).hasMessageContaining("clock order");
    }
}
