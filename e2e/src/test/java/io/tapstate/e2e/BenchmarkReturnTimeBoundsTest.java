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

    @Test void nominal_return_and_source_uncertainty_keep_each_rows_full_call_weight_and_fractional_true_values() {
        var owner = new BenchmarkCausalClock.Identity(17, 1000);
        var clock = nominalClock(owner);
        var target = BenchmarkWorkloadDefinitions.byId("copy").phase("cdc-update").targets().getFirst();
        var one = new BenchmarkWriteReturnExpectations.Association(target, "1", 1, 83, 125,
                7, 1, "pdk.state.bench_copy.sink", "orders", 83, 125, 166, 2, true);
        var two = new BenchmarkWriteReturnExpectations.Association(target, "2", 0, 41, 83,
                7, 1, "pdk.state.bench_copy.sink", "orders", 83, 125, 166, 2, true);
        var mapped = BenchmarkReturnTimeBounds.map(List.of(one, two), owner, clock);
        assertThat(mapped).hasSize(2);
        assertThat(mapped.stream().map(BenchmarkReturnTimeBounds.Delivery::callSequence).toList()).containsExactly(7L, 7L);
        assertThat(mapped.get(0).capturedPoint()).isEqualTo(new BenchmarkCausalClock.Interval(166, 167));
        assertThat(mapped.get(0).literalReturn()).isEqualTo(new BenchmarkCausalClock.Interval(125, 167));
        assertThat(mapped.get(1).literalReturn()).isEqualTo(mapped.get(0).literalReturn());
        assertThat(mapped.get(0).latency()).isEqualTo(new BenchmarkCausalClock.Interval(41, 84));
        assertThat(mapped.get(1).latency()).isEqualTo(new BenchmarkCausalClock.Interval(83, 126));
        // Source ticks 2 and 1 and return ticks 3 or 4 have exact 125/3 nominal coordinates.
        // Difference-of-floors alone excludes the 125/3 lower latency for the first row.
        for (int row = 0; row < mapped.size(); row++) {
            long sourceTicks = row == 0 ? 2 : 1;
            for (long returnTicks : new long[]{3, 4}) {
                var numerator = java.math.BigInteger.valueOf((returnTicks - sourceTicks) * 125);
                var bounds = mapped.get(row).latency();
                assertThat(java.math.BigInteger.valueOf(bounds.lowerNanos()).multiply(java.math.BigInteger.valueOf(3)))
                        .isLessThanOrEqualTo(numerator);
                assertThat(java.math.BigInteger.valueOf(bounds.upperNanos()).multiply(java.math.BigInteger.valueOf(3)))
                        .isGreaterThanOrEqualTo(numerator);
            }
        }
        assertThat(java.math.BigInteger.valueOf(42).multiply(java.math.BigInteger.valueOf(3)))
                .isGreaterThan(java.math.BigInteger.valueOf(125));
    }

    @Test void nominal_source_bounds_preserve_negative_uncertainty_and_still_refuse_wholly_pre_issue_returns() {
        var owner = new BenchmarkCausalClock.Identity(17, 1000);
        var target = BenchmarkWorkloadDefinitions.byId("copy").phase("cdc-update").targets().getFirst();
        var uncertain = new BenchmarkWriteReturnExpectations.Association(target, "1", 0, 83, 83,
                1, 1, "pdk.state.bench_copy.sink", "orders", 83, 83, 83, 1, true);
        assertThat(BenchmarkReturnTimeBounds.map(List.of(uncertain), owner, nominalClock(owner)).getFirst().latency())
                .isEqualTo(new BenchmarkCausalClock.Interval(-1, 1));
        var tooEarly = new BenchmarkWriteReturnExpectations.Association(target, "1", 0, 200, 208,
                1, 1, "pdk.state.bench_copy.sink", "orders", 83, 125, 166, 1, true);
        assertThatThrownBy(() -> BenchmarkReturnTimeBounds.map(List.of(tooEarly), owner, nominalClock(owner)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("source issue");
    }

    private static BenchmarkReturnPointClock nominalClock(BenchmarkCausalClock.Identity owner) {
        return new BenchmarkReturnPointClock() {
            @Override public BenchmarkCausalClock.Interval map(BenchmarkCausalClock.Identity identity, long pointNanos) {
                if (!owner.equals(identity)) { throw new AssertionError("nominal control requires its owned identity"); }
                return new BenchmarkCausalClock.Interval(pointNanos, Math.addExact(pointNanos, 1));
            }
            @Override public BenchmarkCausalClock.Interval sourcePoint(long pointNanos) {
                return new BenchmarkCausalClock.Interval(pointNanos, Math.addExact(pointNanos, 1));
            }
        };
    }
}
