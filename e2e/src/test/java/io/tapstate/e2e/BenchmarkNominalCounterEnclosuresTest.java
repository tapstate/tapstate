package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkNominalCounterEnclosuresTest {
    @Test void fractional_counter_value_needs_a_closed_upper_successor() {
        long ticks = 1, scaledExact = ticks * 125, floor = scaledExact / 3;
        assertThat(floor * 3).isLessThan(scaledExact);
        var bounds = BenchmarkNominalCounterEnclosures.point(floor);
        assertThat(bounds.lowerNanos() * 3).isLessThanOrEqualTo(scaledExact);
        assertThat(bounds.upperNanos() * 3).isGreaterThanOrEqualTo(scaledExact);
        assertThat(bounds.lowerNanos()).isEqualTo(41);
        assertThat(bounds.upperNanos()).isEqualTo(42);
    }

    @Test void all_conversion_residues_and_admissible_same_call_returns_are_enclosed() {
        for (long source = 0; source < 6; source++) {
            for (long callback = source; callback < 10; callback++) {
                for (long returned = callback; returned < 14; returned++) {
                    for (long observed = returned; observed <= returned + 3; observed++) {
                        var absolute = BenchmarkNominalCounterEnclosures.literalReturn(floor(callback), floor(observed));
                        var latency = BenchmarkNominalCounterEnclosures.sourceLatency(floor(source), floor(callback), floor(observed));
                        assertThat(absolute.lowerNanos() * 3).isLessThanOrEqualTo(returned * 125);
                        assertThat(absolute.upperNanos() * 3).isGreaterThanOrEqualTo(returned * 125);
                        long scaledLatency = (returned - source) * 125;
                        assertThat(latency.lowerNanos() * 3).isLessThanOrEqualTo(scaledLatency);
                        assertThat(latency.upperNanos() * 3).isGreaterThanOrEqualTo(scaledLatency);
                    }
                }
            }
        }
    }

    @Test void source_floor_uncertainty_expands_the_lower_latency_bound() {
        var bounds = BenchmarkNominalCounterEnclosures.sourceLatency(41, 83, 83);
        assertThat(bounds.lowerNanos()).isEqualTo(41);
        assertThat(bounds.upperNanos()).isEqualTo(43);
        assertThat(bounds.lowerNanos() * 3).isLessThanOrEqualTo(125);
        assertThat(bounds.upperNanos() * 3).isGreaterThanOrEqualTo(125);
    }

    @Test void equal_integer_points_retain_fractional_and_zero_latency_possibilities() {
        var bounds = BenchmarkNominalCounterEnclosures.sourceLatency(0, 0, 0);
        assertThat(bounds.lowerNanos()).isEqualTo(-1);
        assertThat(bounds.upperNanos()).isEqualTo(1);
        assertThat(BenchmarkNominalCounterEnclosures.literalReturn(0, 0).widthNanos()).isEqualTo(1);
    }

    @Test void reversed_negative_and_unrepresentable_enclosures_are_refused() {
        assertThatThrownBy(() -> BenchmarkNominalCounterEnclosures.literalReturn(2, 1)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> BenchmarkNominalCounterEnclosures.point(-1)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> BenchmarkNominalCounterEnclosures.sourceLatency(-1, 0, 0)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> BenchmarkNominalCounterEnclosures.point(Long.MAX_VALUE))
                .isInstanceOf(AssertionError.class).hasMessageContaining("overflow");
        assertThatThrownBy(() -> BenchmarkNominalCounterEnclosures.literalReturn(0, Long.MAX_VALUE))
                .isInstanceOf(AssertionError.class).hasMessageContaining("overflow");
    }

    private static long floor(long ticks) { return ticks * 125 / 3; }
}
