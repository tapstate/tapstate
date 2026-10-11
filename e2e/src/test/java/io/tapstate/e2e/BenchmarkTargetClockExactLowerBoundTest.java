package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkTargetClockExactLowerBoundTest {
    @Test void a_fractional_lower_bound_cannot_accept_an_integer_date_outside_two_milliseconds() {
        var before = reading(1000, 1_000_000_000L, 1_001_000_000L);
        var after = reading(1099, 1_102_100_000L, 1_103_100_000L);
        // The exact elapsed lower bound is101.1ms, so99ms is below its99.1ms tolerated boundary.
        assertThatThrownBy(() -> BenchmarkTargetClock.validate(before, after))
                .isInstanceOf(AssertionError.class).hasMessageContaining("clock stepped");
    }

    @Test void the_first_integer_inside_the_exact_lower_boundary_keeps_the_original_tolerance() {
        var before = reading(1000, 1_000_000_000L, 1_001_000_000L);
        var after = reading(1100, 1_102_100_000L, 1_103_100_000L);
        assertThat(BenchmarkTargetClock.validate(before, after))
                .containsEntry("state", "QUALIFIED").containsEntry("endpointResolutionErrorMillis", 2);
    }

    @Test void a_fractional_upper_boundary_never_uses_ceiling_to_accept_the_recorded_refusal() {
        var before = new BenchmarkTargetClock.Reading("owned:27017", "same", 1791754866975L,
                914149522441625L, 914149523234000L, 1791754866978L, 1791754866979L);
        var after = new BenchmarkTargetClock.Reading("owned:27017", "same", 1791754874531L,
                914157075305208L, 914157076271500L, 1791754874531L, 1791754874532L);
        assertThatThrownBy(() -> BenchmarkTargetClock.validate(before, after))
                .isInstanceOf(AssertionError.class).hasMessageContaining("clock stepped")
                .hasMessageContaining("maximumElapsedMillis=7553");
    }

    private static BenchmarkTargetClock.Reading reading(long wall, long start, long end) {
        return new BenchmarkTargetClock.Reading("owned:27017", "same", wall, start, end, wall, wall);
    }
}
