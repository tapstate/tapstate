package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BenchmarkForkEnvironmentScheduleTest {
    private static final long MS = 1_000_000L;

    @Test void timer_overshoot_does_not_accumulate_into_a_slower_offer() {
        long scheduled = 0;
        for (int batch = 0; batch < 480; batch++) {
            // Each wake-up lands 2 ms after its scheduled start, as a coarse timer does.
            scheduled = BenchmarkForkEnvironment.nextBatchStart(scheduled, scheduled + 2 * MS, 80 * MS);
        }
        assertThat(scheduled).isEqualTo(480 * 80 * MS);
    }

    @Test void time_lost_to_a_slow_statement_is_not_repaid_in_a_burst() {
        // The batch scheduled at 0 starts at 300 ms: the grid re-anchors half an interval after it.
        long next = BenchmarkForkEnvironment.nextBatchStart(0, 300 * MS, 80 * MS);
        assertThat(next).isEqualTo(340 * MS);
        long after = BenchmarkForkEnvironment.nextBatchStart(next, 340 * MS, 80 * MS);
        assertThat(after).isEqualTo(420 * MS);
    }

    @Test void an_unpaced_phase_issues_each_batch_as_soon_as_the_previous_one_returns() {
        assertThat(BenchmarkForkEnvironment.nextBatchStart(0, 7 * MS, 0)).isEqualTo(7 * MS);
    }
}
