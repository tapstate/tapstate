package io.tapstate.e2e;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BenchmarkForkEnvironmentScheduleTest {
    private static final long MS = 1_000_000L;

    @Test void timer_overshoot_does_not_accumulate_into_a_slower_offer() {
        long grid = 0, start = 0;
        for (int batch = 0; batch < 480; batch++) {
            // Each wake-up lands 2 ms after its scheduled start, as a coarse timer does.
            long issued = start + 2 * MS;
            grid += 80 * MS;
            start = BenchmarkForkEnvironment.nextBatchStart(grid, issued, 80 * MS);
        }
        assertThat(start).isEqualTo(480 * 80 * MS);
    }

    @Test void time_lost_to_a_slow_statement_is_repaid_at_no_more_than_five_fourths_of_the_rate() {
        // The batch due at 0 starts only at 300 ms; the next one follows it 64 ms later rather than at once.
        long next = BenchmarkForkEnvironment.nextBatchStart(80 * MS, 300 * MS, 80 * MS);
        assertThat(next).isEqualTo(364 * MS);
        // Batch one is due at 80 ms but starts at 364 ms; each later batch (instant SQL) repays 16 ms of debt.
        long grid = 80 * MS, start = next;
        int batches = 1;
        while (start > grid) {
            grid += 80 * MS;
            long following = BenchmarkForkEnvironment.nextBatchStart(grid, start, 80 * MS);
            assertThat(following - start).isGreaterThanOrEqualTo(64 * MS);
            start = following;
            batches++;
        }
        assertThat(batches).isEqualTo(19);
        assertThat(start).isEqualTo(grid);
    }

    @Test void an_unpaced_phase_issues_each_batch_as_soon_as_the_previous_one_returns() {
        assertThat(BenchmarkForkEnvironment.nextBatchStart(0, 7 * MS, 0)).isEqualTo(7 * MS);
    }
}
