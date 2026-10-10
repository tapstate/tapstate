package io.tapstate.e2e;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkNativeCounterBaselineTest {
    @Test void three_seconds_of_fresh_flat_observations_cannot_admit_a_cached_native_baseline() {
        var guard = new BenchmarkNativeCounterBaseline(0, Duration.ofSeconds(3));
        assertThat(guard.observe(snapshot(5000, 10, 100), 100, 20)).isFalse();
        assertThat(guard.observe(snapshot(5000, 3_000_000_030L, 100), 100, 3_000_000_040L)).isFalse();
    }

    @Test void two_successor_publications_and_quiet_corresponding_totals_are_required() {
        var guard = new BenchmarkNativeCounterBaseline(0, Duration.ofSeconds(3));
        assertThat(guard.observe(snapshot(5000, 10, 100), 100, 20)).isFalse();
        assertThat(guard.observe(snapshot(10000, 5_000_000_000L, 100), 100, 5_000_000_010L)).isFalse();
        assertThat(guard.observe(snapshot(15000, 10_000_000_000L, 100), 100, 10_000_000_010L)).isTrue();
        assertThat(guard.total()).isEqualTo(100);
        assertThat(guard.evidence()).containsEntry("successorPublications", 2)
                .containsEntry("state", "RECORDED_FRESH_NATIVE_BASELINE").containsEntry("performanceAcceptanceEligible", false);
        assertThatThrownBy(() -> guard.observe(snapshot(20000, 15_000_000_000L, 100), 100, 15_000_000_010L))
                .isInstanceOf(AssertionError.class).hasMessageContaining("no longer open");
    }

    @Test void delayed_flat_publication_must_match_the_full_native_vector_and_then_settle() {
        var guard = new BenchmarkNativeCounterBaseline(0, Duration.ofSeconds(3));
        guard.observe(snapshot(5000, 10, 100), 100, 20);
        assertThat(guard.observe(snapshot(10000, 5_000_000_000L, 110), 100, 5_000_000_010L)).isFalse();
        assertThat(guard.observe(snapshot(15000, 10_000_000_000L, 110), 100, 10_000_000_010L)).isFalse();
        assertThat(guard.observe(snapshot(15000, 11_000_000_000L, 110), 110, 11_000_000_010L)).isFalse();
        assertThat(guard.observe(snapshot(15000, 14_000_000_000L, 110), 110, 14_000_000_010L)).isTrue();
        assertThat(guard.total()).isEqualTo(110);
    }

    @Test void publication_rollback_and_mutated_cached_vectors_are_sticky_failures() {
        for (boolean rollback : new boolean[]{true, false}) {
            var guard = new BenchmarkNativeCounterBaseline(0, Duration.ofSeconds(3));
            guard.observe(snapshot(5000, 10, 100), 100, 20);
            assertThatThrownBy(() -> guard.observe(snapshot(rollback ? 4999 : 5000, 4_000_000_000L, rollback ? 100 : 110),
                    100, 4_000_000_010L)).isInstanceOf(AssertionError.class);
            assertThat(guard.evidence()).containsEntry("state", "UNKNOWN");
            assertThatThrownBy(() -> guard.observe(snapshot(15000, 9_000_000_000L, 110), 110, 9_000_000_010L))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("no longer open");
        }
    }

    @Test void changed_execution_rosters_and_pre_ack_or_overlapping_requests_are_rejected() {
        for (int change = 0; change < 4; change++) {
            var guard = new BenchmarkNativeCounterBaseline(0, Duration.ofSeconds(3));
            guard.observe(snapshot(5000, 10, 100), 100, 20);
            var changed = new BenchmarkNativeCounterBaseline.Snapshot("pipeline", change == 0 ? "other-job" : "job",
                    "member", change == 1 ? "other-execution" : "execution", 10000, change == 2 ? 19 : 100,
                    101, change == 3 ? Set.of("member/serve.sink/1") : Set.of("member/serve.sink/0"),
                    Map.of("recordsOut.UPDATE.orders|member/serve.sink/0", 100L));
            assertThatThrownBy(() -> guard.observe(changed, 100, 110)).isInstanceOf(AssertionError.class);
        }
        var before = new BenchmarkNativeCounterBaseline(100, Duration.ofSeconds(3));
        assertThatThrownBy(() -> before.observe(snapshot(5000, 99, 100), 100, 110))
                .isInstanceOf(AssertionError.class).hasMessageContaining("precedes");
    }

    @Test void counter_decreases_and_unproven_baseline_access_are_refused() {
        var guard = new BenchmarkNativeCounterBaseline(0, Duration.ofSeconds(3));
        guard.observe(snapshot(5000, 10, 100), 100, 20);
        assertThatThrownBy(guard::total).isInstanceOf(AssertionError.class).hasMessageContaining("not been admitted");
        assertThatThrownBy(() -> guard.observe(snapshot(10000, 100, 99), 100, 110))
                .isInstanceOf(AssertionError.class).hasMessageContaining("settled counter moved backward");
        assertThatThrownBy(() -> new BenchmarkNativeCounterBaseline.Snapshot("pipeline", "job", "member", "execution",
                5000, 10, 11, Set.of("member/serve.sink/0"), Map.of("one", Long.MAX_VALUE, "two", 1L)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("overflow");
    }

    @Test void unavailable_successor_cycles_cannot_poll_past_the_bounded_capacity() {
        var guard = new BenchmarkNativeCounterBaseline(0, Duration.ofSeconds(3));
        for (int i = 0; i < 512; i++) { assertThat(guard.observe(snapshot(5000, i * 10L, 100), 100, i * 10L + 2)).isFalse(); }
        assertThatThrownBy(() -> guard.observe(snapshot(5000, 5120, 100), 100, 5122))
                .isInstanceOf(AssertionError.class).hasMessageContaining("capacity");
    }

    @Test void a_phase_pair_binds_execution_and_existing_cells_while_allowing_new_operation_series() {
        var before = snapshot(5000, 10, 100);
        var after = new BenchmarkNativeCounterBaseline.Snapshot("pipeline", "job", "member", "execution", 15000,
                100, 101, before.expectedSinkIdentities(), Map.of("recordsOut.UPDATE.orders|member/serve.sink/0", 120L,
                        "recordsOut.INSERT.orders|member/serve.sink/0", 30L));
        assertThat(BenchmarkNativeCounterBaseline.delta(before, after)).isEqualTo(50);
        var other = new BenchmarkNativeCounterBaseline.Snapshot(after.pipeline(), after.jobId(), after.memberUuid(),
                "other-execution", after.publicationStamp(), after.readStartedAtNanos(), after.readCompletedAtNanos(),
                after.expectedSinkIdentities(), after.counters());
        assertThatThrownBy(() -> BenchmarkNativeCounterBaseline.delta(before, other))
                .isInstanceOf(AssertionError.class).hasMessageContaining("changed execution");
        assertThatThrownBy(() -> BenchmarkNativeCounterBaseline.delta(before, snapshot(15000, 100, 99)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("lost a settled counter");
    }

    @Test void admitted_baseline_evidence_is_serializable_by_the_actual_diagnostic_json_writer() {
        var guard = new BenchmarkNativeCounterBaseline(0, Duration.ofSeconds(3));
        guard.observe(snapshot(5000, 10, 100), 100, 20);
        guard.observe(snapshot(10000, 5_000_000_000L, 100), 100, 5_000_000_010L);
        assertThat(guard.observe(snapshot(15000, 10_000_000_000L, 100), 100, 10_000_000_010L)).isTrue();
        String json = io.tapstate.core.common.JsonWriter.write(guard.evidence());
        assertThat(json).contains("RECORDED_FRESH_NATIVE_BASELINE", "member/serve.sink/0", "recordsOut.UPDATE.orders");
        assertThat(io.tapstate.core.common.JsonReader.parse(json)).isInstanceOf(Map.class);
    }

    private static BenchmarkNativeCounterBaseline.Snapshot snapshot(long publication, long read, long count) {
        return new BenchmarkNativeCounterBaseline.Snapshot("pipeline", "job", "member", "execution", publication,
                read, read + 1, Set.of("member/serve.sink/0"), Map.of("recordsOut.UPDATE.orders|member/serve.sink/0", count));
    }
}
