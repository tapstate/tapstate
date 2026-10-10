package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class BenchmarkReturnResourceBoundsTest {
    @Test void inner_and_outer_actual_reads_enclose_reported_counters_without_replacing_full_phase_receipts() {
        var full = summary(readings(), 0, 0);
        var evidence = BenchmarkReturnResourceBounds.evidence(full, cohort(15, 35, 65, 95));
        assertThat(evidence).containsEntry("state", "RECORDED_COUNTER_ENCLOSURE")
                .containsEntry("cpuNanosLower", 150L).containsEntry("cpuNanosUpper", 600L)
                .containsEntry("gcCollectionMillisLower", 15L).containsEntry("gcCollectionMillisUpper", 90L)
                .containsEntry("innerReadCount", 2).containsEntry("possibleWindowReadCount", 4)
                .containsEntry("samplingCostQualified", false).containsEntry("performanceAcceptanceEligible", false);
        assertThat(evidence.get("startBoundsNanos")).isEqualTo(List.of(15L, 35L));
        assertThat(evidence.get("endBoundsNanos")).isEqualTo(List.of(65L, 95L));
        assertThat(evidence.get("outerFirstRead")).isEqualTo(java.util.Map.of("index", 1L, "startedAtNanos", 0L, "completedAtNanos", 10L));
        assertThat(evidence.get("outerLastRead")).isEqualTo(java.util.Map.of("index", 6L, "startedAtNanos", 100L, "completedAtNanos", 110L));
        assertThat(full.cpuNanos()).isEqualTo(600); assertThat(full.gcCollectionMillis()).isEqualTo(90);
        assertThat(full.sampleCount()).isEqualTo(6);
        assertThat(evidence).containsEntry("resourceReadTotalDurationNanos", 50L)
                .containsEntry("resourceReadMaximumDurationNanos", 10L);
    }

    @Test void overlapping_endpoint_uncertainty_has_a_proved_zero_lower_bound_without_inventing_zero_usage() {
        var evidence = BenchmarkReturnResourceBounds.evidence(summary(readings(), 0, 0), cohort(15, 85, 25, 95));
        assertThat(evidence).containsEntry("state", "RECORDED_COUNTER_ENCLOSURE")
                .containsEntry("cpuNanosLower", 0L).containsEntry("cpuNanosUpper", 600L)
                .containsEntry("gcCollectionMillisLower", 0L).containsEntry("gcCollectionMillisUpper", 90L)
                .containsEntry("innerReadCount", 0);
        assertThat(evidence.get("reason").toString()).contains("FOLLOWS_MONOTONICITY");
        assertThat(evidence).doesNotContainKeys("innerSampledHeapPeakBytes", "innerSampledRssPeakBytes");
    }

    @Test void sampled_memory_values_cannot_be_promoted_to_continuous_peak_enclosures() {
        var evidence = BenchmarkReturnResourceBounds.evidence(summary(readings(), 0, 0), cohort(15, 35, 65, 95));
        assertThat(evidence).containsEntry("innerSampledHeapPeakBytes", 950L)
                .containsEntry("innerSampledRssPeakBytes", 1100L)
                .containsEntry("possibleWindowSampledHeapPeakBytes", 10_000L)
                .containsEntry("possibleWindowSampledRssPeakBytes", 20_000L)
                .containsEntry("memoryScope", "ACTUAL_SAMPLED_VALUES_ONLY_NO_CONTINUOUS_PEAK_BOUND")
                .containsEntry("ownedIdentityProof", "CALLER_OWNED_RESOURCE_CONNECTION");
        assertThat(evidence).doesNotContainKeys("continuousHeapPeakUpper", "continuousRssPeakUpper", "samplingCostPercent");
    }

    @Test void missing_outer_reads_never_turn_an_interior_slice_into_complete_window_cost() {
        List<BenchmarkResourceSampler.Attempt> middle = new ArrayList<>();
        for (var attempt : readings().subList(1, 5)) {
            middle.add(new BenchmarkResourceSampler.Attempt(middle.size() + 1L, attempt.startedAtNanos(),
                    attempt.completedAtNanos(), attempt.outcome(), attempt.failureType(), attempt.reading()));
        }
        var evidence = BenchmarkReturnResourceBounds.evidence(summary(middle, 0, 0), cohort(15, 35, 65, 95));
        assertThat(evidence).containsEntry("state", "UNKNOWN").containsEntry("reason", "OUTER_RESOURCE_READ_COVERAGE_MISSING")
                .doesNotContainKeys("cpuNanosLower", "cpuNanosUpper", "gcCollectionMillisLower", "gcCollectionMillisUpper");
    }

    @Test void a_hidden_middle_counter_regression_is_not_excused_by_positive_full_phase_totals() {
        for (boolean gc : List.of(false, true)) {
            var attempts = new ArrayList<>(readings());
            attempts.set(2, attempt(3, 40, 45, gc ? 250 : 190, gc ? 19 : 35, 900, 1000));
            var full = summary(attempts, 0, 0);
            assertThat(full.cpuNanos()).isPositive(); assertThat(full.gcCollectionMillis()).isPositive();
            assertThat(BenchmarkReturnResourceBounds.evidence(full, cohort(15, 35, 65, 95)))
                    .containsEntry("state", "UNKNOWN").containsEntry("reason", "CUMULATIVE_RESOURCE_COUNTER_MOVED_BACKWARD")
                    .doesNotContainKey("cpuNanosUpper");
        }
    }

    @Test void failed_or_omitted_attempts_do_not_receive_numeric_enclosures() {
        for (boolean omitted : List.of(false, true)) {
            var full = summary(readings(), omitted ? 1 : 0, omitted ? 0 : 1);
            assertThat(BenchmarkReturnResourceBounds.evidence(full, cohort(15, 35, 65, 95)))
                    .containsEntry("state", "UNKNOWN").containsEntry("reason", "RESOURCE_TRACE_HAS_OMITTED_OR_FAILED_READS")
                    .doesNotContainKeys("cpuNanosLower", "cpuNanosUpper");
        }
    }

    @Test void summary_mismatches_serial_read_errors_and_missing_inputs_remain_unknown() {
        var full = summary(readings(), 0, 0);
        var forged = new BenchmarkResourceSampler.Summary(full.cpuNanos() + 1, full.gcCollectionMillis(),
                full.peakHeapBytes(), full.peakRssBytes(), full.sampleCount(), full.sampling());
        assertThat(BenchmarkReturnResourceBounds.evidence(forged, cohort(15, 35, 65, 95)))
                .containsEntry("reason", "FULL_RESOURCE_SUMMARY_CONTRADICTS_TRACE");
        var overlap = new ArrayList<>(readings()); overlap.set(3, attempt(4, 44, 60, 400, 50, 950, 1100));
        assertThat(BenchmarkReturnResourceBounds.evidence(summary(overlap, 0, 0), cohort(15, 35, 65, 95)))
                .containsEntry("reason", "RESOURCE_READ_BRACKETS_NOT_SERIAL");
        assertThat(BenchmarkReturnResourceBounds.evidence(full, List.of()))
                .containsEntry("reason", "COMPLETE_COHORT_UNAVAILABLE");
        assertThat(BenchmarkReturnResourceBounds.evidence(new BenchmarkResourceSampler.Summary(600, 90, 10_000, 20_000, 6),
                cohort(15, 35, 65, 95))).containsEntry("reason", "FULL_RESOURCE_TRACE_UNAVAILABLE");
    }

    @Test void incomplete_or_failed_individual_snapshots_cannot_be_hidden_by_trace_flags() {
        var baseline = summary(readings(), 0, 0);
        for (boolean failed : List.of(false, true)) {
            var attempts = new ArrayList<>(readings());
            var incomplete = new BenchmarkProcessProbe.Snapshot(OptionalLong.of(250), OptionalLong.empty(),
                    OptionalLong.of(1000), OptionalLong.of(35));
            attempts.set(2, new BenchmarkResourceSampler.Attempt(3, 40, 45,
                    failed ? BenchmarkResourceSampler.Outcome.ERROR : BenchmarkResourceSampler.Outcome.SUCCESS,
                    failed ? "CONTROLLED_READ_ERROR" : null, incomplete));
            var old = baseline.sampling().orElseThrow();
            var trace = new BenchmarkResourceSampler.SamplingDiagnostics("COMPLETE", 6, 0, old.totalDurationNanos(),
                    old.maxDurationNanos(), 0, attempts);
            var full = new BenchmarkResourceSampler.Summary(600, 90, 10_000, 20_000, 6, Optional.of(trace));
            assertThat(BenchmarkReturnResourceBounds.evidence(full, cohort(15, 35, 65, 95)))
                    .containsEntry("state", "UNKNOWN").containsEntry("reason", "RESOURCE_READ_SHAPE_UNAVAILABLE")
                    .doesNotContainKeys("cpuNanosLower", "cpuNanosUpper");
        }
    }

    private static List<BenchmarkReturnTimeBounds.Delivery> cohort(long startLower, long startUpper, long endLower, long endUpper) {
        var first = new BenchmarkCausalClock.Interval(startLower, startUpper);
        var last = new BenchmarkCausalClock.Interval(endLower, endUpper);
        return List.of(new BenchmarkReturnTimeBounds.Delivery("target", "first", 1, first, first, first),
                new BenchmarkReturnTimeBounds.Delivery("target", "last", 2, last, last, last));
    }
    private static List<BenchmarkResourceSampler.Attempt> readings() {
        return List.of(attempt(1, 0, 10, 100, 10, 300, 400), attempt(2, 20, 30, 200, 20, 10_000, 20_000),
                attempt(3, 40, 45, 250, 35, 900, 1000), attempt(4, 55, 60, 400, 50, 950, 1100),
                attempt(5, 80, 90, 600, 80, 800, 900), attempt(6, 100, 110, 700, 100, 500, 600));
    }
    private static BenchmarkResourceSampler.Attempt attempt(long index, long before, long after, long cpu, long gc, long heap, long rss) {
        return new BenchmarkResourceSampler.Attempt(index, before, after, BenchmarkResourceSampler.Outcome.SUCCESS, null,
                new BenchmarkProcessProbe.Snapshot(OptionalLong.of(cpu), OptionalLong.of(heap), OptionalLong.of(rss), OptionalLong.of(gc)));
    }
    private static BenchmarkResourceSampler.Summary summary(List<BenchmarkResourceSampler.Attempt> attempts, long omitted, long failures) {
        var totals = BenchmarkResourceSampler.summarize(attempts.stream().map(BenchmarkResourceSampler.Attempt::reading).toList());
        long duration = attempts.stream().mapToLong(BenchmarkResourceSampler.Attempt::durationNanos).sum();
        long maximum = attempts.stream().mapToLong(BenchmarkResourceSampler.Attempt::durationNanos).max().orElseThrow();
        var trace = new BenchmarkResourceSampler.SamplingDiagnostics(failures == 0 ? "COMPLETE" : "FAILED",
                attempts.size() + omitted, failures, duration, maximum, omitted, attempts);
        return new BenchmarkResourceSampler.Summary(totals.cpuNanos(), totals.gcCollectionMillis(), totals.peakHeapBytes(),
                totals.peakRssBytes(), totals.sampleCount(), Optional.of(trace));
    }
}
