package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnCommonWindowTest {
    @Test void an_uncertain_latest_call_is_included_at_its_own_closed_common_end() {
        var rows = new ArrayList<BenchmarkReturnTimeBounds.Delivery>();
        add(rows, "target", 1, 0, 0, 1); add(rows, "target", 2, 90, 100, 7);
        var certificate = BenchmarkReturnCommonWindow.evidence(rows);
        assertThat(certificate).containsEntry("completedRowsLower", 7L).containsEntry("completedRowsUpper", 7L);
        assertThat(binCounts(certificate, "lowerRows").getLast()).isEqualTo(7L);
    }

    @Test void a_wide_latest_call_interval_stays_in_the_final_bin_through_its_shared_end_variable() {
        var rows = new ArrayList<BenchmarkReturnTimeBounds.Delivery>();
        add(rows, "target", 1, 0, 0, 1); add(rows, "target", 2, 1, 100, 7);
        var certificate = BenchmarkReturnCommonWindow.evidence(rows);
        assertThat(certificate).containsEntry("completedRowsLower", 7L).containsEntry("completedRowsUpper", 7L);
        assertThat(binCounts(certificate, "lowerRows").getLast()).isEqualTo(7L);
    }

    @Test void uncertain_closed_end_pairs_preserve_every_balanced_bin_and_the_original_half_trend() {
        var rows = balanced(0, 10_000, 10_000);
        for (int index = 0; index < rows.size(); index++) {
            var delivery = rows.get(index);
            if (delivery.literalReturn().lowerNanos() == 1000) {
                rows.set(index, row(delivery.target(), delivery.key(), delivery.callSequence(), 1000, 1001));
            }
        }
        var certificate = BenchmarkReturnCommonWindow.evidence(rows);
        assertThat(certificate).containsEntry("completedRowsLower", 20_000L).containsEntry("completedRowsUpper", 20_000L)
                .containsEntry("state", "PASS").containsEntry("earlyRowsLower", 10_000L).containsEntry("earlyRowsUpper", 10_000L)
                .containsEntry("lateRowsLower", 10_000L).containsEntry("lateRowsUpper", 10_000L);
        assertThat(binCounts(certificate, "lowerRows")).containsExactly(2000L, 2000L, 2000L, 2000L, 2000L,
                2000L, 2000L, 2000L, 2000L, 2000L);
        assertThat(binCounts(certificate, "upperRows")).isEqualTo(binCounts(certificate, "lowerRows"));
    }

    @Test void a_latest_own_call_after_the_other_target_end_is_excluded_from_the_common_window() {
        var rows = new ArrayList<BenchmarkReturnTimeBounds.Delivery>();
        add(rows, "one", 1, 0, 0, 1); add(rows, "one", 2, 90, 100, 7);
        add(rows, "two", 3, 0, 0, 1); add(rows, "two", 4, 50, 60, 5);
        var certificate = BenchmarkReturnCommonWindow.evidence(rows);
        assertThat(certificate).containsEntry("completedRowsLower", 5L).containsEntry("completedRowsUpper", 5L);
        assertThat(binCounts(certificate, "lowerRows").getLast()).isEqualTo(5L);
        assertThat(binCounts(certificate, "upperRows").getLast()).isEqualTo(5L);
    }

    @Test void shared_call_assignments_are_enclosed_for_one_and_two_target_membership_bins_and_halves() {
        List<List<CallRange>> scenarios = List.of(
                List.of(new CallRange("one", 0, 1, 2), new CallRange("one", 2, 3, 3), new CallRange("one", 8, 10, 5)),
                List.of(new CallRange("one", 0, 2, 1), new CallRange("one", 1, 3, 2), new CallRange("one", 2, 4, 3)),
                List.of(new CallRange("one", 0, 1, 1), new CallRange("one", 8, 10, 3),
                        new CallRange("two", 2, 3, 2), new CallRange("two", 6, 7, 4)),
                List.of(new CallRange("one", 0, 1, 1), new CallRange("one", 5, 7, 3),
                        new CallRange("two", 0, 2, 2), new CallRange("two", 6, 8, 4)),
                List.of(new CallRange("one", 0, 1, 1), new CallRange("one", 1, 2, 2), new CallRange("one", 3, 4, 3),
                        new CallRange("two", 0, 2, 4), new CallRange("two", 4, 5, 5)));
        for (var scenario : scenarios) {
            var rows = new ArrayList<BenchmarkReturnTimeBounds.Delivery>();
            for (int index = 0; index < scenario.size(); index++) {
                var call = scenario.get(index);
                add(rows, call.target(), index + 1L, call.lower(), call.upper(), call.weight());
            }
            var certificate = BenchmarkReturnCommonWindow.evidence(rows);
            var truth = new AssignmentBounds();
            enumerate(scenario, 0, new long[scenario.size()], truth);
            assertThat((Long) certificate.get("completedRowsLower")).isLessThanOrEqualTo(truth.minimum[0]);
            assertThat((Long) certificate.get("completedRowsUpper")).isGreaterThanOrEqualTo(truth.maximum[0]);
            assertThat((Long) certificate.get("earlyRowsLower")).isLessThanOrEqualTo(truth.minimum[1]);
            assertThat((Long) certificate.get("earlyRowsUpper")).isGreaterThanOrEqualTo(truth.maximum[1]);
            assertThat((Long) certificate.get("lateRowsLower")).isLessThanOrEqualTo(truth.minimum[2]);
            assertThat((Long) certificate.get("lateRowsUpper")).isGreaterThanOrEqualTo(truth.maximum[2]);
            for (int bin = 0; bin < 10; bin++) {
                assertThat(binCounts(certificate, "lowerRows").get(bin)).isLessThanOrEqualTo(truth.minimum[bin + 3]);
                assertThat(binCounts(certificate, "upperRows").get(bin)).isGreaterThanOrEqualTo(truth.maximum[bin + 3]);
            }
            assertThat(certificate).containsEntry("performanceAcceptanceEligible", false);
            if ("PASS".equals(certificate.get("state"))) { assertThat(truth.allFrozenRulesHold).isTrue(); }
        }
    }

    @Test void exact_call_returns_reproduce_the_original_ten_bins_and_half_trend_rule() {
        var rows = balanced(0, 10_000, 10_000);
        var exact = rows.stream().map(row -> row.literalReturn().lowerNanos()).sorted().toList();
        var frozen = BenchmarkSteadyOutputWindow.read(exact);
        var certificate = BenchmarkReturnCommonWindow.evidence(rows);
        assertThat(certificate).containsEntry("state", "PASS").containsEntry("completedRowsLower", 20_000L)
                .containsEntry("completedRowsUpper", 20_000L).containsEntry("performanceAcceptanceEligible", false);
        assertThat(binCounts(certificate, "lowerRows")).isEqualTo(frozen.fixedBins());
        assertThat(binCounts(certificate, "upperRows")).isEqualTo(frozen.fixedBins());
        assertThat(certificate.get("lateToEarlyRatioLower")).isEqualTo(Map.of("numerator", 10_000L, "denominator", 10_000L));
        assertThat(certificate.get("halfTrendUpper")).isEqualTo(Map.of("numerator", 0L, "denominator", 10_000L));
    }

    @Test void the_inclusive_five_percent_boundary_is_kept_in_both_directions() {
        assertThat(BenchmarkReturnCommonWindow.evidence(balanced(0, 10_000, 10_500))).containsEntry("state", "PASS");
        assertThat(BenchmarkReturnCommonWindow.evidence(balanced(0, 10_000, 9_500))).containsEntry("state", "PASS");
        var outside = balanced(0, 10_000, 10_500);
        add(outside, "target", 500, 1000, 1000, 1);
        assertThat(BenchmarkReturnCommonWindow.evidence(outside)).containsEntry("state", "UNQUALIFIED")
                .containsEntry("reason", "FIVE_PERCENT_HALF_TREND_NOT_PROVEN");
    }

    @Test void common_target_activity_is_the_intersection_rather_than_the_union_of_return_times() {
        var rows = new ArrayList<BenchmarkReturnTimeBounds.Delivery>();
        add(rows, "one", 1, 0, 0, 1); add(rows, "one", 2, 1000, 1000, 1); add(rows, "one", 3, 300, 300, 3);
        add(rows, "two", 4, 200, 200, 1); add(rows, "two", 5, 800, 800, 2); add(rows, "two", 6, 300, 300, 4);
        var certificate = BenchmarkReturnCommonWindow.evidence(rows);
        assertThat(certificate).containsEntry("commonStartBoundsNanos", List.of(200L, 200L))
                .containsEntry("commonEndBoundsNanos", List.of(800L, 800L))
                .containsEntry("durationNanosLower", 600L).containsEntry("durationNanosUpper", 600L)
                .containsEntry("fullCohortRows", 12).containsEntry("completedRowsLower", 9L)
                .containsEntry("completedRowsUpper", 9L);
    }

    @Test void exact_internal_bin_edges_enter_the_next_bin_and_the_lower_endpoint_is_excluded() {
        var rows = new ArrayList<BenchmarkReturnTimeBounds.Delivery>();
        add(rows, "target", 1, 0, 0, 1000);
        for (int edge = 1; edge <= 10; edge++) { add(rows, "target", edge + 1, edge * 100, edge * 100, 1000); }
        var certificate = BenchmarkReturnCommonWindow.evidence(rows);
        assertThat(certificate).containsEntry("completedRowsLower", 10_000L).containsEntry("completedRowsUpper", 10_000L)
                .containsEntry("state", "UNQUALIFIED").containsEntry("reason", "TEN_BIN_PROGRESS_NOT_PROVEN");
        assertThat(binCounts(certificate, "lowerRows"))
                .containsExactly(0L, 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, 1000L, 2000L);
    }

    @Test void shared_ambiguous_call_weights_cannot_be_split_at_a_midpoint_to_manufacture_steadiness() {
        var rows = balanced(0, 22_976, 22_976);
        add(rows, "target", 500, 499, 501, 1024); add(rows, "target", 501, 499, 501, 1024);
        var certificate = BenchmarkReturnCommonWindow.evidence(rows);
        assertThat(certificate).containsEntry("state", "UNQUALIFIED")
                .containsEntry("reason", "FIVE_PERCENT_HALF_TREND_NOT_PROVEN")
                .containsEntry("completedRowsLower", 48_000L).containsEntry("completedRowsUpper", 48_000L)
                .containsEntry("earlyRowsLower", 22_976L).containsEntry("earlyRowsUpper", 25_024L)
                .containsEntry("lateRowsLower", 22_976L).containsEntry("lateRowsUpper", 25_024L);
        assertThat(binCounts(certificate, "lowerRows")).allMatch(count -> count > 0);
        assertThat(certificate.get("lateToEarlyRatioUpper")).isEqualTo(Map.of("numerator", 25_024L, "denominator", 22_976L));
    }

    @Test void uncertain_membership_keeps_possible_rows_instead_of_trimming_them() {
        var rows = new ArrayList<BenchmarkReturnTimeBounds.Delivery>();
        add(rows, "target", 1, 0, 20, 1000); add(rows, "target", 2, 10, 30, 1000);
        add(rows, "target", 3, 90, 100, 1000);
        var certificate = BenchmarkReturnCommonWindow.evidence(rows);
        // The last call dominates every earlier upper bound and belongs to its own closed target end.
        assertThat(certificate).containsEntry("state", "UNQUALIFIED")
                .containsEntry("completedRowsLower", 1000L).containsEntry("completedRowsUpper", 3000L)
                .containsEntry("ambiguousMembershipRows", 2000L).containsEntry("fullCohortRows", 3000);
    }

    @Test void endpoint_uncertainty_cannot_be_hidden_by_selecting_a_convenient_shorter_window() {
        var rows = new ArrayList<BenchmarkReturnTimeBounds.Delivery>();
        add(rows, "one", 1, 0, 100, 1000); add(rows, "one", 2, 50, 150, 1000);
        add(rows, "two", 3, 25, 125, 1000); add(rows, "two", 4, 75, 175, 1000);
        assertThat(BenchmarkReturnCommonWindow.evidence(rows))
                .containsEntry("state", "UNQUALIFIED").containsEntry("reason", "NO_GUARANTEED_POSITIVE_COMMON_INTERVAL")
                .containsEntry("commonStartBoundsNanos", List.of(25L, 125L))
                .containsEntry("commonEndBoundsNanos", List.of(50L, 150L))
                .containsEntry("durationNanosLower", -75L).containsEntry("durationNanosUpper", 125L);
    }

    @Test void exact_rational_edges_do_not_overflow_or_change_when_clock_origin_is_shifted() {
        var baseline = BenchmarkReturnCommonWindow.evidence(balanced(0, 10_000, 10_000));
        for (long origin : List.of(Long.MAX_VALUE - 1000, Long.MIN_VALUE + 7)) {
            var shifted = BenchmarkReturnCommonWindow.evidence(balanced(origin, 10_000, 10_000));
            assertThat(shifted).containsEntry("state", "PASS").containsEntry("durationNanosLower", 1000L)
                    .containsEntry("durationNanosUpper", 1000L);
            assertThat(binCounts(shifted, "lowerRows")).isEqualTo(binCounts(baseline, "lowerRows"));
            assertThat(shifted.get("recordsPerSecondLower")).isEqualTo(baseline.get("recordsPerSecondLower"));
            assertThat(shifted.get("recordsPerSecondUpper")).isEqualTo(baseline.get("recordsPerSecondUpper"));
        }
        var unrepresentable = new ArrayList<BenchmarkReturnTimeBounds.Delivery>();
        add(unrepresentable, "target", 1, Long.MIN_VALUE, Long.MIN_VALUE, 1);
        add(unrepresentable, "target", 2, Long.MAX_VALUE, Long.MAX_VALUE, 1);
        assertThat(BenchmarkReturnCommonWindow.evidence(unrepresentable))
                .containsEntry("state", "UNQUALIFIED").containsEntry("reason", "COMMON_WINDOW_DURATION_OVERFLOW")
                .doesNotContainKeys("recordsPerSecondLower", "recordsPerSecondUpper");
    }

    @Test void inconsistent_call_endpoints_targets_and_duplicate_keys_are_refused() {
        var interval = new ArrayList<BenchmarkReturnTimeBounds.Delivery>();
        add(interval, "target", 1, 100, 200, 1);
        interval.add(row("target", "other", 1, 101, 200));
        assertThatThrownBy(() -> BenchmarkReturnCommonWindow.evidence(interval))
                .isInstanceOf(AssertionError.class).hasMessageContaining("inconsistent");
        var target = new ArrayList<BenchmarkReturnTimeBounds.Delivery>();
        add(target, "one", 1, 100, 200, 1); target.add(row("two", "other", 1, 100, 200));
        assertThatThrownBy(() -> BenchmarkReturnCommonWindow.evidence(target))
                .isInstanceOf(AssertionError.class).hasMessageContaining("inconsistent");
        var duplicate = row("target", "same", 1, 100, 200);
        assertThatThrownBy(() -> BenchmarkReturnCommonWindow.evidence(List.of(duplicate, duplicate)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("duplicate");
    }

    @Test void all_row_call_and_target_rosters_remain_bounded() {
        var sample = row("target", "key", 1, 100, 200);
        assertThatThrownBy(() -> BenchmarkReturnCommonWindow.evidence(Collections.nCopies(262_145, sample)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("bounded cohort");
        var calls = new ArrayList<BenchmarkReturnTimeBounds.Delivery>();
        for (int index = 1; index <= 513; index++) { add(calls, "target", index, index, index, 1); }
        assertThatThrownBy(() -> BenchmarkReturnCommonWindow.evidence(calls))
                .isInstanceOf(AssertionError.class).hasMessageContaining("call roster");
        var tooManyRows = new ArrayList<BenchmarkReturnTimeBounds.Delivery>();
        add(tooManyRows, "target", 1, 100, 200, 1025);
        assertThatThrownBy(() -> BenchmarkReturnCommonWindow.evidence(tooManyRows))
                .isInstanceOf(AssertionError.class).hasMessageContaining("native row bound");
        assertThatThrownBy(() -> BenchmarkReturnCommonWindow.evidence(List.of(row("one", "k", 1, 100, 100),
                row("two", "k", 2, 100, 100), row("three", "k", 3, 100, 100))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("target roster");
    }

    private static ArrayList<BenchmarkReturnTimeBounds.Delivery> balanced(long origin, int early, int late) {
        var rows = new ArrayList<BenchmarkReturnTimeBounds.Delivery>();
        add(rows, "target", 1, origin, origin, 1);
        long sequence = 2;
        for (int bin = 0; bin < 10; bin++) {
            int total = bin < 5 ? early : late;
            int halfBin = bin < 5 ? bin : bin - 5;
            int count = total / 5 + (halfBin < total % 5 ? 1 : 0);
            long at = origin + (bin == 9 ? 1000 : bin * 100 + 50);
            while (count > 0) {
                int weight = Math.min(1024, count); add(rows, "target", sequence++, at, at, weight); count -= weight;
            }
        }
        return rows;
    }
    private static void add(List<BenchmarkReturnTimeBounds.Delivery> rows, String target, long sequence,
                            long lower, long upper, int weight) {
        for (int index = 0; index < weight; index++) { rows.add(row(target, sequence + ":" + index, sequence, lower, upper)); }
    }
    private static BenchmarkReturnTimeBounds.Delivery row(String target, String key, long sequence, long lower, long upper) {
        var returned = new BenchmarkCausalClock.Interval(lower, upper);
        return new BenchmarkReturnTimeBounds.Delivery(target, key, sequence, returned, returned, returned);
    }
    private static List<Long> binCounts(Map<String, Object> evidence, String field) {
        @SuppressWarnings("unchecked") var bins = (List<Map<String, Object>>) evidence.get("bins");
        return bins.stream().map(bin -> (Long) bin.get(field)).toList();
    }

    private record CallRange(String target, long lower, long upper, int weight) { }
    private static final class AssignmentBounds {
        final long[] minimum = new long[13], maximum = new long[13];
        boolean allFrozenRulesHold = true;
        AssignmentBounds() { java.util.Arrays.fill(minimum, Long.MAX_VALUE); }
    }
    private static void enumerate(List<CallRange> calls, int index, long[] times, AssignmentBounds bounds) {
        if (index < calls.size()) {
            var call = calls.get(index);
            for (long at = call.lower(); at <= call.upper(); at++) {
                times[index] = at; enumerate(calls, index + 1, times, bounds);
            }
            return;
        }
        var first = new java.util.LinkedHashMap<String, Long>();
        var last = new java.util.LinkedHashMap<String, Long>();
        for (int call = 0; call < calls.size(); call++) {
            first.merge(calls.get(call).target(), times[call], Math::min);
            last.merge(calls.get(call).target(), times[call], Math::max);
        }
        long start = first.values().stream().mapToLong(Long::longValue).max().orElseThrow();
        long end = last.values().stream().mapToLong(Long::longValue).min().orElseThrow();
        long[] actual = new long[13];
        for (int call = 0; call < calls.size(); call++) {
            long at = times[call], weight = calls.get(call).weight();
            if (at <= start || at > end) { continue; }
            actual[0] += weight;
            actual[2 * at < start + end ? 1 : 2] += weight;
            int bin = 0;
            while (bin < 9 && 10 * at >= (9 - bin) * start + (bin + 1) * end) { bin++; }
            actual[bin + 3] += weight;
        }
        for (int field = 0; field < actual.length; field++) {
            bounds.minimum[field] = Math.min(bounds.minimum[field], actual[field]);
            bounds.maximum[field] = Math.max(bounds.maximum[field], actual[field]);
        }
        bounds.allFrozenRulesHold &= end > start && actual[0] >= 10_000
                && 20 * Math.abs(actual[2] - actual[1]) <= actual[1]
                && java.util.Arrays.stream(actual, 3, 13).allMatch(count -> count > 0);
    }
}
