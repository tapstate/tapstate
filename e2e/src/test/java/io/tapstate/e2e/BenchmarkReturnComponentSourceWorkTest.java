package io.tapstate.e2e;

import java.sql.SQLException;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnComponentSourceWorkTest {
    @Test void absent_scope_keeps_one_original_action_and_no_counter_supplier_or_worker() throws Exception {
        assertThat(BenchmarkReturnWorkBudget.current()).isNull();
        var calls = new AtomicInteger();
        assertThat(BenchmarkReturnComponentSourceWork.entered(BenchmarkReturnComponentSourceWork.Kind.PREFLIGHT,
                () -> { calls.incrementAndGet(); return 7; })).isEqualTo(7);
        assertThat(calls).hasValue(1);
    }

    @Test void actual_pool_entries_and_existing_p99_children_are_retained_without_charging_upper_as_actual() {
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "component-pool")) {
            scope.bindRoot(17, 23);
            var phase = new BenchmarkReturnIntervalMetrics.Phase("one", 10000, 10000, 1000000000, 1000000000,
                    java.util.Collections.nCopies(10000, new BenchmarkCausalClock.Interval(1, 2)));
            var result = BenchmarkReturnIntervalMetrics.aggregate(List.of(phase));
            assertThat(result.p99()).isEqualTo(new BenchmarkCausalClock.Interval(1, 2));
            var associations = row(scope.snapshot(), "ASSOCIATION");
            assertThat(associations).containsEntry("attempts", 3L).containsEntry("completedAttempts", 3L);
            assertThat(row(scope.snapshot(), "SORT")).containsEntry("issuedRequests", 2L).containsEntry("completedRequests", 2L);
            assertThat(row(associations, "actual")).containsEntry("records", 0L);
            assertThat(scope.snapshot()).containsEntry("state", "OPEN").containsEntry("pendingOperations", 0L);
        }
    }

    @Test void actual_missing_preflight_keeps_typed_primary_and_its_bounded_refusal_facts() {
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "missing-preflight")) {
            assertThatThrownBy(() -> BenchmarkReturnMethodAdmission.admit(null))
                    .isInstanceOf(BenchmarkReturnMethodAdmission.Refusal.class).hasMessageContaining("PREFLIGHT_COMPONENT_MISSING");
            assertThat(scope.primaryCause()).isInstanceOf(BenchmarkReturnMethodAdmission.Refusal.class);
            assertThat(row(scope.snapshot(), "INSTALL_BINDINGS")).containsEntry("failedAttempts", 1L);
            assertThat(row(scope.snapshot(), "FAILURE_RETENTION")).containsEntry("completedAttempts", 1L);
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN").containsEntry("pendingOperations", 0L);
        }
    }

    @Test void checked_primary_is_the_same_object_and_recording_does_not_replace_it() {
        SQLException primary = new SQLException("controlled primary");
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "component-primary")) {
            assertThatThrownBy(() -> BenchmarkReturnComponentSourceWork.entered(BenchmarkReturnComponentSourceWork.Kind.METRIC_POOL,
                    () -> { throw primary; })).isSameAs(primary);
            assertThat(scope.primaryCause()).isSameAs(primary);
            assertThat(scope.snapshot()).containsEntry("pendingOperations", 0L).containsEntry("state", "UNKNOWN");
        }
    }

    @Test void source_tree_formula_is_checked_and_has_nonzero_explicit_request_slots() {
        var plan = new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(32768, 16, 8192, 65536, 512, 64);
        var limits = new BenchmarkReturnComponentSourceWork.Limits(BenchmarkReturnWorkBudgetBindings.SourceRole.values().length, plan);
        for (var kind : BenchmarkReturnComponentSourceWork.Kind.values()) {
            assertThat(BenchmarkReturnComponentSourceWork.upper(kind, limits).requests()).isPositive();
        }
        var tree = BenchmarkReturnComponentSourceWork.treeUpper(4096, 65536);
        assertThat(tree.requests()).isEqualTo(32 + 64L * 4096 + 16);
        assertThat(tree.bytes()).isEqualTo(16L * 4096 + 4L * 65536);
        assertThatThrownBy(() -> BenchmarkReturnComponentSourceWork.treeUpper(Long.MAX_VALUE / 48, 1))
                .isInstanceOf(ArithmeticException.class);
    }

    @Test void known_readless_authority_has_its_own_entry_unknown_callback_cannot_receive_body_credit() {
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "authority-source")) {
            BenchmarkReturnComponentSourceWork.semanticBody(BenchmarkNativeSourceAuthority.instance());
            assertThatThrownBy(() -> BenchmarkNativeSourceAuthority.instance().requireInspectedDirectInvocationAndProviderContract(null))
                    .hasMessageContaining("METHOD_OR_INSPECTION_MISSING");
            assertThat(row(scope.snapshot(), "INSTALL_BINDINGS")).containsEntry("attempts", 1L).containsEntry("failedAttempts", 1L);
        }
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "unknown-callback")) {
            BenchmarkReturnComponentSourceWork.semanticBody(ignored -> { });
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN");
        }
    }

    @Test void source_role_bound_must_match_the_exact_registered_roster_before_table_construction() {
        var plan = new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(32768, 16, 8192, 65536, 512, 64);
        int actualRoles = BenchmarkReturnWorkBudgetBindings.SourceRole.values().length;
        for (int mismatched : new int[]{actualRoles - 1, actualRoles + 1}) {
            assertThatThrownBy(() -> new BenchmarkReturnComponentSourceWork.Limits(mismatched, plan))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("source role roster differs");
        }
        var exact = new BenchmarkReturnComponentSourceWork.Limits(actualRoles, plan);
        assertThat(BenchmarkReturnComponentSourceWork.upper(BenchmarkReturnComponentSourceWork.Kind.PREFLIGHT, exact).requests())
                .isPositive();
    }

    private static BenchmarkReturnWorkBudget.Definition definition() {
        var plan = new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(32768, 16, 8192, 65536, 512, 64);
        var upper = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        upper.putAll(BenchmarkReturnLocalSourceWork.bounds(new BenchmarkReturnLocalSourceWork.Capacity(96000, 48000, 960)));
        BenchmarkReturnComponentSourceWork.additions(new BenchmarkReturnComponentSourceWork.Limits(
                BenchmarkReturnWorkBudgetBindings.SourceRole.values().length, plan))
                .forEach((actor, value) -> upper.merge(actor, value, BenchmarkSharedSupportSourceWork::maximum));
        var excluded = EnumSet.allOf(BenchmarkReturnWorkBudget.Activity.class); excluded.removeAll(upper.keySet());
        return new BenchmarkReturnWorkBudget.Definition("controlled-component-source", "a".repeat(64), Map.of(), upper, excluded);
    }
    @SuppressWarnings("unchecked") private static Map<String, Object> row(Map<String, Object> value, String key) {
        Object nested = value.get(key);
        if (nested == null) nested = ((Map<String, Object>) value.get("activities")).get(key);
        return (Map<String, Object>) nested;
    }
}
