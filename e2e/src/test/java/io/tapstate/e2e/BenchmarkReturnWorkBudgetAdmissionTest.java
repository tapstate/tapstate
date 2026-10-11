package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnWorkBudgetAdmissionTest {
    private static final String HASH = "a".repeat(64);
    private static final BenchmarkReturnWorkBudgetAdmission.Runtime OWNER =
            new BenchmarkReturnWorkBudgetAdmission.Runtime("controlled-run", 17, 23);

    @Test void complete_actual_requests_are_recomputed_without_a_future_lifetime_cap() throws Exception {
        var definition = definition(false, Set.of());
        var external = new AtomicInteger();
        Map<String, Object> receipt;
        try (var scope = BenchmarkReturnWorkBudget.activate(definition, OWNER.runId())) {
            scope.bindRoot(OWNER.pid(), OWNER.jvmStartTimeMillis());
            for (int i = 0; i < 3; i++) {
                var ticket = scope.begin(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, BenchmarkReturnWorkBudget.Work.request());
                assertThat(external.incrementAndGet()).isPositive();
                scope.complete(ticket, BenchmarkReturnWorkBudget.Work.bytes(2), null);
            }
            receipt = outputAndFinish(scope);
        }
        var verified = BenchmarkReturnWorkBudgetAdmission.verifyCounters(definition, OWNER, receipt);
        var actual = verified.actual().get(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT);
        var upper = verified.upper().get(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT);
        assertThat(actual.requests()).isEqualTo(external.get()).isEqualTo(3);
        assertThat(actual.bytes()).isEqualTo(6); assertThat(upper.bytes()).isEqualTo(12);
        assertThat(verified.selfUpper().evidence()).isEqualTo(receipt.get("selfSourceUpper"));
        assertThat(verified.selfUpper().requests()).isPositive();
        assertThat(receipt).containsEntry("structuralWorkBudgetQualified", false)
                .containsEntry("wholeMethodCostQualified", false).containsEntry("performanceAcceptanceEligible", false);
    }

    @Test void an_extra_actual_request_exceeds_the_registered_prior_budget() throws Exception {
        var definition = definition(true, Set.of()); var external = new AtomicInteger();
        Map<String, Object> receipt;
        try (var scope = BenchmarkReturnWorkBudget.activate(definition, OWNER.runId())) {
            scope.bindRoot(OWNER.pid(), OWNER.jvmStartTimeMillis());
            for (int i = 0; i < 4; i++) {
                BenchmarkReturnWorkBudget.request(scope, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, external::incrementAndGet);
            }
            receipt = outputAndFinish(scope);
        }
        assertThat(external).hasValue(4);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetAdmission.verifyCounters(definition, OWNER, receipt))
                .isInstanceOf(AssertionError.class).hasMessageContaining("open or refused");
    }

    @Test void supplied_upper_fields_and_qualification_flags_cannot_replace_the_source_formula() throws Exception {
        var fixture = fixture(); var receipt = mutable(fixture.receipt());
        var row = row(receipt, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT);
        var fabricated = new LinkedHashMap<>((Map<String, Object>) row.get("upper")); fabricated.put("bytes", 999L);
        row.put("upper", fabricated);
        var substituted = receipt;
        assertThatThrownBy(() -> verify(fixture, substituted)).isInstanceOf(AssertionError.class).hasMessageContaining("source formula");
        receipt = mutable(fixture.receipt()); receipt.put("structuralWorkBudgetQualified", true);
        var qualified = receipt;
        assertThatThrownBy(() -> verify(fixture, qualified)).isInstanceOf(AssertionError.class).hasMessageContaining("claimed a qualification");
    }

    @Test void wrong_source_runtime_or_missing_activity_is_refused() throws Exception {
        var fixture = fixture();
        var source = mutable(fixture.receipt()); source.put("sourceFormulaSha256", "b".repeat(64));
        assertThatThrownBy(() -> verify(fixture, source)).isInstanceOf(AssertionError.class).hasMessageContaining("source formula registration");
        var runtime = mutable(fixture.receipt()); runtime.put("rootRuntime", Map.of("pid", 18L, "jvmStartTimeMillis", 23L));
        assertThatThrownBy(() -> verify(fixture, runtime)).isInstanceOf(AssertionError.class).hasMessageContaining("runtime identity");
        var missing = mutable(fixture.receipt()); activities(missing).remove(BenchmarkReturnWorkBudget.Activity.CLEANUP.name());
        assertThatThrownBy(() -> verify(fixture, missing)).isInstanceOf(AssertionError.class).hasMessageContaining("activity roster");
    }

    @Test void open_omitted_gap_and_fractional_counters_are_refused_even_with_a_closed_label() throws Exception {
        var fixture = fixture();
        var pending = mutable(fixture.receipt()); pending.put("pendingOperations", 1L);
        assertThatThrownBy(() -> verify(fixture, pending)).isInstanceOf(AssertionError.class).hasMessageContaining("pending or omitted");
        var omitted = mutable(fixture.receipt()); omitted.put("omittedOperations", 1L);
        assertThatThrownBy(() -> verify(fixture, omitted)).isInstanceOf(AssertionError.class).hasMessageContaining("pending or omitted");
        var gap = mutable(fixture.receipt()); row(gap, BenchmarkReturnWorkBudget.Activity.CLEANUP).put("gap", "UNCOVERED_CLOSE");
        assertThatThrownBy(() -> verify(fixture, gap)).isInstanceOf(AssertionError.class).hasMessageContaining("incomplete");
        var fractional = mutable(fixture.receipt()); row(fractional, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT).put("attempts", 1.0);
        assertThatThrownBy(() -> verify(fixture, fractional)).isInstanceOf(AssertionError.class).hasMessageContaining("exact integer");
    }

    @Test void excluded_activity_needs_zero_actual_work_and_zero_attempts() throws Exception {
        var definition = definition(false, Set.of(BenchmarkReturnWorkBudget.Activity.ROOT_CPU));
        Map<String, Object> receipt;
        try (var scope = BenchmarkReturnWorkBudget.activate(definition, OWNER.runId())) {
            scope.bindRoot(OWNER.pid(), OWNER.jvmStartTimeMillis()); receipt = outputAndFinish(scope);
        }
        BenchmarkReturnWorkBudgetAdmission.verifyCounters(definition, OWNER, receipt);
        var used = mutable(receipt); var row = row(used, BenchmarkReturnWorkBudget.Activity.ROOT_CPU);
        row.put("attempts", 1L); row.put("completedAttempts", 1L);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetAdmission.verifyCounters(definition, OWNER, used))
                .isInstanceOf(AssertionError.class).hasMessageContaining("excluded activity");
    }

    @Test void an_intended_method_attempt_cannot_replace_the_actual_request_completion() throws Exception {
        var fixture = fixture(); var receipt = mutable(fixture.receipt());
        row(receipt, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT).put("completedRequests", 0L);
        assertThatThrownBy(() -> verify(fixture, receipt)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("issued requests");
        var mismatch = mutable(fixture.receipt());
        row(mismatch, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT).put("issuedRequests", 0L);
        assertThatThrownBy(() -> verify(fixture, mismatch)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("issued requests");
    }

    @Test void self_accounting_is_recomputed_and_cannot_be_replaced_by_a_caller_upper() throws Exception {
        var fixture = fixture(); var replaced = mutable(fixture.receipt());
        @SuppressWarnings("unchecked") var upper = new LinkedHashMap<>((Map<String, Object>) replaced.get("selfSourceUpper"));
        upper.put("requests", 0L); replaced.put("selfSourceUpper", upper);
        assertThatThrownBy(() -> verify(fixture, replaced)).isInstanceOf(AssertionError.class).hasMessageContaining("self upper");
        var missing = mutable(fixture.receipt());
        @SuppressWarnings("unchecked") var counters = (Map<String, Object>) missing.get("selfCounters");
        counters.remove("outputCloneBytes");
        assertThatThrownBy(() -> verify(fixture, missing)).isInstanceOf(AssertionError.class).hasMessageContaining("self counter roster");
        var overflow = mutable(fixture.receipt()); overflow.put("selfOverflow", true);
        assertThatThrownBy(() -> verify(fixture, overflow)).isInstanceOf(AssertionError.class).hasMessageContaining("overflowed");
    }

    @Test void output_failure_preserves_the_primary_and_cannot_become_complete() {
        var definition = definition(false, Set.of()); var original = new java.io.IOException("controlled writer failed");
        try (var scope = BenchmarkReturnWorkBudget.activate(definition, OWNER.runId())) {
            scope.bindRoot(OWNER.pid(), OWNER.jvmStartTimeMillis());
            var prepared = scope.prepareOutput("controlled", new BenchmarkReturnWorkBudget.TreePlan(8, 32, 128, 8), "body");
            assertThatThrownBy(() -> scope.writeOutput(prepared, bytes -> { throw original; })).isSameAs(original);
            assertThat(scope.primaryCause()).isSameAs(original);
            assertThatThrownBy(() -> BenchmarkReturnWorkBudgetAdmission.verifyCounters(definition, OWNER, scope.finish()))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("open or refused");
        }
    }

    @Test void actual_file_bytes_and_the_registered_tree_plan_bind_the_completed_write(@TempDir java.nio.file.Path directory) throws Exception {
        var definition = definition(false, Set.of());
        var plan = new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(8, 8, 32, 64, 32, 24);
        var tree = BenchmarkReturnWorkBudgetDefinitions.freezeOutputTree(Map.of("body", "value"), plan);
        BenchmarkReturnWorkBudget.PreparedOutput prepared;
        Map<String, Object> completed;
        var path = directory.toRealPath().resolve("output.json");
        try (var scope = BenchmarkReturnWorkBudget.activate(definition, OWNER.runId())) {
            scope.bindRoot(OWNER.pid(), OWNER.jvmStartTimeMillis());
            prepared = scope.prepareOutput("controlled", new BenchmarkReturnWorkBudget.TreePlan(plan.nodeUpper(),
                    plan.stringUtf8Upper(), plan.encodedUpperBytes(), plan.depthUpper()), tree.value(), plan.sha256());
            scope.writeOutput(prepared, bytes -> java.nio.file.Files.write(path, bytes));
            BenchmarkReturnWorkBudgetAdmission.verifyOutputFile(path, prepared, plan, scope.snapshot());
            completed = scope.finish();
            BenchmarkReturnWorkBudgetAdmission.verifyCounters(definition, OWNER, completed);
        }
        var substituted = new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(9, 8, 32, 64, 32, 24);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetAdmission.verifyOutputFile(path, prepared, substituted, completed))
                .isInstanceOf(AssertionError.class).hasMessageContaining("registered prior plan");
        var sameBytesDifferentShape = new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(8, 7, 31, 64, 31, 24);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetAdmission.verifyOutputFile(path, prepared, sameBytesDifferentShape, completed))
                .isInstanceOf(AssertionError.class).hasMessageContaining("registered material plan");
        java.nio.file.Files.writeString(path, "corrupt");
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetAdmission.verifyOutputFile(path, prepared, plan, completed))
                .isInstanceOf(AssertionError.class).hasMessageContaining("length differs");
    }

    @Test void source_multiplication_overflow_and_work_after_seal_are_refused() throws Exception {
        var fixture = fixture(); var overflow = mutable(fixture.receipt());
        var row = row(overflow, BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT);
        row.put("attempts", Long.MAX_VALUE); row.put("completedAttempts", Long.MAX_VALUE);
        assertThatThrownBy(() -> verify(fixture, overflow)).isInstanceOf(AssertionError.class).hasMessageContaining("arithmetic overflowed");
        var definition = definition(false, Set.of());
        try (var scope = BenchmarkReturnWorkBudget.activate(definition, OWNER.runId())) {
            scope.bindRoot(OWNER.pid(), OWNER.jvmStartTimeMillis()); outputAndFinish(scope);
            scope.add(BenchmarkReturnWorkBudget.Activity.CLEANUP, BenchmarkReturnWorkBudget.Work.call());
            assertThatThrownBy(() -> BenchmarkReturnWorkBudgetAdmission.verifyCounters(definition, OWNER, scope.snapshot()))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("open or refused");
        }
    }

    private record Fixture(BenchmarkReturnWorkBudget.Definition definition, Map<String, Object> receipt) { }
    private static Fixture fixture() throws Exception {
        var definition = definition(false, Set.of());
        try (var scope = BenchmarkReturnWorkBudget.activate(definition, OWNER.runId())) {
            scope.bindRoot(OWNER.pid(), OWNER.jvmStartTimeMillis());
            var ticket = scope.begin(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, BenchmarkReturnWorkBudget.Work.request());
            scope.complete(ticket, BenchmarkReturnWorkBudget.Work.bytes(2), null);
            return new Fixture(definition, outputAndFinish(scope));
        }
    }

    private static BenchmarkReturnWorkBudget.Definition definition(boolean fixedRequests,
            Set<BenchmarkReturnWorkBudget.Activity> excluded) {
        var perOperation = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        var prior = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        for (var activity : BenchmarkReturnWorkBudget.Activity.values()) {
            if (!excluded.contains(activity)) {
                perOperation.put(activity, new BenchmarkReturnWorkBudget.Work(64, 64, 262144, 512, 131072, 64, 1048576, 4096));
            }
        }
        if (fixedRequests) { perOperation.remove(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT);
            prior.put(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, new BenchmarkReturnWorkBudget.Work(3, 3, 0, 0, 12, 0, 0, 0)); }
        else { perOperation.put(BenchmarkReturnWorkBudget.Activity.SHARED_SUPPORT, new BenchmarkReturnWorkBudget.Work(1, 1, 0, 0, 4, 0, 0, 0)); }
        return new BenchmarkReturnWorkBudget.Definition("CONTROLLED_COUNTER_SOURCE_V1", HASH, prior, perOperation, excluded);
    }

    private static Map<String, Object> outputAndFinish(BenchmarkReturnWorkBudget.Scope scope) throws Exception {
        var prepared = scope.prepareOutput("controlled", new BenchmarkReturnWorkBudget.TreePlan(8, 32, 128, 8), "body");
        scope.writeOutput(prepared, bytes -> assertThat(bytes).containsExactly((byte) '"', (byte) 'b', (byte) 'o', (byte) 'd', (byte) 'y', (byte) '"'));
        return scope.finish();
    }
    private static BenchmarkReturnWorkBudgetAdmission.Recomputed verify(Fixture fixture, Map<String, Object> receipt) {
        return BenchmarkReturnWorkBudgetAdmission.verifyCounters(fixture.definition(), OWNER, receipt);
    }
    @SuppressWarnings("unchecked") private static Map<String, Object> mutable(Map<String, Object> receipt) {
        return (Map<String, Object>) JsonReader.parse(JsonWriter.write(receipt));
    }
    @SuppressWarnings("unchecked") private static Map<String, Object> activities(Map<String, Object> receipt) {
        return (Map<String, Object>) receipt.get("activities");
    }
    @SuppressWarnings("unchecked") private static Map<String, Object> row(Map<String, Object> receipt, BenchmarkReturnWorkBudget.Activity activity) {
        return (Map<String, Object>) activities(receipt).get(activity.name());
    }
}
