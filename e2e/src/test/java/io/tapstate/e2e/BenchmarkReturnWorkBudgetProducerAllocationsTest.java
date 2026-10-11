package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnWorkBudgetProducerAllocationsTest {
    @Test
    void fixed_producer_vector_is_recomputed_without_runtime_or_method_credit() {
        Map<String, Object> result = BenchmarkReturnWorkBudgetProducerAllocations.evaluate(body());
        assertThat(result).containsEntry("state", "SOURCE_UPPER_RECOMPUTED_DIAGNOSTIC")
                .containsEntry("sourceDefinitionMatched", true).containsEntry("countPairingComplete", true);
        assertThat(result.get("recomputedSourceUpper")).isEqualTo(vector(3, 598316, 0, 0, 444044, 865, 1018612, 953140));
        for (String key : List.of("sourceCoverageVerified", "classArtifactBindingVerified", "callerCeilingsAuthoritative",
                "futureInvocationBoundClaimed", "wholeMethodCostQualified", "costAcceptanceEligible", "performanceAcceptanceEligible", "formalPerformance")) {
            assertThat(result.get(key)).isEqualTo(false);
        }
    }

    @Test
    void all_cold_origin_invocations_use_the_independent_source_vector() {
        var body = body(); rows(body).put("COLD_ORIGINS", row(32, 32, 0, 0));
        body.put("sourceUpper", vector(532217891, 520015372, 32, 0, 422496012, 214369, 802950132, 532351796));
        var result = BenchmarkReturnWorkBudgetProducerAllocations.evaluate(body);
        assertThat(result.get("recomputedSourceUpper")).isEqualTo(body.get("sourceUpper"));
        assertThat(result).containsEntry("countPairingComplete", true).containsEntry("wholeMethodCostQualified", false);
        rows(body).put("COLD_ORIGINS", row(31, 31, 0, 0));
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProducerAllocations.evaluate(body))
                .isInstanceOf(AssertionError.class).hasMessageContaining("independent source sum");
    }

    @Test
    void explicit_bookkeeping_actor_and_separate_mutations_are_both_in_the_source_sum() {
        var body = body();
        rows(body).put("BOOKKEEPING", row(1, 1, 0, 0));
        body.put("sourceUpper", vector(4, 598632, 0, 0, 447304, 871, 1020144, 954672));
        Map<String, Object> result = BenchmarkReturnWorkBudgetProducerAllocations.evaluate(body);
        assertThat(result.get("recomputedSourceUpper")).isEqualTo(body.get("sourceUpper"));
        assertThat(result).containsEntry("bookkeepingInvocations", 1L);
    }

    @Test
    void a_tampered_scalar_upper_cannot_replace_the_trusted_source_table() {
        var body = body();
        body.put("sourceUpper", vector(3, 598317, 0, 0, 444044, 865, 1018612, 953140));
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProducerAllocations.evaluate(body))
                .isInstanceOf(AssertionError.class).hasMessageContaining("independent source sum");
    }

    @Test
    void a_tampered_tail_allocation_or_source_hash_is_refused() {
        var tail = body(); tail.put("sourceSealTailUpper", vector(1, 201047, 0, 0, 308032, 843, 751576, 686104));
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProducerAllocations.evaluate(tail))
                .isInstanceOf(AssertionError.class).hasMessageContaining("tail allocation");
        var hash = body(); hash.put("sourceFormulaSha256", "a".repeat(64));
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProducerAllocations.evaluate(hash))
                .isInstanceOf(AssertionError.class).hasMessageContaining("source definition");
    }

    @Test
    void missing_and_foreign_activity_rows_are_never_trimmed() {
        var missing = body(); rows(missing).remove("HASH_MATERIAL");
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProducerAllocations.evaluate(missing))
                .isInstanceOf(AssertionError.class).hasMessageContaining("activity roster");
        var extra = body(); rows(extra).put("FOREIGN", row(0, 0, 0, 0));
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProducerAllocations.evaluate(extra))
                .isInstanceOf(AssertionError.class).hasMessageContaining("activity roster");
    }

    @Test
    void failed_or_pending_pairs_remain_diagnostic_and_are_not_called_complete() {
        var failed = body(); failed.put("state", "UNKNOWN"); rows(failed).put("CONFIG", row(1, 0, 1, 0));
        Map<String, Object> result = BenchmarkReturnWorkBudgetProducerAllocations.evaluate(failed);
        assertThat(result).containsEntry("failedOperations", 1L).containsEntry("countPairingComplete", false)
                .containsEntry("producerState", "UNKNOWN").containsEntry("wholeMethodCostQualified", false);
        var pending = body(); pending.put("state", "UNKNOWN"); pending.put("pending", 1L);
        rows(pending).put("CONFIG", row(1, 0, 0, 1));
        assertThat(BenchmarkReturnWorkBudgetProducerAllocations.evaluate(pending))
                .containsEntry("pendingOperations", 1L).containsEntry("countPairingComplete", false);
    }

    @Test
    void pairing_regression_and_pending_total_mismatch_are_refused() {
        var bad = body(); rows(bad).put("CONFIG", row(1, 0, 0, 0));
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProducerAllocations.evaluate(bad))
                .isInstanceOf(AssertionError.class).hasMessageContaining("pairing");
        var total = body(); total.put("pending", 1L);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProducerAllocations.evaluate(total))
                .isInstanceOf(AssertionError.class).hasMessageContaining("pending total");
    }

    @Test
    void checked_multiply_and_pairing_add_overflow_refuse_with_the_original_arithmetic_cause() {
        var multiple = body(); rows(multiple).put("MBEAN_BUDGET", row(Long.MAX_VALUE, Long.MAX_VALUE, 0, 0));
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProducerAllocations.evaluate(multiple))
                .isInstanceOf(AssertionError.class).hasCauseInstanceOf(ArithmeticException.class);
        var add = body(); rows(add).put("CONFIG", row(Long.MAX_VALUE, Long.MAX_VALUE, 1, 0));
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProducerAllocations.evaluate(add))
                .isInstanceOf(AssertionError.class).hasCauseInstanceOf(ArithmeticException.class);
    }

    @Test
    void late_omitted_and_overflowed_basis_cannot_be_credited_by_raising_caller_limits() {
        for (String key : List.of("lateMutations", "omitted", "overflow")) {
            var body = body(); body.put(key, key.equals("overflow") ? true : 1L);
            assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProducerAllocations.evaluate(body))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("omitted, late or overflowed");
        }
        var falseCredit = body(); falseCredit.put("limitsIndependentlyVerified", true);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProducerAllocations.evaluate(falseCredit))
                .isInstanceOf(AssertionError.class).hasMessageContaining("unsupported binding");
    }

    @Test
    void forged_qualification_and_large_caller_limits_never_authorize_a_source_upper() {
        var qualified = body(); qualified.put("structuralBudgetQualified", true);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProducerAllocations.evaluate(qualified))
                .isInstanceOf(AssertionError.class).hasMessageContaining("qualification");
        var fabricated = body(); fabricated.put("sourceUpper", vector(0, 0, 0, 0, 0, 0, 0, 0));
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProducerAllocations.evaluate(fabricated))
                .isInstanceOf(AssertionError.class).hasMessageContaining("independent source sum");
        var label = body(); label.put("state", "QUALIFIED");
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProducerAllocations.evaluate(label))
                .isInstanceOf(AssertionError.class).hasMessageContaining("producer state");
    }

    @Test
    void strict_units_integer_types_and_prior_material_constraints_are_checked() {
        var wrongUnit = body(); var vector = new LinkedHashMap<String, Object>((Map<String, Object>) wrongUnit.get("actualMaterial"));
        vector.put("cpuNanos", 0L); wrongUnit.put("actualMaterial", vector);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProducerAllocations.evaluate(wrongUnit))
                .isInstanceOf(AssertionError.class).hasMessageContaining("unit roster");
        var floating = body(); rows(floating).put("CONFIG", Map.of("invocations", 1.0, "completed", 1L, "failed", 0L, "pending", 0L));
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProducerAllocations.evaluate(floating))
                .isInstanceOf(AssertionError.class).hasMessageContaining("strict integer");
        var material = body(); material.put("materialLimits", Map.of("connectorHashBytes", 134217729L,
                "runtimeArguments", 128L, "runtimeArgumentBytes", 2093056L, "primaryKeys", 2L));
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProducerAllocations.evaluate(material))
                .isInstanceOf(AssertionError.class).hasMessageContaining("material constraints");
    }

    @Test
    void mutable_input_changes_cannot_change_the_retained_immutable_evaluation() {
        var body = body(); Map<String, Object> actual = BenchmarkReturnWorkBudgetProducerAllocations.evaluate(body);
        rows(body).put("CONFIG", row(0, 0, 0, 0)); body.put("sourceUpper", vector(0, 0, 0, 0, 0, 0, 0, 0));
        assertThat(actual.get("recomputedSourceUpper")).isEqualTo(vector(3, 598316, 0, 0, 444044, 865, 1018612, 953140));
        assertThatThrownBy(() -> actual.put("wholeMethodCostQualified", true)).isInstanceOf(UnsupportedOperationException.class);
        @SuppressWarnings("unchecked") Map<String, Object> retained = (Map<String, Object>) actual.get("activityBasis");
        assertThatThrownBy(() -> retained.put("FOREIGN", row(0, 0, 0, 0))).isInstanceOf(UnsupportedOperationException.class);
    }

    private static Map<String, Object> body() {
        var body = new LinkedHashMap<String, Object>();
        body.put("schemaVersion", 1); body.put("enabled", true);
        body.put("definitionId", "WRITE_RETURN_PRODUCER_WORK_V1");
        body.put("sourceFormulaSha256", "5e7e3babad7f8e2bd19f22c8ce0aac1ba5164e72e3867d5957cc568a6bfa4c89");
        body.put("inputSha256", "a".repeat(64)); body.put("runId", "owned"); body.put("pid", 17L);
        body.put("jvmStartTimeMillis", 23L); body.put("stage", "SHUTDOWN_HOOK"); body.put("lifecycle", "SHUTDOWN_HOOK");
        body.put("generation", 1L); body.put("nonceSha256", "b".repeat(64)); body.put("state", "RECORDED_STRUCTURAL_DIAGNOSTIC");
        body.put("limits", vector(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE,
                Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE));
        body.put("materialLimits", Map.of("connectorHashBytes", 134217728L, "runtimeArguments", 128L,
                "runtimeArgumentBytes", 2093056L, "primaryKeys", 2L));
        body.put("actualMaterial", vector(0, 0, 0, 0, 0, 0, 0, 0));
        // One CONFIG now includes the fixed transport descriptor: +8 requests, +1680 bytes,
        // +3360 elements and updates. The independent bookkeeping and seal tails are unchanged.
        body.put("sourceUpper", vector(3, 598316, 0, 0, 444044, 865, 1018612, 953140));
        body.put("inputRowsAttempted", 0L); body.put("sourceUpperIsActualWork", false); body.put("limitsIndependentlyVerified", false);
        var rows = new LinkedHashMap<String, Object>();
        for (String actor : List.of("CONFIG", "INSTALL", "WRITER_INSTALL", "WRITER_ROSTER", "WRITER_CLOSE_ATTEMPT", "WRITER_CLOSE",
                "WRITE_WRAPPER", "BEGIN", "ACTIVE_BEGIN", "INACTIVE_BEGIN", "REJECTION", "IDENTITY_ROW", "IDENTITY_PIECE", "KEY_SHAPE",
                "CALLBACK_WRAPPER", "CALLBACK", "CALLBACK_ERROR", "CALLBACK_EXIT", "RETURN_OBSERVATION", "COMPLETE", "RECEIPT_PIECE",
                "PUBLICATION", "COST_AGGREGATION", "CLOCK_READ", "CLOCK_INITIALIZATION", "CLOCK_METADATA", "SCOPE_INSTALL",
                "SCOPE_CACHE_ENTRY", "SCOPE_BEFORE", "SCOPE_AFTER", "TRANSACTION_WRAPPER", "HASH_READ", "HASH_MATERIAL", "ARGUMENT",
                "ARGUMENT_MATERIAL", "MBEAN_SCALAR", "MBEAN_START", "MBEAN_STOP", "MBEAN_PAGE", "PAGE_FRAME_VISIT", "MBEAN_COST",
                "MBEAN_BUDGET", "ARM", "SHUTDOWN", "FINAL_OWNER", "FINAL_WAIT_ITERATION", "BOOKKEEPING", "COLD_ORIGINS")) rows.put(actor, row(0, 0, 0, 0));
        rows.put("CONFIG", row(1, 1, 0, 0)); body.put("activities", rows);
        body.put("bookkeepingInvocations", 1L); body.put("epochs", List.of()); body.put("pending", 0L); body.put("openWriters", 0L);
        body.put("omitted", 0L); body.put("overflow", false); body.put("overBudget", false); body.put("gaps", List.of());
        body.put("lateMutations", 0L); body.put("sourceSealTailUpper", vector(1, 201048, 0, 0, 308032, 843, 751576, 686104));
        body.put("sealTailIncludedInSourceUpper", true); body.put("actualSealTailInCompanion", true);
        body.put("processExitObserved", false); body.put("crashDurabilityQualified", false);
        body.put("opaqueScope", "REQUESTS_AND_BOUNDED_MATERIAL_ONLY");
        body.put("excluded", "OPAQUE_PROVIDER_INSTRUCTIONS_CPU_ELAPSED_AND_VM_EXIT_INTERNALS");
        body.put("costUnit", "STRUCTURAL_WORK_VECTOR_NOT_CPU_OR_ELAPSED");
        for (String flag : List.of("structuralBudgetQualified", "accountingErrorBoundQualified", "wholeMethodCostQualified",
                "collectionCostUpperBoundQualified", "samplingCostQualified", "causalOverheadQualified", "costAcceptanceEligible",
                "performanceAcceptanceEligible", "formalPerformance")) body.put(flag, false);
        return body;
    }
    @SuppressWarnings("unchecked") private static Map<String, Object> rows(Map<String, Object> body) { return (Map<String, Object>) body.get("activities"); }
    private static Map<String, Object> row(long count, long complete, long failed, long pending) {
        return Map.of("invocations", count, "completed", complete, "failed", failed, "pending", pending);
    }
    private static Map<String, Object> vector(long c, long q, long r, long f, long b, long p, long e, long u) {
        return Map.of("calls", c, "requests", q, "records", r, "frames", f, "bytes", b, "passes", p, "elements", e, "updates", u);
    }
}
