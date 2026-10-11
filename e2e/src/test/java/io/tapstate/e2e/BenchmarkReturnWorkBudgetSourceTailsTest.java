package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnWorkBudgetSourceTailsTest {
    @Test
    void factory_upper_includes_seven_original_constructs_and_all_three_initializer_workloads() {
        var workload = BenchmarkReturnWorkBudgetProfile.WorkloadInput.original("stateless");
        var upper = BenchmarkReturnWorkBudgetSourceTails.factoryUpper(workload);
        var evidence = BenchmarkReturnWorkBudgetSourceTails.factoryEvidence(workload);
        assertThat(evidence).containsEntry("factoryCalls", 7).containsEntry("initializerFactories", 3)
                .containsEntry("actualCompletionObserved", false).containsEntry("futureLifetimeCountBoundClaimed", false);
        assertThat(upper.calls()).isEqualTo(10);
        assertThat(upper.records()).isGreaterThan(7L * 96000);
        assertThat((Long) evidence.get("initializerRecords")).isGreaterThan(3L * 12000);
        assertThat(upper.requests()).isGreaterThan(upper.records());
        assertThat(upper.bytes()).isPositive(); assertThat(upper.elements()).isPositive();
        assertThat(evidence.get("sourceSteps")).asList().contains("BOUNDED_NESTED_ITEM_MATERIAL67_PER_ROOT");
    }

    @Test
    void factory_does_not_accept_changed_seed_rows_or_paced_inputs() {
        var original = BenchmarkReturnWorkBudgetProfile.WorkloadInput.original("stateless");
        var wrongSeed = new BenchmarkReturnWorkBudgetProfile.WorkloadInput(original.id(), original.rows(), 1,
                original.database(), original.setupStatements(), original.pipelines(), original.sourceChains(), original.phases());
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetSourceTails.factoryUpper(wrongSeed)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetSourceTails.factoryUpper(
                BenchmarkReturnWorkBudgetProfile.WorkloadInput.from(BenchmarkWorkloadDefinitions.pacedCalibration("stateless"))))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetSourceTails.factoryUpper(
                BenchmarkReturnWorkBudgetProfile.WorkloadInput.original("stateful"))).isInstanceOf(AssertionError.class);
    }

    @Test
    void terminal_tail_is_more_than_json_and_keeps_verification_finish_print_and_release_explicit() {
        var plan = plan();
        var upper = BenchmarkReturnWorkBudgetSourceTails.terminalUpper(plan);
        var evidence = BenchmarkReturnWorkBudgetSourceTails.terminalEvidence(plan);
        assertThat(evidence).containsEntry("rootActivityRows", 57).containsEntry("selfCounterFields", 20)
                .containsEntry("unitFields", 8).containsEntry("releaseCalls", 1)
                .containsEntry("actualCompletionObserved", false);
        assertThat(upper.requests()).isGreaterThan((Long) evidence.get("renderRequestsUpper"));
        assertThat(upper.bytes()).isGreaterThan(plan.encodedUpperBytes());
        assertThat(evidence.get("sourceSteps")).asList().contains(
                "INDEPENDENT_ADMISSION_57_ACTIVITY_20_SELF_EIGHT_UNIT_VERIFICATION",
                "FINAL_FINISH_ROW_CHECKS_AND_IMMUTABLE_SNAPSHOT",
                "PRIOR_UTF8_UPPER_ONE_PRINTLN_AND_METHOD_RETURN",
                "ONE_SCOPE_CLOSE_COUNTER_AND_ATOMIC_PRIVATE_RELEASE");
    }

    @Test
    void all_six_output_plan_parameters_bind_identity_and_hash_without_observed_encoded_bytes() {
        var original = plan();
        var baseline = BenchmarkReturnWorkBudgetSourceTails.terminalEvidence(original);
        List<BenchmarkReturnWorkBudgetDefinitions.OutputPlan> changes = List.of(
                new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(2001, 12, 512, 4096, 128, 32),
                new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(2000, 13, 512, 4096, 128, 32),
                new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(2000, 12, 513, 4096, 128, 32),
                new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(2000, 12, 512, 4097, 128, 32),
                new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(2000, 12, 512, 4096, 129, 32),
                new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(2000, 12, 512, 4096, 128, 33));
        for (var changed : changes) {
            var actual = BenchmarkReturnWorkBudgetSourceTails.terminalEvidence(changed);
            assertThat(actual.get("outputPlanIdentity")).isNotEqualTo(baseline.get("outputPlanIdentity"));
            assertThat(actual.get("outputPlanSha256")).isNotEqualTo(baseline.get("outputPlanSha256"));
        }
        assertThat(baseline).containsEntry("observedEncodedBytesUsedAsPriorBound", false);
    }

    @Test
    void checked_terminal_formula_refuses_overflow_instead_of_inventing_a_cap() {
        var large = new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(Long.MAX_VALUE / 40, 12, 512, 1, 128, 24);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetSourceTails.terminalUpper(large))
                .isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetSourceTails.terminalUpper(null)).isInstanceOf(AssertionError.class);
    }

    @Test
    void metadata_is_immutable_and_cannot_turn_a_source_tail_into_completion_or_performance_credit() {
        Map<String, Object> actual = BenchmarkReturnWorkBudgetSourceTails.terminalEvidence(plan());
        for (String flag : List.of("actualCompletionObserved", "structuralWorkBudgetQualified", "wholeMethodCostQualified",
                "costAcceptanceEligible", "performanceAcceptanceEligible", "formalPerformance")) {
            assertThat(actual.get(flag)).isEqualTo(false);
        }
        assertThat(actual.get("opaqueExcluded")).isEqualTo("JDK_SORT_PROVIDER_PRINTSTREAM_CPU_ELAPSED_AND_EXTERNAL_SINK_INTERIORS");
        assertThatThrownBy(() -> actual.put("actualCompletionObserved", true)).isInstanceOf(UnsupportedOperationException.class);
        @SuppressWarnings("unchecked") Map<String, Object> identity = (Map<String, Object>) actual.get("outputPlanIdentity");
        assertThat(identity).hasSize(6);
        assertThatThrownBy(() -> identity.put("nodeUpper", Long.MAX_VALUE)).isInstanceOf(UnsupportedOperationException.class);
    }

    private static BenchmarkReturnWorkBudgetDefinitions.OutputPlan plan() {
        return new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(2000, 12, 512, 4096, 128, 32);
    }
}
