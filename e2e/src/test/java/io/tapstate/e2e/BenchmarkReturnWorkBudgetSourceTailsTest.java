package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnWorkBudgetSourceTailsTest {
    @Test
    void factory_upper_includes_seven_original_constructs_three_initializer_workloads_and_four_support_tables() {
        var workload = BenchmarkReturnWorkBudgetProfile.WorkloadInput.original("stateless");
        var upper = BenchmarkReturnWorkBudgetSourceTails.factoryUpper(workload);
        var evidence = BenchmarkReturnWorkBudgetSourceTails.factoryEvidence(workload);
        assertThat(evidence).containsEntry("factoryCalls", 7).containsEntry("initializerFactories", 3)
                .containsEntry("actualCompletionObserved", false).containsEntry("futureLifetimeCountBoundClaimed", false);
        // openFromProperties inspects once and open revalidates once before activation.
        // Both inspections build the shared and cleanup tables.
        // Four new table/catalogue entries, one private cache and one readless initializer add
        // plus two scoped-material tables to the previous fourteen. This is a fixed source tail, not observed completion.
        assertThat(upper.calls()).isEqualTo(23);
        assertThat(evidence).containsEntry("returnComponentTableConstructions", 2)
                .containsEntry("returnComponentCatalogueConstructions", 2)
                .containsEntry("privateAuthenticatedProfileCacheConstructions", 1)
                .containsEntry("knownReadlessAuthorityInitializers", 1);
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
        assertThat(BenchmarkReturnWorkBudgetSourceTails.factoryUpper(
                BenchmarkReturnWorkBudgetProfile.WorkloadInput.original("stateful")).requests()).isPositive();
    }

    @Test void original_geometry_is_source_exact_and_no_selected_factory_count_or_opaque_flags_change() {
        long[] full = {96000, 96000, 288000}; long[] cohort = {48000, 48000, 144000};
        int at = 0;
        for (String id : List.of("copy", "stateless", "stateful")) {
            var input = BenchmarkReturnWorkBudgetProfile.WorkloadInput.original(id);
            var shape = BenchmarkReturnWorkBudgetSourceTails.geometry(input);
            assertThat(shape.phases()).isEqualTo(id.equals("copy") ? 4 : 6);
            assertThat(shape.measuredPhases()).isEqualTo(id.equals("stateful") ? 2 : 1);
            assertThat(shape.measuredOutputs()).isEqualTo(full[at]); assertThat(shape.cohortRows()).isEqualTo(cohort[at++]);
            var evidence = BenchmarkReturnWorkBudgetSourceTails.factoryEvidence(input);
            assertThat(evidence).containsEntry("factoryCalls", 7).containsEntry("initializerFactories", 3)
                    .containsEntry("nativeClassPacketRosterFactories", 1).containsEntry("actualCompletionObserved", false)
                    .containsEntry("wholeMethodCostQualified", false).containsEntry("futureLifetimeCountBoundClaimed", false);
        }
        var stateless = BenchmarkReturnWorkBudgetSourceTails.geometry(BenchmarkReturnWorkBudgetProfile.WorkloadInput.original("stateless"));
        assertThat(stateless.allOutputs()).isEqualTo(240250);
        var stateful = BenchmarkReturnWorkBudgetSourceTails.geometry(BenchmarkReturnWorkBudgetProfile.WorkloadInput.original("stateful"));
        assertThat(stateful.sourceRows()).isEqualTo(384356); assertThat(stateful.targetRows()).isEqualTo(1152002);
        assertThat(stateful.nestedItems()).isEqualTo(865537); assertThat(stateful.nestedRoots()).isEqualTo(576001);
        assertThat(stateful.coverageEntries()).isEqualTo(16);
        assertThat(stateful.statements()).isEqualTo(3470);
    }

    @Test void roster_guard_rejects_before_walk_and_phase_cadence_or_target_mutations_cannot_borrow_source_credit() {
        var input = BenchmarkReturnWorkBudgetProfile.WorkloadInput.original("stateful");
        var phases = new java.util.ArrayList<>(input.phases()); var first = phases.get(3);
        phases.set(3, new BenchmarkReturnWorkBudgetProfile.PhaseInput(first.id(), first.stage(), first.measured(),
                first.logicalOutputs(), first.statements(), first.statementsPerBatch(), first.batches(), 50000000,
                first.logicalCoverage(), first.targets()));
        var changed = new BenchmarkReturnWorkBudgetProfile.WorkloadInput(input.id(), input.rows(), input.seed(), input.database(),
                input.setupStatements(), input.pipelines(), input.sourceChains(), phases);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetSourceTails.geometry(changed))
                .isInstanceOf(AssertionError.class).hasMessageContaining("original phase scalar recipe");
        phases = new java.util.ArrayList<>(input.phases()); phases.add(input.phases().getLast());
        var extra = new BenchmarkReturnWorkBudgetProfile.WorkloadInput(input.id(), input.rows(), input.seed(), input.database(),
                input.setupStatements(), input.pipelines(), input.sourceChains(), phases);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetSourceTails.geometry(extra))
                .isInstanceOf(AssertionError.class).hasMessageContaining("topology");
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
