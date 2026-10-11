package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnWorkBudgetProfileTest {
    private static final String HASH = "a".repeat(64);
    private static final BenchmarkReturnWorkBudgetProfile.WorkloadInput ORIGINAL =
            BenchmarkReturnWorkBudgetProfile.WorkloadInput.original("stateless");

    @Test void registered_scalars_cover_every_source_row_with_a_finite_report_plan_without_method_credit() {
        var prepared = BenchmarkReturnWorkBudgetProfile.inspect(inputs(ORIGINAL, false, files(false)));
        assertThat(prepared.perOperationUpper().get(BenchmarkReturnWorkBudget.Activity.CLOCK_REQUEST))
                .isEqualTo(new BenchmarkReturnWorkBudget.Work(1, 1, 0, 0, 496, 0, 10, 0));
        assertThat(prepared.perOperationUpper().get(BenchmarkReturnWorkBudget.Activity.SORT).elements()).isEqualTo(2L * 48_000);
        assertThat(prepared.perOperationUpper().get(BenchmarkReturnWorkBudget.Activity.SPAN).elements()).isEqualTo(4L * 48_000);
        assertThat(prepared.explicitExcludedActivities()).containsExactly(BenchmarkReturnWorkBudget.Activity.ROOT_CPU);
        assertThat(prepared.gaps()).isEmpty();
        var definition = prepared.requireDefinition();
        assertThat(definition.perOperationUpper().size() + definition.explicitExcludedActivities().size())
                .isEqualTo(BenchmarkReturnWorkBudget.Activity.values().length);
        assertThat(prepared.inputRegistrationInfo()).containsEntry("wholeMethodCostQualified", false)
                .containsEntry("structuralWorkBudgetQualified", false);
        assertThatThrownBy(prepared.inputRegistrationInfo()::clear).isInstanceOf(UnsupportedOperationException.class);
        assertThat(BenchmarkReturnWorkBudget.current()).isNull();
    }

    @Test void altered_original_cadence_is_rejected_before_service_or_scope_activation() {
        var phases = new ArrayList<>(ORIGINAL.phases());
        int measured = 0; while (!phases.get(measured).measured()) { measured++; }
        var phase = phases.get(measured);
        phases.set(measured, new BenchmarkReturnWorkBudgetProfile.PhaseInput(phase.id(), phase.stage(), phase.measured(),
                phase.logicalOutputs(), phase.statements(), phase.statementsPerBatch(), phase.batches(),
                phase.cadenceNanos() + 1, phase.logicalCoverage(), phase.targets()));
        var changed = new BenchmarkReturnWorkBudgetProfile.WorkloadInput(ORIGINAL.id(), ORIGINAL.rows(), ORIGINAL.seed(),
                ORIGINAL.database(), ORIGINAL.setupStatements(), ORIGINAL.pipelines(), ORIGINAL.sourceChains(), phases);
        var services = new AtomicInteger();
        assertThatThrownBy(() -> {
            BenchmarkReturnWorkBudgetProfile.inspect(inputs(changed, false, files(false))).requireDefinition();
            services.incrementAndGet();
        }).isInstanceOf(AssertionError.class).hasMessageContaining("phase, cadence, coverage or batch scalars");
        assertThat(services).hasValue(0); assertThat(BenchmarkReturnWorkBudget.current()).isNull();
    }

    @Test void altered_rows_and_seed_cannot_borrow_the_original_profile_bound() {
        var rows = new BenchmarkReturnWorkBudgetProfile.WorkloadInput(ORIGINAL.id(), 12_000, ORIGINAL.seed(),
                ORIGINAL.database(), ORIGINAL.setupStatements(), ORIGINAL.pipelines(), ORIGINAL.sourceChains(), ORIGINAL.phases());
        var seed = new BenchmarkReturnWorkBudgetProfile.WorkloadInput(ORIGINAL.id(), ORIGINAL.rows(), ORIGINAL.seed() + 1,
                ORIGINAL.database(), ORIGINAL.setupStatements(), ORIGINAL.pipelines(), ORIGINAL.sourceChains(), ORIGINAL.phases());
        for (var changed : List.of(rows, seed)) {
            assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProfile.inspect(inputs(changed, false, files(false))))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("row count or seed");
        }
    }

    @Test void missing_repeated_and_oversized_file_roles_refuse_without_reading_a_file() {
        var missing = new ArrayList<>(files(false)); missing.removeFirst();
        var repeated = new ArrayList<>(files(false)); repeated.add(repeated.getFirst());
        var oversized = new ArrayList<>(files(false));
        int connector = 0; while (oversized.get(connector).role() != BenchmarkReturnWorkBudgetProfile.FileRole.CONNECTOR_JAR_1) { connector++; }
        oversized.set(connector, new BenchmarkReturnWorkBudgetProfile.FileInput(BenchmarkReturnWorkBudgetProfile.FileRole.CONNECTOR_JAR_1,
                HASH, BenchmarkReturnWorkBudget.MAX_CONNECTOR_INPUT_BYTES + 1));
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProfile.inspect(inputs(ORIGINAL, false, missing)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("file scalars");
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProfile.inspect(inputs(ORIGINAL, false, repeated)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("repeated");
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProfile.inspect(inputs(ORIGINAL, false, oversized)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("connector scalar");
        assertThat(BenchmarkReturnWorkBudget.current()).isNull();
    }

    @Test void unsupported_controls_do_not_acquire_a_definition_from_boolean_feature_flags() {
        var flags = new BenchmarkReturnWorkBudgetProfile.Flags("B", "PLAIN", "PERIODIC", "ON", true, true, true, true, false, Set.of("method"));
        var input = new BenchmarkReturnWorkBudgetProfile.Inputs("control", HASH, ORIGINAL, files(false), flags);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProfile.inspect(input)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("unsupported profile mode or conflicting control");
    }

    @Test void full_output_plan_covers_the_independent_raw_cap_and_is_not_an_observed_encoded_size() {
        var prepared = BenchmarkReturnWorkBudgetProfile.inspect(inputs(ORIGINAL, false, files(false)));
        var pages = new ArrayList<String>();
        for (int page = 0; page < 32; page++) { pages.add("A".repeat(4 * ((65_536 + 2) / 3))); }
        var checked = BenchmarkReturnWorkBudgetDefinitions.requireOutputTree(Map.of("pages", pages), prepared.fieldsRegisteredOutputPlan());
        assertThat(checked.stringUtf8Bytes()).isGreaterThan(2L * 1024 * 1024);
        assertThat(prepared.fieldsRegisteredOutputPlan().encodedUpperBytes()).isGreaterThan(checked.stringUtf8Bytes());
        assertThat(Arrays.stream(BenchmarkReturnWorkBudgetProfile.Inputs.class.getRecordComponents()).map(component -> component.getName()))
                .doesNotContain("observedBytes", "actualRequests", "completedAttempts");
        assertThat(prepared.inputRegistrationInfo()).containsEntry("outputPlanScope", "FULL_FORK_CONTROLLED_STDOUT_AND_FAILURE_TREES");
    }

    @Test void root_cpu_requires_its_registered_provider_and_has_a_finite_local_material_row() {
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProfile.inspect(inputs(ORIGINAL, true, files(false))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("file scalars");
        var prepared = BenchmarkReturnWorkBudgetProfile.inspect(inputs(ORIGINAL, true, files(true)));
        assertThat(prepared.explicitExcludedActivities()).doesNotContain(BenchmarkReturnWorkBudget.Activity.ROOT_CPU);
        assertThat(prepared.gaps()).extracting(BenchmarkReturnWorkBudgetProfile.Gap::id).doesNotContain("SOURCE_ROW_ROOT_CPU");
        assertThat(prepared.perOperationUpper().get(BenchmarkReturnWorkBudget.Activity.ROOT_CPU).bytes())
                .isEqualTo(BenchmarkReturnWorkBudget.MAX_MATERIAL_BYTES);
        assertThat(prepared.requireDefinition().perOperationUpper()).containsKey(BenchmarkReturnWorkBudget.Activity.ROOT_CPU);
    }

    private static BenchmarkReturnWorkBudgetProfile.Inputs inputs(BenchmarkReturnWorkBudgetProfile.WorkloadInput workload,
            boolean cpu, List<BenchmarkReturnWorkBudgetProfile.FileInput> files) {
        return new BenchmarkReturnWorkBudgetProfile.Inputs("control", HASH, workload, files,
                new BenchmarkReturnWorkBudgetProfile.Flags("B", "PLAIN", "PERIODIC", "ON", true, true, true, true, cpu, Set.of()));
    }
    private static List<BenchmarkReturnWorkBudgetProfile.FileInput> files(boolean cpu) {
        return Arrays.stream(BenchmarkReturnWorkBudgetProfile.FileRole.values())
                .filter(role -> cpu || role != BenchmarkReturnWorkBudgetProfile.FileRole.ROOT_CPU_PROVIDER_LIBRARY)
                .map(role -> new BenchmarkReturnWorkBudgetProfile.FileInput(role, HASH, 1024 * 1024)).toList();
    }
}
