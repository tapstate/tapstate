package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkAllWorkloadMethodRecipeTest {
    // Controlled original inputs are shared within this test invocation, outside every work scope.
    private static final java.util.Map<String, BenchmarkWorkloadDefinitions.Workload> ORIGINALS = java.util.Map.of(
            "copy", BenchmarkWorkloadDefinitions.steadyPilot("copy"),
            "stateless", BenchmarkWorkloadDefinitions.steadyPilot("stateless"),
            "stateful", BenchmarkWorkloadDefinitions.steadyPilot("stateful"));
    @Test void each_original_recipe_keeps_exact_topology_phase_geometry_and_instance_bound_workload() {
        for (String id : List.of("copy", "stateless", "stateful")) {
            var workload = ORIGINALS.get(id);
            var prepared = BenchmarkReturnWorkBudgetProfile.inspect(inputs(workload, false));
            var recipe = BenchmarkReturnMethodAdmission.structuralRecipe(BenchmarkReturnWorkBudgetBindings.authenticate(prepared));
            assertThat(recipe.supportedWorkloads()).containsExactly(id);
            assertThat(prepared.inputs().workload()).isEqualTo(BenchmarkReturnWorkBudgetProfile.WorkloadInput.from(workload));
            assertThat(prepared.gaps()).isEmpty();
            assertThat(prepared.inputRegistrationInfo()).containsEntry("wholeMethodCostQualified", false);
            long full = workload.phases().stream().filter(BenchmarkWorkloadDefinitions.Phase::measured)
                    .mapToLong(BenchmarkWorkloadDefinitions.Phase::expectedLogicalOutputChanges).sum();
            assertThat(full).isEqualTo(id.equals("stateful") ? 288000 : 96000);
            assertThat(full / 2).isEqualTo(id.equals("stateful") ? 144000 : 48000);
        }
    }

    @Test void every_original_workload_has_the_same_separate_writer_semantic_route_for_both_arms() {
        for (String id : List.of("copy", "stateless", "stateful")) {
            var workload = ORIGINALS.get(id);
            var a = BenchmarkReturnWorkBudgetProfile.inspect(inputs(workload, true));
            var b = BenchmarkReturnWorkBudgetProfile.inspect(inputs(workload, false));
            assertThat(a.inputRegistrationInfo()).containsEntry("registeredProducerRoute",
                    "A_PLAIN_PERIODIC_NATIVE_DOMAIN_COLLECTOR_ENABLED_CALIBRATION_ABSENT");
            assertThat(a.inputs().workload()).isEqualTo(b.inputs().workload());
            assertThat(a.fieldsRegisteredOutputPlan().nodeUpper()).isGreaterThan(b.fieldsRegisteredOutputPlan().nodeUpper());
            assertThat(a.perOperationUpper().get(BenchmarkReturnWorkBudget.Activity.SORT).elements()).isEqualTo(2L * 262144);
        }
    }

    @Test void original_stateful_cadence_cannot_be_replaced_by_a_same_count_projection() {
        var original = ORIGINALS.get("stateful");
        var input = inputs(original, false); var old = input.workload(); var phases = new ArrayList<>(old.phases());
        var phase = phases.stream().filter(BenchmarkReturnWorkBudgetProfile.PhaseInput::measured).findFirst().orElseThrow();
        phases.set(phases.indexOf(phase), new BenchmarkReturnWorkBudgetProfile.PhaseInput(phase.id(), phase.stage(), phase.measured(),
                phase.logicalOutputs(), phase.statements(), phase.statementsPerBatch(), phase.batches(),
                phase.cadenceNanos() + 1, phase.logicalCoverage(), phase.targets()));
        var changed = new BenchmarkReturnWorkBudgetProfile.WorkloadInput(old.id(), old.rows(), old.seed(), old.database(),
                old.setupStatements(), old.pipelines(), old.sourceChains(), phases);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProfile.inspect(new BenchmarkReturnWorkBudgetProfile.Inputs(
                input.runId(), input.inputSha256(), changed, input.files(), input.flags())))
                .isInstanceOf(AssertionError.class).hasMessageContaining("original pilot phase");
    }

    private static BenchmarkReturnWorkBudgetProfile.Inputs inputs(BenchmarkWorkloadDefinitions.Workload workload, boolean separate) {
        var files = java.util.Arrays.stream(BenchmarkReturnWorkBudgetProfile.FileRole.values())
                .filter(role -> role != BenchmarkReturnWorkBudgetProfile.FileRole.ROOT_CPU_PROVIDER_LIBRARY)
                .map(role -> new BenchmarkReturnWorkBudgetProfile.FileInput(role, "f".repeat(64), 1)).toList();
        var flags = new BenchmarkReturnWorkBudgetProfile.Flags(separate ? "A" : "B", "PLAIN", "PERIODIC", "ON", true, true, true, true, false, Set.of());
        BenchmarkReturnWorkBudgetBindings.ProducerWriterBinding writer = separate
                ? new BenchmarkReturnWorkBudgetBindings.ProducerWriterBinding(BenchmarkReturnWorkBudgetBindings.Owner.PRODUCER,
                        "/controlled/producer-writer", new BenchmarkReturnWorkBudgetBindings.Source(
                                BenchmarkReturnWorkBudgetBindings.SourceRole.PDK_WRITER,
                                BenchmarkReturnWorkBudgetBindings.SourceRole.PDK_WRITER.sourcePath(),
                                new BenchmarkReturnWorkBudgetBindings.Blob("a".repeat(64), 1)),
                        new BenchmarkReturnWorkBudgetBindings.CompiledClass(BenchmarkReturnWorkBudgetBindings.SourceRole.PDK_WRITER,
                                BenchmarkReturnWorkBudgetBindings.SourceRole.PDK_WRITER.binaryName(),
                                new BenchmarkReturnWorkBudgetBindings.Blob("b".repeat(64), 1))) : null;
        return new BenchmarkReturnWorkBudgetProfile.Inputs("controlled-original-" + workload.id(), "e".repeat(64),
                BenchmarkReturnWorkBudgetProfile.WorkloadInput.from(workload), files, flags, writer);
    }
}
