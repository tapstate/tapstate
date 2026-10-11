package io.tapstate.e2e;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnWorkBudgetRunTest {
    @TempDir Path directory;

    @Test void absent_selection_keeps_the_old_entry_inert_without_reading_registration_or_services() throws Exception {
        try (var saved = new Properties()) {
            assertThat(BenchmarkReturnWorkBudgetRun.open("controlled-invalid-workload")).isNull();
            assertThat(BenchmarkReturnWorkBudgetRun.current()).isNull();
            assertThat(BenchmarkReturnWorkBudget.current()).isNull();
        }
    }

    @Test void noncanonical_selection_refuses_before_any_file_or_workload_lookup() {
        try (var saved = new Properties()) {
            System.setProperty(RealBenchmarkForkDriver.NOMINAL_RETURN_DIAGNOSTICS_PROPERTY, "true");
            for (String value : List.of("false", "TRUE", "invalid", "")) {
                System.setProperty(RealBenchmarkForkDriver.WORK_BUDGET_DIAGNOSTICS_PROPERTY, value);
                assertThatThrownBy(() -> BenchmarkReturnWorkBudgetRun.open("controlled-invalid-workload"))
                        .isInstanceOf(AssertionError.class).hasMessageContaining("work-budget diagnostics");
                assertThat(BenchmarkReturnWorkBudgetRun.current()).isNull();
                assertThat(BenchmarkReturnWorkBudget.current()).isNull();
            }
        }
    }

    @Test void incomplete_scalar_registration_refuses_before_activating_a_ledger_or_accessing_files() {
        try (var saved = new Properties()) {
            System.setProperty(RealBenchmarkForkDriver.WORK_BUDGET_DIAGNOSTICS_PROPERTY, "true");
            System.setProperty(RealBenchmarkForkDriver.NOMINAL_RETURN_DIAGNOSTICS_PROPERTY, "true");
            System.clearProperty(BenchmarkReturnWorkBudgetRun.PREFIX + "file.APP_JAR.path");
            assertThatThrownBy(() -> BenchmarkReturnWorkBudgetRun.open("controlled-invalid-workload"))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("registered scalar is missing");
            assertThat(BenchmarkReturnWorkBudgetRun.current()).isNull();
            assertThat(BenchmarkReturnWorkBudget.current()).isNull();
        }
    }

    @Test void lexical_guard_bounds_nested_decoder_material_before_json_parsing() {
        BenchmarkReturnWorkBudgetDefinitions.requireJsonShape("{\"consumer\":{\"seq\":17,\"active\":true}}", 1024, 4, 12, 128);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetDefinitions.requireJsonShape("[[[[[0]]]]]", 1024, 4, 32, 128))
                .isInstanceOf(AssertionError.class).hasMessageContaining("prior shape bound");
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetDefinitions.requireJsonShape("[0,1,2,3]", 1024, 4, 4, 128))
                .isInstanceOf(AssertionError.class).hasMessageContaining("prior node bound");
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetDefinitions.requireJsonShape("{\"x\":\"abcde\"}", 1024, 4, 12, 4))
                .isInstanceOf(AssertionError.class).hasMessageContaining("prior leaf bound");
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetDefinitions.requireJsonShape("{\"x\":\"abc\"}", 4, 4, 12, 128))
                .isInstanceOf(AssertionError.class).hasMessageContaining("prior character bound");
    }

    @Test void active_rows_and_epoch_windows_must_match_the_complete_original_wire_capture() {
        var captures = List.of(new BenchmarkReturnWorkBudgetRun.Capture(1, "controlled/measured", 2, 3));
        var actors = new LinkedHashMap<String, Object>();
        for (String actor : List.of("ACTIVE_BEGIN", "RETURN_OBSERVATION", "COMPLETE")) {
            actors.put(actor, Map.of("invocations", 2L));
        }
        actors.put("IDENTITY_ROW", Map.of("invocations", 3L));
        var body = new LinkedHashMap<String, Object>();
        body.put("activities", actors); body.put("inputRowsAttempted", 3L);
        body.put("epochs", List.of(Map.of("epoch", 1L, "windowBase64", java.util.Base64.getEncoder()
                .encodeToString("controlled/measured".getBytes(java.nio.charset.StandardCharsets.UTF_8)))));
        BenchmarkReturnWorkBudgetRun.reconcileCaptures(body, captures);
        body.put("inputRowsAttempted", 4L);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetRun.reconcileCaptures(body, captures))
                .isInstanceOf(AssertionError.class).hasMessageContaining("active input rows differ");
        body.put("inputRowsAttempted", 3L);
        actors.put("COMPLETE", Map.of("invocations", 1L));
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetRun.reconcileCaptures(body, captures))
                .isInstanceOf(AssertionError.class).hasMessageContaining("return calls differ");
        actors.put("COMPLETE", Map.of("invocations", 2L));
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetRun.reconcileCaptures(body,
                List.of(new BenchmarkReturnWorkBudgetRun.Capture(2, "controlled/measured", 2, 3))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("epoch or window differs");
    }

    @Test void source_factory_binds_all_actual_registered_files_without_granting_whole_method_credit() throws Exception {
        try (var saved = new Properties()) {
            var fixture = fixture("bound-files");
            configure(fixture);
            assertThat(fixture.paths()).hasSize(9)
                    .doesNotContainKey(BenchmarkReturnWorkBudgetProfile.FileRole.ROOT_CPU_PROVIDER_LIBRARY);
            try (var run = fixture.open()) {
                var scope = BenchmarkReturnWorkBudget.current();
                assertThat(BenchmarkReturnWorkBudgetRun.current()).isSameAs(run);
                assertThat(scope).isNotNull();
                run.requireActive();
                run.requireDriver(fixture.workload(), PipelineBenchmarkComparison.Arm.B, 2, fixture.application());
                var receipt = scope.snapshot();
                assertThat(receipt).containsEntry("state", "OPEN")
                        .containsEntry("runId", fixture.profile().inputs().runId())
                        .containsEntry("structuralWorkBudgetQualified", false)
                        .containsEntry("wholeMethodCostQualified", false);
                assertThat(receipt.get("rootRuntime")).isEqualTo(Map.of("pid", ProcessHandle.current().pid(),
                        "jvmStartTimeMillis", ManagementFactory.getRuntimeMXBean().getStartTime()));
                assertThat(Files.exists(fixture.output())).isFalse();
                assertThat(Files.exists(fixture.body())).isFalse();
            }
            assertInactive();
        }
    }

    @Test void changed_actual_file_length_refuses_and_releases_both_active_references() throws Exception {
        try (var saved = new Properties()) {
            var fixture = fixture("wrong-bytes");
            configure(fixture);
            Files.write(fixture.application(), new byte[] {1}, StandardOpenOption.APPEND);
            assertThatThrownBy(() -> { try (var unexpected = fixture.open()) { } })
                    .isInstanceOf(AssertionError.class).hasMessageContaining("exceeds its prior byte scalar");
            assertInactive();
        }
    }

    @Test void same_length_file_replacement_refuses_on_actual_digest_and_releases_both_active_references() throws Exception {
        try (var saved = new Properties()) {
            var fixture = fixture("wrong-digest");
            configure(fixture);
            byte[] changed = Files.readAllBytes(fixture.application());
            changed[0] ^= 1;
            Files.write(fixture.application(), changed);
            assertThatThrownBy(() -> { try (var unexpected = fixture.open()) { } })
                    .isInstanceOf(AssertionError.class).hasMessageContaining("actual registered file digest differs");
            assertInactive();
        }
    }

    @Test void active_driver_binding_rejects_different_arm_jar_measured_phase_and_fork_without_services() throws Exception {
        try (var saved = new Properties()) {
            var fixture = fixture("driver-binding");
            configure(fixture);
            try (var run = fixture.open()) {
                var scope = BenchmarkReturnWorkBudget.current();
                run.requireDriver(fixture.workload(), PipelineBenchmarkComparison.Arm.B, 2, fixture.application());
                assertThatThrownBy(() -> run.requireDriver(fixture.workload(), PipelineBenchmarkComparison.Arm.A,
                        2, fixture.application())).isInstanceOf(AssertionError.class)
                        .hasMessageContaining("actual driver workload, arm, fork or application differs");
                assertThatThrownBy(() -> run.requireDriver(fixture.workload(), PipelineBenchmarkComparison.Arm.B,
                        2, fixture.output().getParent().resolve("unregistered.jar"))).isInstanceOf(AssertionError.class)
                        .hasMessageContaining("actual driver workload, arm, fork or application differs");
                var changed = changedMeasuredCadence(fixture.workload());
                assertThatThrownBy(() -> run.requireDriver(changed, PipelineBenchmarkComparison.Arm.B,
                        2, fixture.application())).isInstanceOf(AssertionError.class)
                        .hasMessageContaining("actual driver workload, arm, fork or application differs");
                assertThatThrownBy(() -> run.requireDriver(fixture.workload(), PipelineBenchmarkComparison.Arm.B,
                        3, fixture.application())).isInstanceOf(AssertionError.class)
                        .hasMessageContaining("actual driver workload, arm, fork or application differs");
                assertThat(BenchmarkReturnWorkBudgetRun.current()).isSameAs(run);
                assertThat(BenchmarkReturnWorkBudget.current()).isSameAs(scope);
                assertThat(scope.primaryCause()).isNull();
                run.requireDriver(fixture.workload(), PipelineBenchmarkComparison.Arm.B, 2, fixture.application());
            }
            assertInactive();
        }
    }

    @Test void finish_without_an_actual_output_refuses_unknown_and_cleans_the_active_scope() throws Exception {
        try (var saved = new Properties()) {
            var fixture = fixture("missing-output");
            configure(fixture);
            var run = fixture.open();
            var scope = BenchmarkReturnWorkBudget.current();
            try (run) {
                assertThat(scope.snapshot()).containsEntry("state", "OPEN");
                assertThatThrownBy(run::finish).isInstanceOf(AssertionError.class)
                        .hasMessageContaining("root collection is open or refused");
                assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN")
                        .containsEntry("reason", "OUTPUT_NOT_COMPLETED")
                        .containsEntry("wholeMethodCostQualified", false);
                assertThat(scope.primaryCause()).isNull();
                assertThat(Files.exists(fixture.output())).isFalse();
            }
            assertThat(scope.snapshot()).containsEntry("scopeReleased", true);
            assertInactive();
        }
    }

    @Test void finish_refusal_and_cleanup_preserve_the_original_scope_primary() throws Exception {
        try (var saved = new Properties()) {
            var fixture = fixture("primary-failure");
            configure(fixture);
            var run = fixture.open();
            var scope = BenchmarkReturnWorkBudget.current();
            var primary = new IOException("controlled report failure");
            try (run) {
                run.failed(primary);
                assertThatThrownBy(run::finish).isInstanceOf(AssertionError.class)
                        .hasMessageContaining("root collection is open or refused");
                assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN")
                        .containsEntry("reason", "RUN_FAILED")
                        .containsEntry("failureType", IOException.class.getName());
                assertThat(scope.primaryCause()).isSameAs(primary);
            }
            assertThat(scope.primaryCause()).isSameAs(primary);
            assertThat(scope.snapshot()).containsEntry("scopeReleased", true);
            assertInactive();
        }
    }

    private Fixture fixture(String name) throws Exception {
        Path parent = Files.createDirectory(directory.resolve(name)).toRealPath();
        var paths = new EnumMap<BenchmarkReturnWorkBudgetProfile.FileRole, Path>(BenchmarkReturnWorkBudgetProfile.FileRole.class);
        var files = new ArrayList<BenchmarkReturnWorkBudgetProfile.FileInput>();
        for (var role : BenchmarkReturnWorkBudgetProfile.FileRole.values()) {
            if (role == BenchmarkReturnWorkBudgetProfile.FileRole.ROOT_CPU_PROVIDER_LIBRARY) { continue; }
            byte[] content = ("controlled-file:" + role.name() + "\n").getBytes(StandardCharsets.UTF_8);
            Path path = parent.resolve(role.name() + ".bin");
            Files.write(path, content);
            paths.put(role, path);
            files.add(new BenchmarkReturnWorkBudgetProfile.FileInput(role, sha256(content), content.length));
        }
        var workload = BenchmarkWorkloadDefinitions.steadyPilot("stateless");
        var inputs = new BenchmarkReturnWorkBudgetProfile.Inputs("controlled-" + name,
                sha256(("controlled-input:" + name).getBytes(StandardCharsets.UTF_8)),
                BenchmarkReturnWorkBudgetProfile.WorkloadInput.from(workload), files,
                new BenchmarkReturnWorkBudgetProfile.Flags("B", "PLAIN", "PERIODIC", "ON",
                        true, true, true, true, false, Set.of()));
        return new Fixture(workload, BenchmarkReturnWorkBudgetProfile.inspect(inputs), Map.copyOf(paths),
                parent.resolve("fork.json"), parent.resolve("producer.json"));
    }

    private static void configure(Fixture fixture) {
        System.setProperty(RealBenchmarkForkDriver.WORK_BUDGET_DIAGNOSTICS_PROPERTY, "true");
        System.setProperty(RealBenchmarkForkDriver.NOMINAL_RETURN_DIAGNOSTICS_PROPERTY, "true");
        System.setProperty("tapstate.e2e.benchmark-smoke.jar", fixture.application().toString());
        System.setProperty(RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY,
                fixture.paths().get(BenchmarkReturnWorkBudgetProfile.FileRole.NATIVE_LIBRARY).toString());
        System.setProperty("tapstate.e2e.benchmark-smoke.fork", "2");
    }

    private static BenchmarkWorkloadDefinitions.Workload changedMeasuredCadence(BenchmarkWorkloadDefinitions.Workload workload) {
        var phases = new ArrayList<>(workload.phases());
        int index = java.util.stream.IntStream.range(0, phases.size()).filter(i -> phases.get(i).measured()).findFirst().orElseThrow();
        var phase = phases.get(index);
        phases.set(index, new BenchmarkWorkloadDefinitions.Phase(phase.id(), phase.stage(), phase.measured(),
                phase.expectedLogicalOutputChanges(), phase.sql(), phase.statementsPerBatch(), phase.batchInterval().plusNanos(1),
                phase.expectedLogicalCoverage(), phase.targets()));
        return new BenchmarkWorkloadDefinitions.Workload(workload.id(), workload.seed(), workload.rows(), workload.database(),
                workload.pipelineIds(), workload.sourceChains(), workload.setupSql(), phases);
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static void assertInactive() {
        assertThat(BenchmarkReturnWorkBudgetRun.current()).isNull();
        assertThat(BenchmarkReturnWorkBudget.current()).isNull();
    }

    private record Fixture(BenchmarkWorkloadDefinitions.Workload workload, BenchmarkReturnWorkBudgetProfile.Prepared profile,
                           Map<BenchmarkReturnWorkBudgetProfile.FileRole, Path> paths, Path output, Path body) {
        Path application() { return paths.get(BenchmarkReturnWorkBudgetProfile.FileRole.APP_JAR); }
        BenchmarkReturnWorkBudgetRun open() throws IOException {
            return BenchmarkReturnWorkBudgetRun.open(profile, paths, output, body, "controlled-nonce");
        }
    }

    private static final class Properties implements AutoCloseable {
        private final Map<String, String> before = new LinkedHashMap<>();
        Properties() {
            for (String key : List.of(RealBenchmarkForkDriver.WORK_BUDGET_DIAGNOSTICS_PROPERTY,
                    RealBenchmarkForkDriver.NOMINAL_RETURN_DIAGNOSTICS_PROPERTY,
                    "tapstate.e2e.benchmark-smoke.jar", RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY,
                    "tapstate.e2e.benchmark-smoke.fork",
                    BenchmarkReturnWorkBudgetRun.PREFIX + "file.APP_JAR.path")) {
                before.put(key, System.getProperty(key)); System.clearProperty(key);
            }
        }
        @Override public void close() {
            before.forEach((key, value) -> { if (value == null) { System.clearProperty(key); } else { System.setProperty(key, value); } });
        }
    }
}
