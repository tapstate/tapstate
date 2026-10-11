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
import java.io.ByteArrayOutputStream;
import java.util.zip.ZipOutputStream;
import java.util.zip.ZipEntry;
import static io.tapstate.e2e.BenchmarkReturnWorkBudgetBindings.*;
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
                assertThat(run.bindingEvidence()).containsEntry("state", "ACTUAL_DECLARED_BYTES_AND_ROOT_RUNTIME_RECORDED")
                        .containsEntry("wholeMethodCostQualified", false).containsEntry("runtimeOriginAuthenticationQualified", false);
                assertThat(((Map<?, ?>) run.bindingEvidence().get("actualIo")).get("state"))
                        .isEqualTo("DECLARED_BYTE_FACTS_AUTHENTICATED");
                Map<?, ?> actors = (Map<?, ?>) receipt.get("activities");
                assertThat(((Map<?, ?>) actors.get("INSTALL_BINDINGS")).get("completedAttempts")).isEqualTo(1L);
                assertThat(Files.exists(fixture.output())).isFalse();
                assertThat(Files.exists(fixture.body())).isFalse();
            }
            assertInactive();
        }
    }

    @Test void missing_or_foreign_source_root_refuses_before_any_service_can_be_used() throws Exception {
        try (var saved = new Properties()) {
            var fixture = fixture("source-root"); configure(fixture);
            System.clearProperty(BenchmarkReturnWorkBudgetRun.PREFIX + "sourceRoot");
            assertThatThrownBy(() -> fixture.open()).isInstanceOf(AssertionError.class).hasMessageContaining("registered scalar is missing");
            assertInactive();
            System.setProperty(BenchmarkReturnWorkBudgetRun.PREFIX + "sourceRoot", fixture.output().getParent().toString());
            assertThatThrownBy(() -> fixture.open()).isInstanceOf(java.nio.file.NoSuchFileException.class);
            assertInactive();
        }
    }

    @Test void opaque_fake_manifests_are_not_accepted_as_authenticated_source_documents() throws Exception {
        try (var saved = new Properties()) {
            var fixture = fixture("opaque-manifest"); configure(fixture);
            byte[] raw = "opaque-helper".getBytes(StandardCharsets.US_ASCII);
            Files.write(fixture.paths().get(BenchmarkReturnWorkBudgetProfile.FileRole.COMMON_HELPER_MANIFEST), raw);
            var changed = replaceManifest(fixture, BenchmarkReturnWorkBudgetProfile.FileRole.COMMON_HELPER_MANIFEST, raw);
            assertThatThrownBy(changed::open).isInstanceOf(AssertionError.class).hasMessageContaining("manifest");
            assertInactive();
        }
    }

    @Test void helper_document_input_and_root_formula_are_semantically_checked_after_byte_hashing() throws Exception {
        try (var saved = new Properties()) {
            var fixture = fixture("semantic-doc"); configure(fixture); var h = fixture.helper();
            for (boolean input : List.of(true, false)) {
                var changed = new HelperManifest(h.schemaVersion(), h.runId(), input ? "b".repeat(64) : h.inputSha256(),
                        h.buildId(), h.rootDefinitionId(), input ? h.rootFormulaSha256() : "0".repeat(64),
                        h.producerFormulaSha256(), h.failsafeXml(), h.failsafeClasspath(), h.sources(), h.classes(), h.origins());
                byte[] raw = BenchmarkReturnWorkBudgetBindingsIo.helperBytes(changed);
                Files.write(fixture.paths().get(BenchmarkReturnWorkBudgetProfile.FileRole.COMMON_HELPER_MANIFEST), raw);
                var exactHash = replaceManifest(fixture, BenchmarkReturnWorkBudgetProfile.FileRole.COMMON_HELPER_MANIFEST, raw);
                assertThatThrownBy(exactHash::open).isInstanceOf(AssertionError.class)
                        .hasMessageContaining(input ? "manifest run, input or schema differs" : "manifest source definition differs");
                assertInactive();
            }
        }
    }

    @Test void actual_source_and_current_classpath_are_not_borrowed_from_caller_manifest_content() throws Exception {
        try (var saved = new Properties()) {
            var fixture = fixture("actual-source"); configure(fixture);
            Path source = fixture.sourceRoot().resolve(SourceRole.ROOT_RUN.sourcePath());
            byte[] original = Files.readAllBytes(source); byte[] changed = original.clone(); changed[0] ^= 1; Files.write(source, changed);
            assertThatThrownBy(fixture::open).isInstanceOf(AssertionError.class).hasMessageContaining("actual byte digest or length differs");
            assertInactive(); Files.write(source, original);
            System.setProperty("surefire.test.class.path", fixture.home().toString());
            assertThatThrownBy(fixture::open).isInstanceOf(AssertionError.class).hasMessageContaining("current actual ordered classpath differs");
            assertInactive();
        }
    }

    @Test void terminal_separator_is_retained_as_unqualified_token_not_resolved_as_a_cwd_class_origin() throws Exception {
        try (var saved = new Properties()) {
            var original = fixture("terminal-token");
            var entries = new ArrayList<String>(); entries.add(original.classes().toString());
            for (int i = 1; i < MAX_CLASSPATH; i++) entries.add(original.output().getParent().resolve("absent-origin-" + i).toString());
            var h = original.helper();
            var helper = new HelperManifest(h.schemaVersion(), h.runId(), h.inputSha256(), h.buildId(),
                    h.rootDefinitionId(), h.rootFormulaSha256(), h.producerFormulaSha256(), h.failsafeXml(),
                    entries, h.sources(), h.classes(), h.origins());
            byte[] raw = BenchmarkReturnWorkBudgetBindingsIo.helperBytes(helper);
            Path manifest = original.paths().get(BenchmarkReturnWorkBudgetProfile.FileRole.COMMON_HELPER_MANIFEST);
            Files.write(manifest, raw);
            var fixture = replaceManifest(original, BenchmarkReturnWorkBudgetProfile.FileRole.COMMON_HELPER_MANIFEST, raw);
            helper = helperFormula(helper, fixture.profile().requireDefinition().sourceFormulaSha256());
            raw = BenchmarkReturnWorkBudgetBindingsIo.helperBytes(helper); Files.write(manifest, raw);
            fixture = replaceManifest(fixture, BenchmarkReturnWorkBudgetProfile.FileRole.COMMON_HELPER_MANIFEST, raw);
            configure(fixture);
            String classpath = String.join(java.io.File.pathSeparator, entries);
            System.setProperty("surefire.test.class.path", classpath + java.io.File.pathSeparator);
            try (var run = fixture.open()) {
                Map<?, ?> tokens = (Map<?, ?>) run.bindingEvidence().get("classpathTokenEvidence");
                assertThat(tokens.get("terminalEmptyTokens")).isEqualTo(1);
                assertThat(tokens.get("rawTokenCount")).isEqualTo(MAX_CLASSPATH + 1);
                assertThat(tokens.get("runtimeClasspathCompletenessQualified")).isEqualTo(false);
                assertThat(run.bindingEvidence().get("actualOrderedClasspath")).isEqualTo(entries);
            }
            assertInactive();
            System.setProperty("surefire.test.class.path", classpath + java.io.File.pathSeparator
                    + fixture.output().getParent().resolve("extra-origin"));
            var bounded = fixture;
            assertThatThrownBy(bounded::open).isInstanceOf(AssertionError.class).hasMessageContaining("origin roster exceeds");
            assertInactive();
        }
    }

    @Test void cold_class_packets_bind_existing_owned_identity_and_authenticated_application_entries_without_reads() throws Exception {
        try (var saved = new Properties()) {
            var fixture = fixture("cold-class-packets"); configure(fixture);
            try (var run = fixture.open()) {
                var requests = new java.util.concurrent.atomic.AtomicInteger();
                var connection = (javax.management.MBeanServerConnection) java.lang.reflect.Proxy.newProxyInstance(
                        getClass().getClassLoader(), new Class<?>[]{javax.management.MBeanServerConnection.class},
                        (proxy, method, arguments) -> {
                            if (!method.getName().equals("invoke") || !arguments[1].equals("armWorkBudgetFinalReceipt"))
                                throw new AssertionError("unexpected owned metadata operation");
                            requests.incrementAndGet(); return true;
                        });
                var reader = new BenchmarkWriteReturnReader(new BenchmarkCausalClock.Identity(18, 99), connection,
                        () -> true, () -> { throw new AssertionError("unexpected clock read"); });
                run.registerProducer(reader, Map.of("state", "RECORDED_ENABLED", "pid", 18L, "jvmStartTimeMillis", 99L));
                var hashes = new LinkedHashMap<String, Object>();
                fixture.helper().classes().stream().filter(item -> item.role() == SourceRole.PDK_CLOCK)
                        .forEach(item -> hashes.put(item.binaryName().substring(item.binaryName().lastIndexOf('.') + 1), item.blob().sha256()));
                Throwable foreign = org.assertj.core.api.Assertions.catchThrowable(
                        () -> run.recordNativeClassMetadata(nativePacket(19, 99, hashes)));
                assertThat(foreign).isInstanceOf(AssertionError.class).hasMessageContaining("actual cold class runtime differs");
                assertThat(BenchmarkReturnWorkBudget.current().snapshot()).containsEntry("state", "UNKNOWN");
                assertThat(BenchmarkReturnWorkBudget.current().primaryCause()).isSameAs(foreign);
                var changed = new LinkedHashMap<>(hashes); changed.put("PdkBenchmarkClock", "f".repeat(64));
                assertThatThrownBy(() -> run.recordNativeClassMetadata(nativePacket(18, 99, changed)))
                        .isInstanceOf(AssertionError.class).hasMessageContaining("actual cold class hashes differ");
                run.recordNativeClassMetadata(nativePacket(18, 99, hashes));
                run.recordNativeClassMetadata(nativePacket(18, 99, hashes));
                assertThatThrownBy(() -> run.recordNativeClassMetadata(nativePacket(18, 99, hashes)))
                        .isInstanceOf(AssertionError.class).hasMessageContaining("before/after bound");
                assertThat(requests.get()).isEqualTo(1);
                assertThat(BenchmarkReturnWorkBudget.current().snapshot()).containsEntry("state", "UNKNOWN");
                assertThat(BenchmarkReturnWorkBudget.current().primaryCause()).isSameAs(foreign);
                assertThat(run.bindingEvidence()).containsEntry("runtimeInstructionBytesQualified", false)
                        .containsEntry("emittedMemberCompletenessQualified", false);
            }
            assertInactive();
        }
    }

    private static Map<String, Object> nativePacket(long pid, long start, Map<String, Object> hashes) {
        var snapshot = Map.of("classHashes", Map.copyOf(hashes));
        return Map.of("state", "NATIVE_RECORDED", "ownedMetadata", Map.of("pid", pid,
                "jvmStartTimeMillis", start, "before", snapshot, "after", snapshot));
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
        // These owned byte fixtures are authenticated material; they are never loaded or executed as runtime code.
        Path parent = Files.createDirectory(directory.resolve(name)).toRealPath();
        Path sourceRoot = Files.createDirectory(parent.resolve("source")).toRealPath();
        Path classes = Files.createDirectory(parent.resolve("classes")).toRealPath();
        Path home = Files.createDirectory(parent.resolve("jdk")).toRealPath();
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
        var sources = new ArrayList<Source>();
        for (var role : List.of(SourceRole.ROOT_RUN, SourceRole.PDK_CLOCK)) {
            byte[] content = ("controlled owned source:" + role.name()).getBytes(StandardCharsets.US_ASCII);
            Path target = sourceRoot.resolve(role.sourcePath()); Files.createDirectories(target.getParent()); Files.write(target, content);
            sources.add(new Source(role, role.sourcePath(), blob(content)));
        }
        var compiled = new ArrayList<CompiledClass>(); var origins = new ArrayList<Origin>();
        var jarMembers = new LinkedHashMap<String, byte[]>();
        for (var role : List.of(SourceRole.ROOT_RUN, SourceRole.PDK_CLOCK)) {
            for (String suffix : role == SourceRole.PDK_CLOCK ? List.of("", "$Clock", "$JniAccess", "$NativeAccess") : List.of("")) {
                String binary = role.binaryName() + suffix; String member = binary.replace('.', '/') + ".class";
                byte[] content = ("controlled class bytes:" + binary).getBytes(StandardCharsets.US_ASCII);
                compiled.add(new CompiledClass(role, binary, blob(content)));
                Path classFile = classes.resolve(member); Files.createDirectories(classFile.getParent()); Files.write(classFile, content);
                origins.add(new Origin(Owner.ROOT, role, binary, classes.toString(), member, blob(content), null));
                if (role == SourceRole.PDK_CLOCK) jarMembers.put(member, content);
            }
        }
        byte[] nested = zip(jarMembers); byte[] app = zip(Map.of("BOOT-INF/lib/adapter-pdk-controlled.jar", nested));
        Files.write(paths.get(BenchmarkReturnWorkBudgetProfile.FileRole.APP_JAR), app);
        replace(files, BenchmarkReturnWorkBudgetProfile.FileRole.APP_JAR, app);
        for (var item : compiled) if (item.role() == SourceRole.PDK_CLOCK) origins.add(new Origin(Owner.PRODUCER,
                item.role(), item.binaryName(), paths.get(BenchmarkReturnWorkBudgetProfile.FileRole.APP_JAR).toString(),
                "BOOT-INF/lib/adapter-pdk-controlled.jar!/" + item.binaryName().replace('.', '/') + ".class", item.blob(), sha256(app)));
        var jdkFiles = new ArrayList<JdkFile>();
        for (var role : JdkRole.values()) {
            if (role == JdkRole.CPU_PROVIDER) continue;
            Path target = home.resolve(role.relativePath()); Files.createDirectories(target.getParent());
            byte[] content = ("controlled JDK bytes:" + role.name()).getBytes(StandardCharsets.US_ASCII); Files.write(target, content);
            jdkFiles.add(new JdkFile(role, target.toString(), blob(content)));
        }
        var workload = BenchmarkWorkloadDefinitions.steadyPilot("stateless");
        String runId = "controlled-" + name, inputHash = sha256(("controlled-input:" + name).getBytes(StandardCharsets.US_ASCII));
        var jdk = new JdkManifest(1, runId, inputHash, home.toString(), System.getProperty("java.version"), jdkFiles);
        byte[] jdkRaw = BenchmarkReturnWorkBudgetBindingsIo.jdkBytes(jdk);
        Files.write(paths.get(BenchmarkReturnWorkBudgetProfile.FileRole.JDK_INPUT_MANIFEST), jdkRaw);
        replace(files, BenchmarkReturnWorkBudgetProfile.FileRole.JDK_INPUT_MANIFEST, jdkRaw);
        var helper = new HelperManifest(1, runId, inputHash, "controlled-build", BenchmarkReturnWorkBudgetProfile.DEFINITION_ID,
                "0".repeat(64), BenchmarkReturnWorkBudgetProducerAllocations.FORMULA_SHA256, blob("actual-xml".getBytes(StandardCharsets.US_ASCII)),
                List.of(classes.toString()), sources, compiled, origins);
        byte[] helperRaw = BenchmarkReturnWorkBudgetBindingsIo.helperBytes(helper);
        Files.write(paths.get(BenchmarkReturnWorkBudgetProfile.FileRole.COMMON_HELPER_MANIFEST), helperRaw);
        replace(files, BenchmarkReturnWorkBudgetProfile.FileRole.COMMON_HELPER_MANIFEST, helperRaw);
        var inputs = inputs(runId, inputHash, workload, files);
        var profile = BenchmarkReturnWorkBudgetProfile.inspect(inputs);
        helper = helperFormula(helper, profile.requireDefinition().sourceFormulaSha256());
        helperRaw = BenchmarkReturnWorkBudgetBindingsIo.helperBytes(helper);
        Files.write(paths.get(BenchmarkReturnWorkBudgetProfile.FileRole.COMMON_HELPER_MANIFEST), helperRaw);
        replace(files, BenchmarkReturnWorkBudgetProfile.FileRole.COMMON_HELPER_MANIFEST, helperRaw);
        profile = BenchmarkReturnWorkBudgetProfile.inspect(inputs(runId, inputHash, workload, files));
        assertThat(profile.requireDefinition().sourceFormulaSha256()).isEqualTo(helper.rootFormulaSha256());
        return new Fixture(workload, profile, Map.copyOf(paths), parent.resolve("fork.json"), parent.resolve("producer.json"),
                sourceRoot, classes, home, helper, jdk);
    }

    private static BenchmarkReturnWorkBudgetProfile.Inputs inputs(String run, String hash, BenchmarkWorkloadDefinitions.Workload workload,
            List<BenchmarkReturnWorkBudgetProfile.FileInput> files) {
        return new BenchmarkReturnWorkBudgetProfile.Inputs(run, hash, BenchmarkReturnWorkBudgetProfile.WorkloadInput.from(workload), files,
                new BenchmarkReturnWorkBudgetProfile.Flags("B", "PLAIN", "PERIODIC", "ON", true, true, true, true, false, Set.of()));
    }
    private static HelperManifest helperFormula(HelperManifest h, String formula) {
        return new HelperManifest(h.schemaVersion(), h.runId(), h.inputSha256(), h.buildId(), h.rootDefinitionId(), formula,
                h.producerFormulaSha256(), h.failsafeXml(), h.failsafeClasspath(), h.sources(), h.classes(), h.origins());
    }
    private static void replace(List<BenchmarkReturnWorkBudgetProfile.FileInput> files, BenchmarkReturnWorkBudgetProfile.FileRole role,
                                byte[] content) throws Exception {
        for (int i = 0; i < files.size(); i++) if (files.get(i).role() == role) {
            files.set(i, new BenchmarkReturnWorkBudgetProfile.FileInput(role, sha256(content), content.length)); return;
        }
        throw new AssertionError("controlled file role missing");
    }
    private static Fixture replaceManifest(Fixture fixture, BenchmarkReturnWorkBudgetProfile.FileRole role, byte[] raw) throws Exception {
        var files = new ArrayList<>(fixture.profile().inputs().files()); replace(files, role, raw);
        var in = fixture.profile().inputs();
        var profile = BenchmarkReturnWorkBudgetProfile.inspect(new BenchmarkReturnWorkBudgetProfile.Inputs(in.runId(), in.inputSha256(),
                in.workload(), files, in.flags()));
        return new Fixture(fixture.workload(), profile, fixture.paths(), fixture.output(), fixture.body(),
                fixture.sourceRoot(), fixture.classes(), fixture.home(), fixture.helper(), fixture.jdk());
    }
    private static Blob blob(byte[] value) throws Exception { return new Blob(sha256(value), value.length); }
    private static byte[] zip(Map<String, byte[]> values) throws Exception {
        var raw = new ByteArrayOutputStream();
        try (var zip = new ZipOutputStream(raw)) {
            // Stored fixture entries preserve the prior outer-byte bound for every nested member.
            for (var item : values.entrySet()) {
                var crc = new java.util.zip.CRC32(); crc.update(item.getValue());
                var entry = new ZipEntry(item.getKey()); entry.setMethod(ZipEntry.STORED);
                entry.setSize(item.getValue().length); entry.setCompressedSize(item.getValue().length); entry.setCrc(crc.getValue());
                zip.putNextEntry(entry); zip.write(item.getValue()); zip.closeEntry();
            }
        }
        return raw.toByteArray();
    }

    private static void configure(Fixture fixture) {
        System.setProperty(RealBenchmarkForkDriver.WORK_BUDGET_DIAGNOSTICS_PROPERTY, "true");
        System.setProperty(RealBenchmarkForkDriver.NOMINAL_RETURN_DIAGNOSTICS_PROPERTY, "true");
        System.setProperty("tapstate.e2e.benchmark-smoke.jar", fixture.application().toString());
        System.setProperty(RealBenchmarkForkDriver.NATIVE_CLOCK_LIBRARY_PROPERTY,
                fixture.paths().get(BenchmarkReturnWorkBudgetProfile.FileRole.NATIVE_LIBRARY).toString());
        System.setProperty("tapstate.e2e.benchmark-smoke.fork", "2");
        System.setProperty(BenchmarkReturnWorkBudgetRun.PREFIX + "sourceRoot", fixture.sourceRoot().toString());
        System.setProperty("surefire.test.class.path", fixture.classes().toString());
        System.setProperty("java.home", fixture.home().toString());
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
                           Map<BenchmarkReturnWorkBudgetProfile.FileRole, Path> paths, Path output, Path body,
                           Path sourceRoot, Path classes, Path home, HelperManifest helper, JdkManifest jdk) {
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
                    BenchmarkReturnWorkBudgetRun.PREFIX + "sourceRoot", "surefire.test.class.path", "java.home",
                    BenchmarkReturnWorkBudgetRun.PREFIX + "file.APP_JAR.path")) {
                before.put(key, System.getProperty(key)); System.clearProperty(key);
            }
        }
        @Override public void close() {
            before.forEach((key, value) -> { if (value == null) { System.clearProperty(key); } else { System.setProperty(key, value); } });
        }
    }
}
