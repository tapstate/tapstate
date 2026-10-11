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

    @Test void a_native_domain_run_authenticates_the_separate_writer_and_retains_two_original_producer_packets() throws Exception {
        try (var saved = new Properties()) {
            var fixture = fixture("writer-a", true); configure(fixture);
            try (var run = fixture.open()) {
                run.requireActive(); run.requireDriver(fixture.workload(), PipelineBenchmarkComparison.Arm.A, 2, fixture.application());
                assertThat(run.bindingEvidence()).containsEntry("producerWriterRoute",
                        "A_PLAIN_PERIODIC_NATIVE_DOMAIN_COLLECTOR_ENABLED_CALIBRATION_ABSENT")
                        .containsEntry("producerWriter", fixture.helper().producerWriter().evidence()).containsEntry("wholeMethodCostQualified", false);
                var owner = new BenchmarkCausalClock.Identity(18, 99); var armed = new java.util.concurrent.atomic.AtomicInteger();
                var connection = (javax.management.MBeanServerConnection) java.lang.reflect.Proxy.newProxyInstance(
                        getClass().getClassLoader(), new Class<?>[]{javax.management.MBeanServerConnection.class},
                        (proxy, method, arguments) -> { assertThat(method.getName()).isEqualTo("invoke"); armed.incrementAndGet(); return true; });
                var reader = new BenchmarkWriteReturnReader(owner, connection, () -> true, () -> 100);
                run.registerProducer(reader, Map.of("state", "RECORDED_ENABLED", "pid", 18L, "jvmStartTimeMillis", 99L));
                var hashes = new LinkedHashMap<String, Object>();
                for (var o : fixture.helper().origins()) if (o.owner() == Owner.PRODUCER && o.role() == SourceRole.PDK_CLOCK)
                    hashes.put(o.binaryName().substring(o.binaryName().lastIndexOf('.') + 1), o.classBytes().sha256());
                var packet = new LinkedHashMap<>(nativePacket(18, 99, hashes)); packet.put("producerOrigins", producerPacket(fixture, 18, 99));
                run.recordNativeClassMetadata(packet); run.recordNativeClassMetadata(packet);
                assertThat(armed).hasValue(1);
                assertThat((List<?>) run.bindingEvidence().get("actualProducerOrigins")).hasSize(2);
                assertThatThrownBy(() -> run.requireDriver(fixture.workload(), PipelineBenchmarkComparison.Arm.B, 2, fixture.application()))
                        .isInstanceOf(AssertionError.class).hasMessageContaining("actual driver workload, arm, fork or application differs");
                assertThatThrownBy(run::finish).isInstanceOf(AssertionError.class).hasMessageContaining("root collection is open or refused");
            }
            assertInactive();
        }
    }

    @Test void a_source_declaration_or_runtime_route_cannot_replace_actual_writer_authentication() throws Exception {
        try (var saved = new Properties()) {
            var fixture = fixture("writer-a-refusal", true); configure(fixture);
            System.setProperty(RealBenchmarkForkDriver.RETURN_COLLECTOR_CALIBRATION_PROPERTY, "ON");
            assertThatThrownBy(fixture::open).isInstanceOf(AssertionError.class).hasMessageContaining("native domain without collector calibration");
            assertInactive(); System.clearProperty(RealBenchmarkForkDriver.RETURN_COLLECTOR_CALIBRATION_PROPERTY);
            var writer = fixture.helper().producerWriter(); Path source = Path.of(writer.sourceRoot()).resolve(writer.source().path());
            byte[] bytes = Files.readAllBytes(source); bytes[0] ^= 1; Files.write(source, bytes);
            assertThatThrownBy(fixture::open).isInstanceOf(AssertionError.class).hasMessageContaining("actual byte digest or length differs");
            assertInactive();
        }
    }

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
                assertThat(((Map<?, ?>) actors.get("INSTALL_BINDINGS")).get("completedAttempts")).isEqualTo(2L);
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
                var complete = new LinkedHashMap<>(nativePacket(18, 99, hashes));
                complete.put("producerOrigins", producerPacket(fixture, 18, 99));
                assertThat(BenchmarkReturnWorkBudgetRun.coldProducerApplication(BenchmarkReturnWorkBudget.current(),
                        new BenchmarkCausalClock.Identity(18, 99))).isEqualTo(fixture.application().toString());
                assertThat(BenchmarkReturnWorkBudgetRun.coldProducerApplication(null, new BenchmarkCausalClock.Identity(18, 99))).isNull();
                assertThatThrownBy(() -> BenchmarkReturnWorkBudgetRun.coldProducerApplication(BenchmarkReturnWorkBudget.current(),
                        new BenchmarkCausalClock.Identity(19, 99))).isInstanceOf(AssertionError.class)
                        .hasMessageContaining("registered owned producer");
                assertThatThrownBy(() -> run.recordNativeClassMetadata(nativePacket(18, 99, hashes)))
                        .isInstanceOf(AssertionError.class).hasMessageContaining("actual class metadata object is missing");
                var wrongParts = new ArrayList<>((List<String>) producerPacket(fixture, 18, 99).get("rawParts"));
                String writerHash = fixture.helper().classes().stream().filter(item -> item.role() == SourceRole.PDK_WRITER)
                        .findFirst().orElseThrow().blob().sha256();
                wrongParts.set(27, wrongParts.get(27).replace(writerHash, "f".repeat(64)));
                complete.put("producerOrigins", Map.of("rawParts", List.copyOf(wrongParts)));
                assertThatThrownBy(() -> run.recordNativeClassMetadata(complete)).isInstanceOf(AssertionError.class)
                        .hasMessageContaining("actual producer origin differs from authenticated application entries");
                wrongParts.set(0, "{\"changed\":true}");
                complete.put("producerOrigins", Map.of("rawParts", List.copyOf(wrongParts)));
                assertThatThrownBy(() -> run.recordNativeClassMetadata(complete)).isInstanceOf(AssertionError.class)
                        .hasMessageContaining("native part differs from the original cold read");
                complete.put("producerOrigins", producerPacket(fixture, 18, 99));
                run.recordNativeClassMetadata(complete);
                run.recordNativeClassMetadata(complete);
                assertThat(((List<?>) run.bindingEvidence().get("gaps")).contains("ACTUAL_LOADED_ROOT_CLASS_ORIGINS_NOT_OBSERVED")).isTrue();
                assertThatThrownBy(() -> run.recordNativeClassMetadata(complete))
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
        return Map.of("state", "NATIVE_RECORDED", "raw", "{}", "ownedMetadata", Map.of("pid", pid,
                "jvmStartTimeMillis", start, "before", snapshot, "after", snapshot));
    }

    @Test void trusted_cold_packet_uses_one_getter_and_preserves_original_clock_brackets() throws Exception {
        coldGetterControl(false);
    }

    @Test void bounded_ascii_cold_packet_exceeding_the_generic_four_byte_estimate_keeps_an_exact_open_ledger() throws Exception {
        coldGetterControl(true);
    }

    private void coldGetterControl(boolean large) throws Exception {
        try (var saved = new Properties()) {
            var fixture = fixture("cold-getter"); configure(fixture);
            try (var run = fixture.open()) {
                var owner = new BenchmarkCausalClock.Identity(18, 99);
                String library = fixture.paths().get(BenchmarkReturnWorkBudgetProfile.FileRole.NATIVE_LIBRARY).toString();
                var packet = ((List<?>) producerPacket(fixture, 18, 99).get("rawParts")).toArray(String[]::new);
                if (large) makeLargePartialPacket(packet);
                packet[0] = BenchmarkNativeClockEvidenceTest.metadata(owner).replace(BenchmarkNativeClockEvidenceTest.LIBRARY, library);
                var operations = new ArrayList<String>(); var clock = new java.util.concurrent.atomic.AtomicLong(100);
                var connection = (javax.management.MBeanServerConnection) java.lang.reflect.Proxy.newProxyInstance(
                        getClass().getClassLoader(), new Class<?>[]{javax.management.MBeanServerConnection.class},
                        (proxy, method, arguments) -> {
                            if (method.getName().equals("invoke")) {
                                assertThat(arguments[1]).isEqualTo("armWorkBudgetFinalReceipt"); operations.add("arm"); return true;
                            }
                            if (method.getName().equals("getAttributes")) {
                                assertThat((String[]) arguments[1]).containsExactly("Pid", "StartTime", "InputArguments");
                                operations.add("runtime"); var attributes = new javax.management.AttributeList();
                                attributes.add(new javax.management.Attribute("Pid", 18L));
                                attributes.add(new javax.management.Attribute("StartTime", 99L));
                                attributes.add(new javax.management.Attribute("InputArguments", new String[]{
                                        "-Dtapstate.benchmark.write-return=true", "-Dtapstate.benchmark.native-clock-library=" + library}));
                                return attributes;
                            }
                            assertThat(method.getName()).isEqualTo("getAttribute");
                            assertThat(arguments[1]).isEqualTo("ColdMetadata"); operations.add("cold"); return packet;
                        });
                var reader = new BenchmarkWriteReturnReader(owner, connection, () -> true, clock::incrementAndGet);
                run.registerProducer(reader, Map.of("state", "RECORDED_ENABLED", "pid", 18L, "jvmStartTimeMillis", 99L));
                var result = reader.nativeClockEvidence(library, BenchmarkNativeClockEvidenceTest.metadata(
                        BenchmarkNativeClockEvidenceTest.rootOwner()).replace(BenchmarkNativeClockEvidenceTest.LIBRARY, library));
                assertThat(result).containsEntry("state", "NATIVE_RECORDED").containsEntry("raw", packet[0]);
                assertThat(result.get("clockMetadataRead")).isEqualTo(Map.of("startedAtNanos", 103L, "completedAtNanos", 104L));
                assertThat(operations).containsExactly("arm", "runtime", "cold", "runtime"); assertThat(clock).hasValue(106);
                Map<?, ?> origins = (Map<?, ?>) result.get("producerOrigins");
                assertThat(origins.get("rawParts")).isEqualTo(List.of(packet));
                if (large) assertThat(origins.get("state")).isEqualTo("UNKNOWN");
                BenchmarkReturnProducerOrigins.FLAGS.forEach(flag -> assertThat(origins.get(flag)).isEqualTo(false));
                Map<?, ?> activities = (Map<?, ?>) BenchmarkReturnWorkBudget.current().snapshot().get("activities");
                assertThat(((Map<?, ?>) activities.get("DECODE")).get("completedAttempts")).isEqualTo(3L);
                assertThat(BenchmarkReturnWorkBudget.current().snapshot()).containsEntry("state", "OPEN");
            }
            assertInactive();
        }
    }

    @SuppressWarnings("unchecked")
    private static void makeLargePartialPacket(String[] packet) {
        long characters = 0;
        for (int i = 2; i < packet.length; i++) {
            var part = new LinkedHashMap<>((Map<String, Object>) io.tapstate.core.common.JsonReader.parse(packet[i]));
            part.put("state", "UNKNOWN"); part.put("reason", "ORIGIN_READ_REFUSED"); part.put("failureType", IOException.class.getName());
            part.put("codeSource", "file:/" + "a".repeat(18000)); part.put("resource", "file:/" + "b".repeat(18000));
            part.put("sha256", null); part.put("bytes", 0L);
            packet[i] = io.tapstate.core.common.JsonWriter.write(part); characters = Math.addExact(characters, packet[i].length());
        }
        var header = new LinkedHashMap<>((Map<String, Object>) io.tapstate.core.common.JsonReader.parse(packet[1]));
        header.put("state", "UNKNOWN"); header.put("reason", "ORIGIN_READ_REFUSED"); header.put("failureType", IOException.class.getName());
        header.put("completedClassCount", 0); header.put("classBytesConsumed", 0); header.put("classBytesHashed", 0);
        packet[1] = io.tapstate.core.common.JsonWriter.write(header);
        assertThat(characters).isGreaterThan(524288).isLessThan(2080768);
    }

    private static Map<String, Object> producerPacket(Fixture fixture, long pid, long start) throws Exception {
        var runtime = Map.of("pid", pid, "jvmStartTimeMillis", start,
                "javaHome", fixture.jdk().javaHome(), "javaVersion", fixture.jdk().javaVersion());
        var raw = new String[34]; raw[0] = "{}"; long bytes = 0;
        for (int i = 0; i < BenchmarkReturnProducerOrigins.NAMES.size(); i++) {
            String binary = BenchmarkReturnProducerOrigins.NAMES.get(i);
            var origin = fixture.helper().origins().stream().filter(item -> item.owner() == Owner.PRODUCER
                    && item.binaryName().equals(binary)).findFirst().orElseThrow();
            String module = origin.entry().substring(0, origin.entry().indexOf("!/"));
            String entry = binary.replace('.', '/') + ".class";
            String prefix = "jar:nested:" + new java.net.URI(null, null, origin.container(), null).toASCIIString()
                    + "/!" + module + "!/";
            var part = new LinkedHashMap<String, Object>();
            part.put("binaryName", binary); part.put("actualName", binary); part.put("state", "RECORDED");
            part.put("loaderClass", "org.springframework.boot.loader.launch.LaunchedClassLoader"); part.put("loaderIdentityHashCode", 17);
            part.put("codeSource", prefix); part.put("resource", prefix + entry); part.put("container", origin.container());
            part.put("outer", origin.container()); part.put("module", module); part.put("entry", entry);
            part.put("sha256", origin.classBytes().sha256()); part.put("bytes", origin.classBytes().bytes());
            part.put("reason", "NONE"); part.put("failureType", "NONE");
            BenchmarkReturnProducerOrigins.FLAGS.forEach(flag -> part.put(flag, false));
            raw[i + 2] = io.tapstate.core.common.JsonWriter.write(part); bytes = Math.addExact(bytes, origin.classBytes().bytes());
        }
        var header = new LinkedHashMap<String, Object>(); header.put("schemaVersion", 1); header.put("state", "RECORDED_UNQUALIFIED");
        header.putAll(runtime); header.put("beforeRuntime", runtime); header.put("afterRuntime", runtime);
        header.put("expectedClassCount", 32); header.put("completedClassCount", 32); header.put("serviceRequests", 0);
        header.put("classBytesConsumed", bytes); header.put("classBytesHashed", bytes); header.put("reason", "NONE");
        header.put("failureType", "NONE"); header.put("suppressedFailureTypes", List.of());
        header.put("resolution", "EXACT_NAMES_INITIALIZE_FALSE");
        header.put("byteScope", "OWN_RESOURCE_CODE_SOURCE_AND_CONTAINER_NOT_VM_INSTRUCTION_BODY");
        BenchmarkReturnProducerOrigins.FLAGS.forEach(flag -> header.put(flag, false));
        raw[1] = io.tapstate.core.common.JsonWriter.write(header);
        return Map.of("rawParts", List.of(raw));
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

    private Fixture fixture(String name) throws Exception { return fixture(name, false); }

    private Fixture fixture(String name, boolean separateWriter) throws Exception {
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
        var sourceRoles = new java.util.LinkedHashSet<SourceRole>(); sourceRoles.add(SourceRole.ROOT_RUN);
        for (String binary : BenchmarkReturnProducerOrigins.NAMES) sourceRoles.add(producerRole(binary));
        for (var role : sourceRoles) {
            byte[] content = ("controlled owned source:" + role.name()).getBytes(StandardCharsets.US_ASCII);
            Path target = sourceRoot.resolve(role.sourcePath()); Files.createDirectories(target.getParent()); Files.write(target, content);
            sources.add(new Source(role, role.sourcePath(), blob(content)));
        }
        var compiled = new ArrayList<CompiledClass>(); var origins = new ArrayList<Origin>();
        var pdkMembers = new LinkedHashMap<String, byte[]>(); var commonMembers = new LinkedHashMap<String, byte[]>();
        var binaries = new ArrayList<>(BenchmarkReturnProducerOrigins.NAMES); binaries.add(SourceRole.ROOT_RUN.binaryName());
        for (String binary : binaries) {
                var role = binary.equals(SourceRole.ROOT_RUN.binaryName()) ? SourceRole.ROOT_RUN : producerRole(binary);
                String member = binary.replace('.', '/') + ".class";
                byte[] content = ("controlled class bytes:" + binary).getBytes(StandardCharsets.US_ASCII);
                compiled.add(new CompiledClass(role, binary, blob(content)));
                Path classFile = classes.resolve(member); Files.createDirectories(classFile.getParent()); Files.write(classFile, content);
                origins.add(new Origin(Owner.ROOT, role, binary, classes.toString(), member, blob(content), null));
                if (role.module().equals("adapter-pdk")) pdkMembers.put(member, content);
                else if (role.module().equals("core-common")) commonMembers.put(member, content);
        }
        ProducerWriterBinding writer = null;
        if (separateWriter) {
            Path producerRoot = Files.createDirectory(parent.resolve("producer-source")).toRealPath();
            byte[] source = "controlled separate producer source".getBytes(StandardCharsets.US_ASCII);
            byte[] content = "controlled separate producer writer class".getBytes(StandardCharsets.US_ASCII);
            Path sourceFile = producerRoot.resolve(SourceRole.PDK_WRITER.sourcePath()); Files.createDirectories(sourceFile.getParent()); Files.write(sourceFile, source);
            writer = new ProducerWriterBinding(Owner.PRODUCER, producerRoot.toString(),
                    new Source(SourceRole.PDK_WRITER, SourceRole.PDK_WRITER.sourcePath(), blob(source)),
                    new CompiledClass(SourceRole.PDK_WRITER, SourceRole.PDK_WRITER.binaryName(), blob(content)));
            Path compilerFile = Path.of(writer.compilerPath()); Files.createDirectories(compilerFile.getParent()); Files.write(compilerFile, content);
            pdkMembers.put(writer.compilerEntry(), content);
        }
        byte[] app = zip(Map.of("BOOT-INF/lib/adapter-pdk-controlled.jar", zip(pdkMembers),
                "BOOT-INF/lib/core-common-controlled.jar", zip(commonMembers)));
        Files.write(paths.get(BenchmarkReturnWorkBudgetProfile.FileRole.APP_JAR), app);
        replace(files, BenchmarkReturnWorkBudgetProfile.FileRole.APP_JAR, app);
        for (var item : compiled) if (item.role().owners().contains(Owner.PRODUCER)) origins.add(new Origin(Owner.PRODUCER,
                item.role(), item.binaryName(), paths.get(BenchmarkReturnWorkBudgetProfile.FileRole.APP_JAR).toString(),
                "BOOT-INF/lib/" + item.role().module() + "-controlled.jar!/" + item.binaryName().replace('.', '/') + ".class",
                writer != null && item.role() == SourceRole.PDK_WRITER ? writer.compiled().blob() : item.blob(), sha256(app)));
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
        var helper = new HelperManifest(writer == null ? SCHEMA_VERSION : PRODUCER_WRITER_SCHEMA_VERSION, runId, inputHash, "controlled-build", BenchmarkReturnWorkBudgetProfile.DEFINITION_ID,
                "0".repeat(64), BenchmarkReturnWorkBudgetProducerAllocations.FORMULA_SHA256, blob("actual-xml".getBytes(StandardCharsets.US_ASCII)),
                List.of(classes.toString()), sources, compiled, origins, writer);
        byte[] helperRaw = BenchmarkReturnWorkBudgetBindingsIo.helperBytes(helper);
        Files.write(paths.get(BenchmarkReturnWorkBudgetProfile.FileRole.COMMON_HELPER_MANIFEST), helperRaw);
        replace(files, BenchmarkReturnWorkBudgetProfile.FileRole.COMMON_HELPER_MANIFEST, helperRaw);
        var inputs = inputs(runId, inputHash, workload, files, writer);
        var profile = BenchmarkReturnWorkBudgetProfile.inspect(inputs);
        helper = helperFormula(helper, profile.requireDefinition().sourceFormulaSha256());
        helperRaw = BenchmarkReturnWorkBudgetBindingsIo.helperBytes(helper);
        Files.write(paths.get(BenchmarkReturnWorkBudgetProfile.FileRole.COMMON_HELPER_MANIFEST), helperRaw);
        replace(files, BenchmarkReturnWorkBudgetProfile.FileRole.COMMON_HELPER_MANIFEST, helperRaw);
        profile = BenchmarkReturnWorkBudgetProfile.inspect(inputs(runId, inputHash, workload, files, writer));
        assertThat(profile.requireDefinition().sourceFormulaSha256()).isEqualTo(helper.rootFormulaSha256());
        return new Fixture(workload, profile, Map.copyOf(paths), parent.resolve("fork.json"), parent.resolve("producer.json"),
                sourceRoot, classes, home, helper, jdk);
    }

    private static SourceRole producerRole(String binary) {
        String top = binary.split("\\$", 2)[0];
        return java.util.Arrays.stream(SourceRole.values()).filter(role -> top.equals(role.binaryName())).findFirst().orElseThrow();
    }

    private static BenchmarkReturnWorkBudgetProfile.Inputs inputs(String run, String hash, BenchmarkWorkloadDefinitions.Workload workload,
            List<BenchmarkReturnWorkBudgetProfile.FileInput> files) {
        return inputs(run, hash, workload, files, null);
    }
    private static BenchmarkReturnWorkBudgetProfile.Inputs inputs(String run, String hash, BenchmarkWorkloadDefinitions.Workload workload,
            List<BenchmarkReturnWorkBudgetProfile.FileInput> files, ProducerWriterBinding writer) {
        return new BenchmarkReturnWorkBudgetProfile.Inputs(run, hash, BenchmarkReturnWorkBudgetProfile.WorkloadInput.from(workload), files,
                new BenchmarkReturnWorkBudgetProfile.Flags(writer == null ? "B" : "A", "PLAIN", "PERIODIC", "ON", true, true, true, true, false, Set.of()), writer);
    }
    private static HelperManifest helperFormula(HelperManifest h, String formula) {
        return new HelperManifest(h.schemaVersion(), h.runId(), h.inputSha256(), h.buildId(), h.rootDefinitionId(), formula,
                h.producerFormulaSha256(), h.failsafeXml(), h.failsafeClasspath(), h.sources(), h.classes(), h.origins(), h.producerWriter());
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
                in.workload(), files, in.flags(), in.producerWriter()));
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
        if (fixture.helper().producerWriter() != null) {
            System.setProperty(RealBenchmarkForkDriver.NATIVE_COUNTER_DOMAIN_PROPERTY, "true");
            System.clearProperty(RealBenchmarkForkDriver.RETURN_COLLECTOR_CALIBRATION_PROPERTY);
        }
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
                    RealBenchmarkForkDriver.NATIVE_COUNTER_DOMAIN_PROPERTY, RealBenchmarkForkDriver.RETURN_COLLECTOR_CALIBRATION_PROPERTY,
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
