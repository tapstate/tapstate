package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import io.tapstate.e2e.BenchmarkReturnWorkBudgetBindings.Runtime;
import org.junit.jupiter.api.Test;

import static io.tapstate.e2e.BenchmarkReturnWorkBudgetBindings.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnAfterReceiptTest {
    private static final Fixture FIXTURE = fixture();

    @Test void independent_source_sum_matches_complete_inputs_without_a_root_os_exit_requirement() {
        var receipt = verify(FIXTURE.inputs());
        var e = receipt.evidence();
        assertThat(e).containsEntry("state", "COLLECTION_METHOD_AFTER_FACTS_MATCHED_DIAGNOSTIC_ONLY")
                .containsEntry("sourceUpperIsActualWork", false);
        assertThat(((Map<?, ?>) e.get("rootOsTermination")).get("state")).isEqualTo("UNKNOWN");
        BenchmarkReturnProducerOrigins.FLAGS.forEach(flag -> assertThat(e.get(flag)).isEqualTo(false));
        var sum = BenchmarkReturnWorkBudget.Work.zero();
        for (Object value : ((Map<?, ?>) e.get("partitions")).values()) sum = sum.plus(vector(value));
        assertThat(receipt.sourceUpper()).isEqualTo(sum);
        assertThatThrownBy(e::clear).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(((Map<?, ?>) e.get("partitions"))::clear).isInstanceOf(UnsupportedOperationException.class);
        assertThat(BenchmarkReturnWorkBudget.current()).isNull();
    }

    @Test void duplicate_partial_missing_and_borrowed_final_markers_are_refused() {
        var original = FIXTURE.inputs(); String finalLine = utf8(original.protocolStdout().bytes());
        for (String output : List.of("normal output\n", finalLine + finalLine,
                finalLine.substring(0, finalLine.length() - 1), finalLine.replace("CLOSED", "UNKNOWN"))) {
            var xml = fact("/controlled/current.xml", xml(output, 1, "selected", false));
            assertThatThrownBy(() -> verify(inputs(original.closedSnapshot(), original.report(), original.protocolStdout(), xml,
                    original.actualFailsafeRoot(), original.producer(), original.originalProducerOutcome())))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("final marker");
        }
        for (String protocol : List.of(finalLine + finalLine, finalLine.substring(0, finalLine.length() - 1))) {
            assertThatThrownBy(() -> verify(inputs(original.closedSnapshot(), original.report(), fact("/controlled/final.txt", protocol),
                    original.failsafeXml(), original.actualFailsafeRoot(), original.producer(), original.originalProducerOutcome())))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("partial, duplicated");
        }
    }

    @Test void exact_integer_owner_values_survive_json_wrapper_normalization() {
        var original = FIXTURE.inputs(); var closed = mutable(original.closedSnapshot());
        var owner = object(closed.get("rootRuntime"));
        owner.put("pid", 11); owner.put("jvmStartTimeMillis", 1000);
        assertThat(verify(inputs(closed, original.report(), original.protocolStdout(), original.failsafeXml(),
                original.actualFailsafeRoot(), original.producer(), original.originalProducerOutcome())).evidence())
                .containsEntry("state", "COLLECTION_METHOD_AFTER_FACTS_MATCHED_DIAGNOSTIC_ONLY");
    }

    @Test void acquired_producer_bytes_and_cached_semantics_are_checked_independently() {
        var original = FIXTURE.inputs(); byte[] changed = original.producerBody().bytes().clone(); changed[0] ^= 1;
        var changedFact = new BenchmarkReturnAfterReceipt.ByteFact(original.producerBody().path(),
                original.producerBody().fingerprint(), changed);
        assertThatThrownBy(() -> verify(new BenchmarkReturnAfterReceipt.Inputs(original.closedSnapshot(), original.report(),
                original.protocolStdout(), original.failsafeXml(), original.actualFailsafeRoot(), original.bindings(),
                changedFact, original.producer(), original.originalProducerOutcome())))
                .isInstanceOf(AssertionError.class).hasMessageContaining("byte hash differs");
        var body = mutable(original.producer().body()); body.put("inputRowsAttempted", 1L);
        var changedBound = new BenchmarkReturnWorkBudgetProducerSeal.Bound(body, original.producer().bodySha256(),
                original.producer().companionSha256(), original.producer().bodyBytes(), original.producer().generation(),
                original.producer().mappingForceAttempts());
        assertThatThrownBy(() -> verify(new BenchmarkReturnAfterReceipt.Inputs(original.closedSnapshot(), original.report(),
                original.protocolStdout(), original.failsafeXml(), original.actualFailsafeRoot(), original.bindings(),
                original.producerBody(), changedBound, original.originalProducerOutcome())))
                .isInstanceOf(AssertionError.class).hasMessageContaining("bound body content differs");
    }

    @Test void wrong_case_failure_duplicate_test_and_foreign_root_cannot_replace_the_actual_outcome() {
        var original = FIXTURE.inputs(); String line = utf8(original.protocolStdout().bytes());
        for (String raw : List.of(xml(line, 1, "foreign", false), xml(line, 2, "selected", false), xml(line, 1, "selected", true))) {
            assertThatThrownBy(() -> verify(inputs(original.closedSnapshot(), original.report(), original.protocolStdout(),
                    fact("/controlled/current.xml", raw), original.actualFailsafeRoot(), original.producer(), original.originalProducerOutcome())))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("Failsafe");
        }
        var foreign = new BenchmarkReturnWorkBudgetAdmission.Runtime("after-controlled", 12, 1000);
        assertThatThrownBy(() -> verify(inputs(original.closedSnapshot(), original.report(), original.protocolStdout(), original.failsafeXml(),
                foreign, original.producer(), original.originalProducerOutcome())))
                .isInstanceOf(AssertionError.class).hasMessageContaining("root identity differs");
    }

    @Test void observed_bytes_or_qualification_flags_cannot_replace_the_registered_report() {
        var original = FIXTURE.inputs(); byte[] changed = original.report().bytes().clone(); changed[0] ^= 1;
        assertThatThrownBy(() -> verify(inputs(original.closedSnapshot(),
                new BenchmarkReturnAfterReceipt.ByteFact(original.report().path(), original.report().fingerprint(), changed),
                original.protocolStdout(), original.failsafeXml(), original.actualFailsafeRoot(), original.producer(), original.originalProducerOutcome())))
                .isInstanceOf(AssertionError.class).hasMessageContaining("byte hash differs");
        var closed = mutable(original.closedSnapshot()); closed.put("formalPerformance", true);
        var claimed = closed;
        assertThatThrownBy(() -> verify(inputs(claimed, original.report(), original.protocolStdout(), original.failsafeXml(),
                original.actualFailsafeRoot(), original.producer(), original.originalProducerOutcome())))
                .isInstanceOf(AssertionError.class).hasMessageContaining("claimed a qualification");
        closed = mutable(original.closedSnapshot()); object(closed.get("output")).put("priorTreePlanSha256", "f".repeat(64));
        var wrongPlan = closed;
        assertThatThrownBy(() -> verify(inputs(wrongPlan, original.report(), original.protocolStdout(), original.failsafeXml(),
                original.actualFailsafeRoot(), original.producer(), original.originalProducerOutcome())))
                .isInstanceOf(AssertionError.class).hasMessageContaining("prior plan differs");
    }

    @Test void tampered_self_upper_and_incomplete_or_overflowed_root_counts_are_refused() {
        var original = FIXTURE.inputs();
        var closed = mutable(original.closedSnapshot()); object(closed.get("selfSourceUpper")).put("requests", 0L);
        var badSelf = closed;
        assertThatThrownBy(() -> verify(inputs(badSelf, original.report(), original.protocolStdout(), original.failsafeXml(),
                original.actualFailsafeRoot(), original.producer(), original.originalProducerOutcome())))
                .isInstanceOf(AssertionError.class).hasMessageContaining("self upper");
        closed = mutable(original.closedSnapshot()); closed.put("pendingOperations", 1L); var pending = closed;
        assertThatThrownBy(() -> verify(inputs(pending, original.report(), original.protocolStdout(), original.failsafeXml(),
                original.actualFailsafeRoot(), original.producer(), original.originalProducerOutcome())))
                .isInstanceOf(AssertionError.class).hasMessageContaining("pending");
        closed = mutable(original.closedSnapshot()); var row = object(object(closed.get("activities")).get("INSTALL_BINDINGS"));
        row.put("attempts", Long.MAX_VALUE); row.put("completedAttempts", Long.MAX_VALUE); var overflow = closed;
        assertThatThrownBy(() -> verify(inputs(overflow, original.report(), original.protocolStdout(), original.failsafeXml(),
                original.actualFailsafeRoot(), original.producer(), original.originalProducerOutcome())))
                .isInstanceOf(AssertionError.class).hasMessageContaining("arithmetic overflowed");
    }

    @Test void an_unowned_or_refused_producer_cannot_become_a_completed_after_receipt() {
        var original = FIXTURE.inputs(); var outcome = new LinkedHashMap<>(original.originalProducerOutcome());
        outcome.put("exitEvidenceSource", "PID_NOT_FOUND");
        assertThatThrownBy(() -> verify(inputs(original.closedSnapshot(), original.report(), original.protocolStdout(), original.failsafeXml(),
                original.actualFailsafeRoot(), original.producer(), outcome))).isInstanceOf(AssertionError.class).hasMessageContaining("owned exit");
        var body = mutable(original.producer().body()); body.put("state", "UNKNOWN"); var refused = bound(body);
        var refusedInputs = new BenchmarkReturnAfterReceipt.Inputs(original.closedSnapshot(), original.report(), original.protocolStdout(),
                original.failsafeXml(), original.actualFailsafeRoot(), original.bindings(),
                fact("/controlled/producer.json", JsonWriter.write(body)), refused, original.originalProducerOutcome());
        assertThatThrownBy(() -> verify(refusedInputs))
                .isInstanceOf(AssertionError.class).hasMessageContaining("producer completion is refused");
    }

    private static BenchmarkReturnAfterReceipt.Receipt verify(BenchmarkReturnAfterReceipt.Inputs inputs) {
        return BenchmarkReturnAfterReceipt.verify(FIXTURE.registration(), inputs);
    }
    private static BenchmarkReturnAfterReceipt.Inputs inputs(Map<String, Object> closed, BenchmarkReturnAfterReceipt.ByteFact report,
            BenchmarkReturnAfterReceipt.ByteFact stdout, BenchmarkReturnAfterReceipt.ByteFact xml,
            BenchmarkReturnWorkBudgetAdmission.Runtime root, BenchmarkReturnWorkBudgetProducerSeal.Bound producer, Map<String, Object> exit) {
        return new BenchmarkReturnAfterReceipt.Inputs(closed, report, stdout, xml, root, FIXTURE.inputs().bindings(),
                FIXTURE.inputs().producerBody(), producer, exit);
    }

    private record Fixture(BenchmarkReturnAfterReceipt.Registration registration, BenchmarkReturnAfterReceipt.Inputs inputs) { }
    private static Fixture fixture() {
        String run = "after-controlled", input = "a".repeat(64), home = "/controlled/jdk";
        var files = new ArrayList<FileFact>(); var scalars = new ArrayList<BenchmarkReturnWorkBudgetProfile.FileInput>();
        for (var role : BenchmarkReturnWorkBudgetProfile.FileRole.values()) {
            if (role == BenchmarkReturnWorkBudgetProfile.FileRole.ROOT_CPU_PROVIDER_LIBRARY) continue;
            var blob = blob("file-" + role);
            files.add(new FileFact(role, "/controlled/" + role + ".bin", blob));
            scalars.add(new BenchmarkReturnWorkBudgetProfile.FileInput(role, blob.sha256(), blob.bytes()));
        }
        var profile = BenchmarkReturnWorkBudgetProfile.inspect(new BenchmarkReturnWorkBudgetProfile.Inputs(run, input,
                BenchmarkReturnWorkBudgetProfile.WorkloadInput.original("stateless"), scalars,
                new BenchmarkReturnWorkBudgetProfile.Flags("B", "PLAIN", "PERIODIC", "ON", true, true, true, true, false, Set.of())));
        var authenticated = BenchmarkReturnWorkBudgetBindings.authenticate(profile); var definition = authenticated.definition();
        var root = new Runtime(Owner.ROOT, run, 11, 1000, home, "21.0.9");
        var producer = new Runtime(Owner.PRODUCER, run, 17, 2000, home, "21.0.9");
        var owners = new OwnedRuntimes(root, producer); var sources = new ArrayList<Source>(); var classes = new ArrayList<CompiledClass>();
        for (var role : SourceRole.values()) {
            sources.add(new Source(role, role.sourcePath(), blob("source-" + role)));
            if (role.binaryName() != null) classes.add(new CompiledClass(role, role.binaryName(), blob("class-" + role)));
        }
        for (String suffix : List.of("$Clock", "$JniAccess", "$NativeAccess"))
            classes.add(new CompiledClass(SourceRole.PDK_CLOCK, SourceRole.PDK_CLOCK.binaryName() + suffix, blob("clock" + suffix)));
        List<String> classpath = List.of("/controlled/e2e", "/controlled/adapter-pdk.jar", "/controlled/core-common.jar",
                "/controlled/test-support.jar");
        var origins = new ArrayList<Origin>(); var observed = new ArrayList<ObservedOrigin>();
        var app = files.stream().filter(f -> f.role() == BenchmarkReturnWorkBudgetProfile.FileRole.APP_JAR).findFirst().orElseThrow();
        for (var c : classes) {
            String entry = c.binaryName().replace('.', '/') + ".class";
            String container = c.role().module().equals("e2e") ? classpath.getFirst() : "/controlled/" + c.role().module() + ".jar";
            var o = new Origin(Owner.ROOT, c.role(), c.binaryName(), container, entry, c.blob(), null);
            origins.add(o); observed.add(new ObservedOrigin(o, root));
            if (c.role().owners().contains(Owner.PRODUCER)) {
                o = new Origin(Owner.PRODUCER, c.role(), c.binaryName(), app.path(),
                        "BOOT-INF/lib/" + c.role().module() + "-1.jar!/" + entry, c.blob(), app.blob().sha256());
                origins.add(o); observed.add(new ObservedOrigin(o, producer));
            }
        }
        var jdkFiles = new ArrayList<JdkFile>();
        for (var role : JdkRole.values()) if (role != JdkRole.CPU_PROVIDER)
            jdkFiles.add(new JdkFile(role, home + "/" + role.relativePath(), blob("jdk-" + role)));
        var preflightXml = blob("preflight");
        var helper = new HelperManifest(1, run, input, "controlled-build", definition.definitionId(), definition.sourceFormulaSha256(),
                BenchmarkReturnWorkBudgetProducerAllocations.FORMULA_SHA256, preflightXml, classpath, sources, classes, origins);
        var jdk = new JdkManifest(1, run, input, home, "21.0.9", jdkFiles);
        var actual = new ActualFacts(run, input, "controlled-build", Lifecycle.FAILSAFE_VERIFY, preflightXml, classpath,
                files, sources, classes, observed, jdkFiles, Set.of());
        var binding = new BenchmarkReturnAfterReceipt.BindingFacts(helper, jdk, actual, owners,
                List.of("ACTUAL_IO_AND_VM_TRANSFORMED_BODY_PROVENANCE_REMAINS_EXTERNAL"));
        var rootIdentity = new BenchmarkReturnWorkBudgetAdmission.Runtime(run, 11, 1000);
        var producerRegistration = new BenchmarkReturnWorkBudgetProducerSeal.Registration(run,
                new BenchmarkCausalClock.Identity(17, 2000), input, "nonce", app.blob().sha256());
        var plan = profile.fieldsRegisteredOutputPlan();
        var registration = new BenchmarkReturnAfterReceipt.Registration(run, input, authenticated, rootIdentity, producerRegistration,
                profile.inputs().workload(), plan, "/controlled/report.json", "/controlled/producer.json", "io.tapstate.e2e.SelectedIT", "selected",
                BenchmarkReturnRootClockWork.coldInitializationUpper(), BenchmarkReturnWorkBudgetSourceTails.factoryUpper(profile.inputs().workload()),
                BenchmarkReturnWorkBudgetSourceTails.terminalUpper(plan));
        final byte[][] reportBytes = new byte[1][]; Map<String, Object> closed;
        try (var scope = BenchmarkReturnWorkBudget.activate(definition, run)) {
            scope.bindRoot(11, 1000);
            var report = Map.of("returnWorkBudgetPrefix", scope.snapshot(), "returnWorkBudgetProfile", Map.of("runId", run, "inputSha256", input));
            var prepared = scope.prepareOutput("FULL_FORK_REPORT", new BenchmarkReturnWorkBudget.TreePlan(plan.nodeUpper(), plan.stringUtf8Upper(),
                    plan.encodedUpperBytes(), plan.depthUpper()), report, plan.sha256());
            try { scope.writeOutput(prepared, bytes -> reportBytes[0] = bytes.clone()); }
            catch (Exception failure) { throw new AssertionError(failure); }
            closed = scope.finish();
        }
        String finalLine = BenchmarkReturnAfterReceipt.FINAL_PREFIX + JsonWriter.write(closed) + "\n";
        var producerBody = producerBody(run, input, producerRegistration); var bound = bound(producerBody);
        var exit = Map.<String, Object>of("finishedAfterCloseAt", "actual", "exitEvidenceSource", "ORIGINAL_OWNED_PROCESS_OBJECT",
                "ownedAliveAfterClose", false, "ownedExitCode", 0, "reasons", List.of(), "productAtCapture", Map.of("pid", 17),
                "applicationSha256", app.blob().sha256(), "childJvmRuntime", List.of(Map.of("actualPid", 17, "jvmStartTimeMillis", 2000)));
        var evidence = new BenchmarkReturnAfterReceipt.Inputs(closed, fact("/controlled/report.json", reportBytes[0]),
                fact("/controlled/final.txt", finalLine), fact("/controlled/current.xml", xml("normal output\n" + finalLine + "normal tail\n", 1, "selected", false)),
                rootIdentity, binding, fact("/controlled/producer.json", JsonWriter.write(producerBody)), bound, exit);
        return new Fixture(registration, evidence);
    }

    private static Map<String, Object> producerBody(String run, String input, BenchmarkReturnWorkBudgetProducerSeal.Registration registration) {
        var body = new LinkedHashMap<String, Object>();
        body.put("schemaVersion", 1); body.put("enabled", true); body.put("definitionId", "WRITE_RETURN_PRODUCER_WORK_V1");
        body.put("sourceFormulaSha256", BenchmarkReturnWorkBudgetProducerAllocations.FORMULA_SHA256);
        body.put("inputSha256", input); body.put("runId", run); body.put("pid", 17L); body.put("jvmStartTimeMillis", 2000L);
        body.put("stage", "SHUTDOWN_HOOK"); body.put("lifecycle", "SHUTDOWN_HOOK"); body.put("generation", 1L);
        body.put("nonceSha256", hash(registration.nonce().getBytes(StandardCharsets.US_ASCII))); body.put("state", "RECORDED_STRUCTURAL_DIAGNOSTIC");
        body.put("limits", vector(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE));
        body.put("materialLimits", Map.of("connectorHashBytes", 134217728L, "runtimeArguments", 128L, "runtimeArgumentBytes", 2093056L, "primaryKeys", 2L));
        body.put("actualMaterial", vector(0, 0, 0, 0, 0, 0, 0, 0)); body.put("sourceUpper", vector(3, 598308, 0, 0, 442364, 865, 1015252, 949780));
        body.put("inputRowsAttempted", 0L); body.put("sourceUpperIsActualWork", false); body.put("limitsIndependentlyVerified", false);
        var rows = new LinkedHashMap<String, Object>();
        for (String name : BenchmarkReturnWorkBudgetProducerSeal.ACTIVITY_NAMES) rows.put(name, Map.of("invocations", 0L, "completed", 0L, "failed", 0L, "pending", 0L));
        rows.put("CONFIG", Map.of("invocations", 1L, "completed", 1L, "failed", 0L, "pending", 0L)); body.put("activities", rows);
        body.put("bookkeepingInvocations", 1L); body.put("epochs", List.of()); body.put("pending", 0L); body.put("openWriters", 0L);
        body.put("omitted", 0L); body.put("overflow", false); body.put("overBudget", false); body.put("gaps", List.of()); body.put("lateMutations", 0L);
        body.put("sourceSealTailUpper", vector(1, 201048, 0, 0, 308032, 843, 751576, 686104));
        body.put("sealTailIncludedInSourceUpper", true); body.put("actualSealTailInCompanion", true); body.put("processExitObserved", false);
        body.put("crashDurabilityQualified", false); body.put("opaqueScope", "REQUESTS_AND_BOUNDED_MATERIAL_ONLY");
        body.put("excluded", "OPAQUE_PROVIDER_INSTRUCTIONS_CPU_ELAPSED_AND_VM_EXIT_INTERNALS"); body.put("costUnit", "STRUCTURAL_WORK_VECTOR_NOT_CPU_OR_ELAPSED");
        for (String flag : List.of("structuralBudgetQualified", "accountingErrorBoundQualified", "wholeMethodCostQualified", "collectionCostUpperBoundQualified",
                "samplingCostQualified", "causalOverheadQualified", "costAcceptanceEligible", "performanceAcceptanceEligible", "formalPerformance")) body.put(flag, false);
        return body;
    }
    private static BenchmarkReturnWorkBudgetProducerSeal.Bound bound(Map<String, Object> body) {
        byte[] raw = JsonWriter.write(body).getBytes(StandardCharsets.US_ASCII);
        return new BenchmarkReturnWorkBudgetProducerSeal.Bound(body, hash(raw), "b".repeat(64), raw.length, 1, 2);
    }
    private static String xml(String output, int cases, String method, boolean failure) {
        String selected = "<testcase classname=\"io.tapstate.e2e.SelectedIT\" name=\"" + method + "\">"
                + (failure ? "<failure/>" : "") + "<system-out><![CDATA[" + output + "]]></system-out></testcase>";
        return "<testsuite tests=\"" + cases + "\" failures=\"" + (failure ? 1 : 0) + "\" errors=\"0\" skipped=\"0\">"
                + selected.repeat(cases) + "</testsuite>";
    }
    private static BenchmarkReturnAfterReceipt.ByteFact fact(String path, String bytes) { return fact(path, bytes.getBytes(StandardCharsets.UTF_8)); }
    private static BenchmarkReturnAfterReceipt.ByteFact fact(String path, byte[] bytes) { return new BenchmarkReturnAfterReceipt.ByteFact(path, new Blob(hash(bytes), bytes.length), bytes); }
    private static Blob blob(String bytes) { byte[] raw = bytes.getBytes(StandardCharsets.US_ASCII); return new Blob(hash(raw), raw.length); }
    private static String hash(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (Exception failure) { throw new AssertionError(failure); }
    }
    private static String utf8(byte[] bytes) { return new String(bytes, StandardCharsets.UTF_8); }
    @SuppressWarnings("unchecked") private static Map<String, Object> mutable(Map<String, Object> input) { return (Map<String, Object>) JsonReader.parse(JsonWriter.write(input)); }
    @SuppressWarnings("unchecked") private static Map<String, Object> object(Object value) { return (Map<String, Object>) value; }
    private static Map<String, Object> vector(long... values) {
        var map = new LinkedHashMap<String, Object>(); String[] units = {"calls", "requests", "records", "frames", "bytes", "passes", "elements", "updates"};
        for (int i = 0; i < units.length; i++) map.put(units[i], values[i]); return map;
    }
    private static BenchmarkReturnWorkBudget.Work vector(Object raw) {
        Map<String, Object> d = object(raw);
        return new BenchmarkReturnWorkBudget.Work(n(d, "calls"), n(d, "requests"), n(d, "records"), n(d, "frames"), n(d, "bytes"), n(d, "passes"), n(d, "elements"), n(d, "updates"));
    }
    private static long n(Map<String, Object> d, String key) { return ((Number) d.get(key)).longValue(); }
}
