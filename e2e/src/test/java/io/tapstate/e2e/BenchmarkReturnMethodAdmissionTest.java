package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static io.tapstate.e2e.BenchmarkReturnWorkBudgetBindings.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

class BenchmarkReturnMethodAdmissionTest {
    private static final String HASH = "0".repeat(64);
    private static final String LIBRARY = BenchmarkNativeClockEvidenceTest.LIBRARY;
    private static final BenchmarkCausalClock.Identity ROOT = new BenchmarkCausalClock.Identity(29, 2000);
    private static final BenchmarkWorkloadDefinitions.Workload WORKLOAD = BenchmarkWorkloadDefinitions.steadyPilot("stateless");

    @Test void source_hashes_and_matching_metadata_do_not_install_an_independent_semantic_authority() {
        var input = preflight();
        var refusal = catchThrowable(() -> BenchmarkReturnMethodAdmission.admit(input));
        assertThat(refusal).isInstanceOf(BenchmarkReturnMethodAdmission.Refusal.class)
                .hasMessageContaining("INDEPENDENT_NATIVE_SEMANTIC_REVIEW_NOT_INSTALLED");
        var facts = ((BenchmarkReturnMethodAdmission.Refusal) refusal).retainedEvidence();
        for (String flag : BenchmarkReturnProducerOrigins.FLAGS) assertThat(facts).containsEntry(flag, false);
        assertThat(BenchmarkReturnWorkBudget.current()).isNull();
    }

    @Test void the_control_authority_receives_the_direct_path_and_provider_contract_without_granting_formal_selection() {
        var calls = new AtomicInteger();
        var admitted = BenchmarkReturnMethodAdmission.admitForControl(preflight(), inspected -> {
            calls.incrementAndGet();
            assertThat(inspected.providerContract()).isEqualTo(BenchmarkReturnMethodAdmission.ProviderContract.MACH_ABSOLUTE_TIME_NOMINAL_MONOTONIC_EXCLUDING_SLEEP);
            assertThat(inspected.invocationPath()).containsExactly("PdkBenchmarkClock.nanoTime/source", "Clock.read", "JniAccess.read",
                    "nativeNanoTime", "mach_absolute_time", "unsigned128 multiply/divide");
            assertThat(inspected.jniSourceSha256()).isEqualTo("f25f42fdf196d1af702782cd6a87a86f7b696f10689cf27e768b377a19cff4a1");
            assertThat(inspected.jniBinarySha256()).isEqualTo("4ce1204977af372080e3a3a8ada2c03ce678a15b337e3201f13d30b22a4700c9");
            assertThatThrownBy(inspected.selectedRootRoute()::clear).isInstanceOf(UnsupportedOperationException.class);
        });
        assertThat(calls).hasValue(1); assertThat(admitted.supportedWorkloads()).containsExactly("stateless");
        assertThatThrownBy(admitted::requireProductionAuthority).hasMessageContaining("CONTROL_REVIEW_CANNOT_SELECT_FORMAL_METHOD");
        for (String flag : BenchmarkReturnProducerOrigins.FLAGS) assertThat(admitted.evidence()).containsEntry(flag, false);
    }

    @Test void missing_or_foreign_components_refuse_before_the_review_callback() {
        var good = preflight(); var reviewed = new AtomicInteger();
        var review = (BenchmarkReturnMethodAdmission.IndependentSemanticReview) input -> reviewed.incrementAndGet();
        for (var input : List.of(new BenchmarkReturnMethodAdmission.Preflight("QUALIFIED", good.costRecipe(), good.helper(),
                        good.authenticatedBytes(), good.root(), good.library(), good.actualRootMetadata()),
                new BenchmarkReturnMethodAdmission.Preflight(good.method(), good.costRecipe(), good.helper(), good.authenticatedBytes(),
                        new BenchmarkCausalClock.Identity(29, 2001), good.library(), good.actualRootMetadata()),
                new BenchmarkReturnMethodAdmission.Preflight(good.method(), good.costRecipe(), good.helper(), good.authenticatedBytes(),
                        good.root(), "/other/library.dylib", good.actualRootMetadata()))) {
            assertThatThrownBy(() -> BenchmarkReturnMethodAdmission.admitForControl(input, review)).isInstanceOf(AssertionError.class);
        }
        assertThatThrownBy(() -> BenchmarkReturnMethodAdmission.admitForControl(null, review)).isInstanceOf(AssertionError.class);
        assertThat(reviewed).hasValue(0);
    }

    @Test void claimed_qualification_and_fallback_cannot_replace_native_recipe_facts() {
        var good = preflight();
        for (String flag : BenchmarkNativeClockEvidence.FLAGS) {
            var metadata = metadata(ROOT); metadata.put(flag, true);
            var input = new BenchmarkReturnMethodAdmission.Preflight(good.method(), good.costRecipe(), good.helper(), good.authenticatedBytes(),
                    good.root(), good.library(), JsonWriter.write(metadata));
            assertThatThrownBy(() -> BenchmarkReturnMethodAdmission.admitForControl(input, ignored -> { }))
                    .hasMessageContaining("ACTUAL_ROOT_NATIVE_METADATA_REFUSED");
        }
        var fallback = metadata(ROOT); fallback.put("state", "UNKNOWN"); fallback.put("reason", "NATIVE_READ_UNAVAILABLE");
        fallback.put("failureType", "java.lang.ArithmeticException"); fallback.put("activeProvider", "SYSTEM_NANO_TIME"); fallback.put("mixedDomainPossible", true);
        assertThatThrownBy(() -> BenchmarkReturnMethodAdmission.admitForControl(new BenchmarkReturnMethodAdmission.Preflight(
                good.method(), good.costRecipe(), good.helper(), good.authenticatedBytes(), good.root(), good.library(), JsonWriter.write(fallback)), ignored -> { }))
                .hasMessageContaining("ACTUAL_ROOT_NATIVE_PROVIDER_UNKNOWN");
    }

    @Test void independent_review_rejection_preserves_its_exact_primary() {
        var primary = new AssertionError("controlled independent semantic refusal");
        assertThat(catchThrowable(() -> BenchmarkReturnMethodAdmission.admitForControl(preflight(), inspected -> { throw primary; }))).isSameAs(primary);
        assertThat(primary.getSuppressed()).isEmpty();
    }

    @Test void original_raw_cohort_and_clock_facts_remain_distinct_from_steady_window_or_method_cost_credit() throws Exception {
        var admitted = control(); var fixture = fixture();
        var owner = fixture.result().samples().getFirst().identity();
        var bound = BenchmarkReturnMethodAdmission.verifyPhase(admitted, phase(fixture, cold(owner), cold(owner)));
        assertThat(bound.evidence()).containsEntry("componentState", "ORIGINAL_FORK_CLOCK_COHORT_FACTS_MATCHED_NO_FORMAL_METRIC_CREDIT")
                .containsEntry("fullRows", 96000).containsEntry("fixedCohortRows", 48000L)
                .containsEntry("physicalPerRowCommitTime", false);
        for (String flag : BenchmarkReturnProducerOrigins.FLAGS) assertThat(bound.evidence()).containsEntry(flag, false);
        assertThatThrownBy(bound.evidence()::clear).isInstanceOf(UnsupportedOperationException.class);
        assertThat(bound.deliveries()).hasSize(48000);
        assertThatThrownBy(bound.deliveries()::clear).isInstanceOf(UnsupportedOperationException.class);
        if (!Boolean.TRUE.equals(bound.commonWindow().get("commonWindowCertificatePassed"))) {
            assertThatThrownBy(bound::requireSteadyWindow).hasMessageContaining("RETURN_COMMON_WINDOW_NOT_QUALIFIED");
        }
        assertThatThrownBy(admitted::requireProductionAuthority).hasMessageContaining("CONTROL_REVIEW");
    }

    @Test void changed_fork_boot_or_clock_source_and_missing_raw_pages_refuse_without_repairing_evidence() throws Exception {
        var fixture = fixture(); var owner = fixture.result().samples().getFirst().identity();
        var after = cold(owner); var changed = metadata(owner);
        @SuppressWarnings("unchecked") var snapshot = new LinkedHashMap<>((Map<String, Object>) changed.get("before"));
        snapshot.put("bootUuid", "11111111-1111-1111-1111-111111111111"); changed.put("before", snapshot); changed.put("after", snapshot);
        after.putAll(changed); after.put("raw", JsonWriter.write(changed)); after.put("ownedMetadata", changed);
        assertThatThrownBy(() -> BenchmarkReturnMethodAdmission.verifyPhase(control(), phase(fixture, cold(owner), after)))
                .hasMessageContaining("ORIGINAL_FORK_NATIVE_MAPPING_REFUSED");
        var result = fixture.result(); var partial = new BenchmarkWriteReturnPhaseEvidenceTest.Fixture(fixture.workload(), fixture.phase(), fixture.batches(),
                new BenchmarkWriteReturnCapture.Result(result.calls(), result.samples(), result.summary(), List.of()));
        assertThatThrownBy(() -> BenchmarkReturnMethodAdmission.verifyPhase(control(), phase(partial, cold(owner), cold(owner))))
                .hasMessageContaining("ORIGINAL_RAW_SOURCE_OR_COHORT_REFUSED");
    }

    @Test void serial_hello_two_millisecond_checks_cannot_be_bypassed_by_a_good_nominal_mapping() throws Exception {
        var fixture = fixture(); var owner = fixture.result().samples().getFirst().identity();
        var good = phase(fixture, cold(owner), cold(owner));
        var bad = new BenchmarkReturnMethodAdmission.PhaseFacts(good.workload(), good.phase(), good.sourceBatches(), good.capture(),
                good.root(), good.owned(), good.nativeBefore(), good.nativeAfter(), List.of(hello(0, 1000), hello(200_000_000, 1300), hello(400_000_000, 1400)));
        assertThatThrownBy(() -> BenchmarkReturnMethodAdmission.verifyPhase(control(), bad)).hasMessageContaining("clock stepped");
    }

    @Test void source_serial_order_and_prior_roster_cap_are_checked_without_a_new_read_or_unbounded_copy() throws Exception {
        var fixture = fixture(); var owner = fixture.result().samples().getFirst().identity();
        var good = phase(fixture, cold(owner), cold(owner));
        var batches = new ArrayList<>(good.sourceBatches()); var first = batches.getFirst();
        batches.set(1, new BenchmarkForkEnvironment.BatchResult(1, first.issuedAtNanos(), first.issuedAtNanos() + 1));
        var changed = new BenchmarkReturnMethodAdmission.PhaseFacts(good.workload(), good.phase(), batches, good.capture(),
                good.root(), good.owned(), good.nativeBefore(), good.nativeAfter(), good.serialHello());
        assertThatThrownBy(() -> BenchmarkReturnMethodAdmission.verifyPhase(control(), changed))
                .hasMessageContaining("ORIGINAL_SOURCE_SERIAL_ORDER_DIFFERS");
        List<BenchmarkForkEnvironment.BatchResult> excessive = new java.util.AbstractList<>() {
            @Override public int size() { return 961; }
            @Override public BenchmarkForkEnvironment.BatchResult get(int index) { throw new AssertionError("unbounded copy attempted"); }
        };
        assertThatThrownBy(() -> new BenchmarkReturnMethodAdmission.PhaseFacts(good.workload(), good.phase(), excessive,
                good.capture(), good.root(), good.owned(), good.nativeBefore(), good.nativeAfter(), good.serialHello()))
                .hasMessageContaining("PHASE_ROSTER_EXCEEDS_PRIOR_CAP");
    }

    @Test void a_foreign_helper_and_io_writer_cannot_replace_the_cached_recipe_tuple() {
        var good = preflight(); var old = good.helper(); var bytes = good.authenticatedBytes();
        var foreign = new ProducerWriterBinding(Owner.PRODUCER, "/controlled/foreign-writer",
                new Source(SourceRole.PDK_WRITER, SourceRole.PDK_WRITER.sourcePath(), new Blob("f".repeat(64), 32)),
                new CompiledClass(SourceRole.PDK_WRITER, SourceRole.PDK_WRITER.binaryName(), new Blob("e".repeat(64), 32)));
        var helper = new HelperManifest(PRODUCER_WRITER_SCHEMA_VERSION, old.runId(), old.inputSha256(), old.buildId(),
                old.rootDefinitionId(), old.rootFormulaSha256(), old.producerFormulaSha256(), old.failsafeXml(),
                old.failsafeClasspath(), old.sources(), old.classes(), old.origins(), foreign);
        var io = new BenchmarkReturnWorkBudgetBindingsIo.Result(bytes.files(), bytes.sources(), bytes.classes(),
                bytes.origins(), bytes.jdkFiles(), bytes.evidence(), foreign);
        var altered = new BenchmarkReturnMethodAdmission.Preflight(good.method(), good.costRecipe(), helper, io,
                good.root(), good.library(), good.actualRootMetadata());
        var reviewed = new AtomicInteger();
        assertThatThrownBy(() -> BenchmarkReturnMethodAdmission.admitForControl(altered, inspected -> reviewed.incrementAndGet()))
                .isInstanceOf(BenchmarkReturnMethodAdmission.Refusal.class)
                .hasMessageContaining("PREFLIGHT_PRODUCER_WRITER_DIFFERS_FROM_CACHED_RECIPE");
        assertThat(reviewed).hasValue(0);
    }

    @Test void an_actual_empty_capture_refuses_before_hello_coverage_maximum_or_any_repair() throws Exception {
        var original = fixture(); var result = original.result(); var summary = result.summary();
        var empty = new BenchmarkWriteReturnCapture.Result(List.of(), result.samples(),
                new BenchmarkWriteReturnReader.Summary(summary.window(), summary.state(), 0, 0, 0, 0, 0), List.of());
        var owner = result.samples().getFirst().identity(); var admitted = control();
        assertThatThrownBy(() -> BenchmarkReturnMethodAdmission.verifyPhase(admitted,
                new BenchmarkReturnMethodAdmission.PhaseFacts(original.workload(), original.phase(), original.batches(), empty,
                        ROOT, owner, cold(owner), cold(owner), helloSeries())))
                .isInstanceOf(BenchmarkReturnMethodAdmission.Refusal.class).hasMessageContaining("CAPTURE_CALL_ROSTER_EMPTY");
        assertThat(empty.calls()).isEmpty(); assertThat(empty.pagesBase64()).isEmpty();
        assertThat(empty.summary().completedCalls()).isZero();
    }

    private static BenchmarkReturnMethodAdmission.AdmittedMethod control() {
        return BenchmarkReturnMethodAdmission.admitForControl(preflight(), inspected -> { });
    }
    private static BenchmarkReturnMethodAdmission.PhaseFacts phase(BenchmarkWriteReturnPhaseEvidenceTest.Fixture fixture,
            Map<String, Object> before, Map<String, Object> after) {
        return new BenchmarkReturnMethodAdmission.PhaseFacts(fixture.workload(), fixture.phase(), fixture.batches(), fixture.result(), ROOT,
                fixture.result().samples().getFirst().identity(), before, after,
                helloSeries());
    }
    private static List<BenchmarkTargetClock.Reading> helloSeries() {
        var readings = new ArrayList<BenchmarkTargetClock.Reading>();
        for (int millis = 0; millis <= 10000; millis += 200) readings.add(hello(millis * 1_000_000L, 1000 + millis));
        return List.copyOf(readings);
    }
    private static BenchmarkWriteReturnPhaseEvidenceTest.Fixture fixture() throws Exception {
        // Scale the existing numeric fixture so declared original cadence remains satisfied.
        // This is controlled material, not evidence of an actual native execution or loaded source.
        long scale = 100000;
        var original = BenchmarkWriteReturnPhaseEvidenceTest.nativeFixture(WORKLOAD); var result = original.result();
        var calls = result.calls().stream().map(call -> new BenchmarkWriteReturnAssembly.FullCall(call.sequence(), call.writer(), call.totalRows(),
                Math.multiplyExact(call.beganNanos(), scale), Math.multiplyExact(call.lastCallbackExitNanos(), scale), Math.multiplyExact(call.observedNanos(), scale),
                call.returnedNormally(), call.callbackCount(), call.inserted(), call.modified(), call.removed(), call.errors(), call.errorDetails(),
                call.failureType(), call.scope(), call.writerIdentity(), call.stream(), call.target(), call.keyFields(), call.rows())).toList();
        var samples = result.samples().stream().map(sample -> new BenchmarkCausalClock.Sample(sample.sequence(), sample.identity(),
                Math.multiplyExact(sample.driverBeforeNanos(), scale), Math.multiplyExact(sample.driverAfterNanos(), scale),
                Math.multiplyExact(sample.ownedNanos(), scale))).toList();
        var batches = original.batches().stream().map(batch -> new BenchmarkForkEnvironment.BatchResult(batch.index(),
                Math.multiplyExact(batch.issuedAtNanos(), scale), Math.multiplyExact(batch.completedAtNanos(), scale))).toList();
        var pages = new ArrayList<String>();
        for (String encoded : result.pagesBase64()) {
            byte[] raw = java.util.Base64.getDecoder().decode(encoded); assertThat(raw.length).isLessThanOrEqualTo(65536);
            var page = java.nio.ByteBuffer.wrap(raw); page.position(28);
            int text = Short.toUnsignedInt(page.getShort()); page.position(page.position() + text);
            text = Short.toUnsignedInt(page.getShort()); page.position(page.position() + text);
            while (page.hasRemaining()) {
                int length = page.getInt(), at = page.position();
                for (int offset : new int[]{24, 32, 40}) page.putLong(at + offset, Math.multiplyExact(page.getLong(at + offset), scale));
                page.position(at + length);
            }
            pages.add(java.util.Base64.getEncoder().encodeToString(raw));
        }
        return new BenchmarkWriteReturnPhaseEvidenceTest.Fixture(original.workload(), original.phase(), batches,
                new BenchmarkWriteReturnCapture.Result(calls, samples, result.summary(), pages));
    }
    private static BenchmarkTargetClock.Reading hello(long started, long server) {
        return new BenchmarkTargetClock.Reading("controlled-primary", "controlled-process", server, started, started + 1, server, server);
    }
    private static BenchmarkReturnMethodAdmission.Preflight preflight() {
        var files = new ArrayList<BenchmarkReturnWorkBudgetProfile.FileInput>();
        for (var role : BenchmarkReturnWorkBudgetProfile.FileRole.values()) if (role != BenchmarkReturnWorkBudgetProfile.FileRole.ROOT_CPU_PROVIDER_LIBRARY)
            files.add(new BenchmarkReturnWorkBudgetProfile.FileInput(role, role == BenchmarkReturnWorkBudgetProfile.FileRole.NATIVE_LIBRARY
                    ? "4ce1204977af372080e3a3a8ada2c03ce678a15b337e3201f13d30b22a4700c9" : HASH,
                    role == BenchmarkReturnWorkBudgetProfile.FileRole.NATIVE_LIBRARY ? 512 : 1048576));
        var prepared = BenchmarkReturnWorkBudgetProfile.inspect(new BenchmarkReturnWorkBudgetProfile.Inputs("control", HASH,
                BenchmarkReturnWorkBudgetProfile.WorkloadInput.from(WORKLOAD), files,
                new BenchmarkReturnWorkBudgetProfile.Flags("B", "PLAIN", "PERIODIC", "ON", true, true, true, true, false, Set.of())));
        var profile = BenchmarkReturnWorkBudgetBindings.authenticate(prepared); var parsed = metadata(ROOT);
        @SuppressWarnings("unchecked") var hashes = (Map<String, String>) ((Map<?, ?>) parsed.get("before")).get("classHashes");
        var classes = hashes.entrySet().stream().map(entry -> new CompiledClass(SourceRole.PDK_CLOCK,
                "io.tapstate.adapters.pdk." + entry.getKey(), new Blob(entry.getValue(), 32))).toList();
        var sources = List.of(new Source(SourceRole.JNI_SOURCE, SourceRole.JNI_SOURCE.sourcePath(),
                new Blob("f25f42fdf196d1af702782cd6a87a86f7b696f10689cf27e768b377a19cff4a1", 32)));
        var helper = new HelperManifest(1, "control", HASH, "controlled", profile.definition().definitionId(), profile.definition().sourceFormulaSha256(),
                BenchmarkReturnWorkBudgetProducerAllocations.FORMULA_SHA256, new Blob(HASH, 32), List.of("/controlled/classes"), sources, classes, List.of());
        var bytes = new BenchmarkReturnWorkBudgetBindingsIo.Result(List.of(new FileFact(BenchmarkReturnWorkBudgetProfile.FileRole.NATIVE_LIBRARY,
                LIBRARY, new Blob("4ce1204977af372080e3a3a8ada2c03ce678a15b337e3201f13d30b22a4700c9", 512))), sources, classes, List.of(), List.of(), Map.of());
        return new BenchmarkReturnMethodAdmission.Preflight(BenchmarkReturnMethodAdmission.METHOD,
                BenchmarkReturnMethodAdmission.structuralRecipe(profile), helper, bytes, ROOT, LIBRARY, JsonWriter.write(parsed));
    }
    private static Map<String, Object> cold(BenchmarkCausalClock.Identity owner) {
        var owned = metadata(owner); var root = metadata(ROOT); var result = new LinkedHashMap<String, Object>(owned);
        result.put("rawRetained", true); result.put("raw", JsonWriter.write(owned));
        result.put("expectedPid", owner.pid()); result.put("expectedJvmStartTimeMillis", owner.jvmStartTimeMillis());
        result.put("expectedRootPid", ROOT.pid()); result.put("expectedRootJvmStartTimeMillis", ROOT.jvmStartTimeMillis());
        result.put("matchedRuntimeFlag", "-Dtapstate.benchmark.write-return=true"); result.put("matchedNativeClockFlag", "-Dtapstate.benchmark.native-clock-library=" + LIBRARY);
        result.put("ownedMetadata", owned); result.put("parsedRootMetadata", root);
        result.put("rootMetadata", Map.of("rawRetained", true, "raw", JsonWriter.write(root))); return result;
    }
    private static Map<String, Object> metadata(BenchmarkCausalClock.Identity owner) {
        var metadata = BenchmarkNativeClockEvidenceTest.metadataMap(owner);
        @SuppressWarnings("unchecked") var snapshot = new LinkedHashMap<>((Map<String, Object>) metadata.get("before"));
        snapshot.put("loadedJniSha256", "4ce1204977af372080e3a3a8ada2c03ce678a15b337e3201f13d30b22a4700c9");
        snapshot.put("classSha256", "42fffc3e10a70ae503e85c909cac3d5f437c07c2310115a59db875124c53b1f2");
        snapshot.put("classHashes", Map.of("PdkBenchmarkClock", "42fffc3e10a70ae503e85c909cac3d5f437c07c2310115a59db875124c53b1f2",
                "PdkBenchmarkClock$Clock", "82c52a87d590e4ac400af03e51827a892d7988e27496883169f817fc83a89751",
                "PdkBenchmarkClock$JniAccess", "2ccedc46ae98b4ea4e6c79c112a53fc3953efc3c344d2f3e6ba1e4d78eabaeb2",
                "PdkBenchmarkClock$NativeAccess", "4b67c17ed28c1d341d9e27819563d1d89eb017073b8492f808e35da31f5ddfb7"));
        snapshot.put("osImageUuid", "f63bf4188f7534a4aaf18caf2b9b6a05"); snapshot.put("osFunctionImageOffsetUnsigned", "4236");
        snapshot.put("osFunctionCodePrefixSha256", "c615a84440f8ae7325e75d236ace6e038838f5cf634f9bba912d2904c99ff2fd");
        snapshot.put("userTimebaseSelector", 3L); metadata.put("before", snapshot); metadata.put("after", snapshot); return metadata;
    }
}
