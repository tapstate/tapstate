package io.tapstate.e2e;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import io.tapstate.e2e.BenchmarkReturnWorkBudgetBindings.Runtime;
import org.junit.jupiter.api.Test;

import static io.tapstate.e2e.BenchmarkReturnWorkBudgetBindings.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnWorkBudgetBindingsTest {
    @Test void cached_source_authentication_rejects_substituted_bounds_and_still_checks_later_actual_facts() {
        var fixture = fixture(false, false);
        var authenticated = BenchmarkReturnWorkBudgetBindings.authenticate(fixture.prepared());
        assertThat(BenchmarkReturnWorkBudgetBindings.verify(authenticated, fixture.helper(), fixture.jdk(),
                fixture.actual(), fixture.owners())).containsEntry("state", "REGISTERED_BINDING_FACTS_MATCHED");
        var bounds = new java.util.EnumMap<>(fixture.prepared().perOperationUpper());
        bounds.put(BenchmarkReturnWorkBudget.Activity.DECODE, BenchmarkReturnWorkBudget.Work.zero());
        var replaced = new BenchmarkReturnWorkBudgetProfile.Prepared(fixture.prepared().inputs(), bounds,
                fixture.prepared().explicitExcludedActivities(), fixture.prepared().fieldsRegisteredOutputPlan(),
                fixture.prepared().inputRegistrationInfo(), fixture.prepared().gaps());
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetBindings.authenticate(replaced))
                .isInstanceOf(AssertionError.class).hasMessageContaining("source factory registration differs");
        var changed = new ArrayList<>(fixture.actual().classes()); var first = changed.getFirst();
        changed.set(0, new CompiledClass(first.role(), first.binaryName(), blob("changed-after-authentication")));
        var changedActual = actual(fixture, fixture.actual().files(), fixture.actual().sources(), changed,
                fixture.actual().origins(), fixture.actual().jdkFiles());
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetBindings.verify(authenticated, fixture.helper(), fixture.jdk(),
                changedActual, fixture.owners())).isInstanceOf(AssertionError.class).hasMessageContaining("actual compiled class bytes differ");
    }

    @Test void matching_typed_facts_keep_source_compiled_and_runtime_boundaries_explicit() {
        var fixture = fixture(false, false);
        var evidence = fixture.verify();
        assertThat(evidence).containsEntry("state", "REGISTERED_BINDING_FACTS_MATCHED")
                .containsEntry("sourceUnits", fixture.helper().sources().size())
                .containsEntry("compiledMembers", fixture.helper().classes().size())
                .containsEntry("ownedOrigins", fixture.helper().origins().size())
                .containsEntry("structuralWorkBudgetQualified", false)
                .containsEntry("wholeMethodCostQualified", false)
                .containsEntry("runtimeInstructionBytesQualified", false);
        for (String flag : BenchmarkNativeClockEvidence.FLAGS) { assertThat(evidence).containsEntry(flag, false); }
        assertThat(evidence.get("externalRequirement")).isEqualTo(
                "IO_FACT_AUTHENTICATION_EMITTED_ROSTER_COMPLETENESS_AND_VM_TRANSFORMATION_NOT_PROVEN_HERE");
        assertThatThrownBy(evidence::clear).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void unchanged_source_cannot_hide_changed_compiled_bytes_and_compiled_bytes_cannot_hide_changed_source() {
        var fixture = fixture(false, false);
        var compiled = new ArrayList<>(fixture.actual().classes());
        var first = compiled.getFirst();
        compiled.set(0, new CompiledClass(first.role(), first.binaryName(), blob("different-compiler-output")));
        assertThatThrownBy(() -> fixture.withActual(actual(fixture, fixture.actual().files(), fixture.actual().sources(),
                compiled, fixture.actual().origins(), fixture.actual().jdkFiles())).verify())
                .isInstanceOf(AssertionError.class).hasMessageContaining("actual compiled class bytes differ");
        var sources = new ArrayList<>(fixture.actual().sources());
        var source = sources.getFirst();
        sources.set(0, new Source(source.role(), source.path(), blob("different-source")));
        assertThatThrownBy(() -> fixture.withActual(actual(fixture, fixture.actual().files(), sources,
                fixture.actual().classes(), fixture.actual().origins(), fixture.actual().jdkFiles())).verify())
                .isInstanceOf(AssertionError.class).hasMessageContaining("actual source bytes differ");
    }

    @Test void source_roster_and_native_member_roster_cannot_be_consistently_omitted_from_both_sides() {
        var fixture = fixture(false, false);
        var missingSource = fixture.helper().sources().stream().filter(s -> s.role() != SourceRole.JNI_SOURCE).toList();
        var changedHelper = helper(fixture, missingSource, fixture.helper().classes(), fixture.helper().origins());
        var changedActual = actual(fixture, fixture.actual().files(), missingSource, fixture.actual().classes(),
                fixture.actual().origins(), fixture.actual().jdkFiles());
        assertThatThrownBy(() -> fixture.withHelper(changedHelper).withActual(changedActual).verify())
                .isInstanceOf(AssertionError.class).hasMessageContaining("required source roster missing");
        String member = SourceRole.PDK_CLOCK.binaryName() + "$NativeAccess";
        var missingMember = fixture.helper().classes().stream().filter(c -> !c.binaryName().equals(member)).toList();
        var missingOrigins = fixture.helper().origins().stream().filter(o -> !o.binaryName().equals(member)).toList();
        var missingObserved = fixture.actual().origins().stream().filter(o -> !o.origin().binaryName().equals(member)).toList();
        assertThatThrownBy(() -> fixture.withHelper(helper(fixture, fixture.helper().sources(), missingMember, missingOrigins))
                .withActual(actual(fixture, fixture.actual().files(), fixture.actual().sources(), missingMember,
                        missingObserved, fixture.actual().jdkFiles())).verify())
                .isInstanceOf(AssertionError.class).hasMessageContaining("required native clock member missing");
    }

    @Test void repeated_source_and_foreign_compiled_owner_refuse_even_with_matching_digest_shapes() {
        var fixture = fixture(false, false);
        var sources = new ArrayList<>(fixture.helper().sources()); sources.set(1, sources.getFirst());
        assertThatThrownBy(() -> fixture.withHelper(helper(fixture, sources, fixture.helper().classes(), fixture.helper().origins())).verify())
                .isInstanceOf(AssertionError.class).hasMessageContaining("source role repeated");
        var classes = new ArrayList<>(fixture.helper().classes());
        classes.set(0, new CompiledClass(SourceRole.ROOT_BUDGET, "io.tapstate.foreign.Clock", blob("foreign")));
        assertThatThrownBy(() -> fixture.withHelper(helper(fixture, fixture.helper().sources(), classes, fixture.helper().origins())).verify())
                .isInstanceOf(AssertionError.class).hasMessageContaining("foreign source owner");
    }

    @Test void schema_root_formula_and_producer_formula_are_independent_semantic_requirements() {
        var fixture = fixture(false, false); var h = fixture.helper();
        var wrongSchema = new HelperManifest(2, h.runId(), h.inputSha256(), h.buildId(), h.rootDefinitionId(),
                h.rootFormulaSha256(), h.producerFormulaSha256(), h.failsafeXml(), h.failsafeClasspath(), h.sources(), h.classes(), h.origins());
        assertThatThrownBy(() -> fixture.withHelper(wrongSchema).verify()).isInstanceOf(AssertionError.class).hasMessageContaining("manifest schema differs");
        for (boolean producer : List.of(false, true)) {
            var wrong = new HelperManifest(h.schemaVersion(), h.runId(), h.inputSha256(), h.buildId(), h.rootDefinitionId(),
                    producer ? h.rootFormulaSha256() : blob("another-root-recipe").sha256(),
                    producer ? blob("another-producer-formula").sha256() : h.producerFormulaSha256(),
                    h.failsafeXml(), h.failsafeClasspath(), h.sources(), h.classes(), h.origins());
            assertThatThrownBy(() -> fixture.withHelper(wrong).verify()).isInstanceOf(AssertionError.class).hasMessageContaining("source formula binding differs");
        }
    }

    @Test void surefire_classpath_and_old_build_receipts_cannot_replace_actual_failsafe_facts() {
        var fixture = fixture(false, false); var a = fixture.actual();
        var surefire = new ActualFacts(a.runId(), a.inputSha256(), a.buildId(), Lifecycle.SUREFIRE_TEST,
                a.failsafeXml(), a.failsafeClasspath(), a.files(), a.sources(), a.classes(), a.origins(), a.jdkFiles(), Set.of());
        assertThatThrownBy(() -> fixture.withActual(surefire).verify()).isInstanceOf(AssertionError.class).hasMessageContaining("actual Failsafe provenance differs");
        var extra = new ArrayList<>(a.failsafeClasspath()); extra.add("/controlled/extra.jar");
        var differentClasspath = new ActualFacts(a.runId(), a.inputSha256(), a.buildId(), a.lifecycle(),
                a.failsafeXml(), extra, a.files(), a.sources(), a.classes(), a.origins(), a.jdkFiles(), Set.of());
        assertThatThrownBy(() -> fixture.withActual(differentClasspath).verify()).isInstanceOf(AssertionError.class).hasMessageContaining("actual Failsafe classpath differs");
        var old = new ActualFacts(a.runId(), a.inputSha256(), "prior-build", a.lifecycle(), a.failsafeXml(),
                a.failsafeClasspath(), a.files(), a.sources(), a.classes(), a.origins(), a.jdkFiles(), Set.of());
        assertThatThrownBy(() -> fixture.withActual(old).verify()).isInstanceOf(AssertionError.class).hasMessageContaining("fresh build");
    }

    @Test void application_entry_owner_and_exact_owned_pid_start_are_required_in_addition_to_class_digest() {
        var fixture = fixture(false, false);
        var observations = new ArrayList<>(fixture.actual().origins());
        int index = java.util.stream.IntStream.range(0, observations.size())
                .filter(i -> observations.get(i).origin().owner() == Owner.PRODUCER).findFirst().orElseThrow();
        var observed = observations.get(index); var original = observed.origin();
        var foreignEntry = new Origin(original.owner(), original.role(), original.binaryName(), original.container(),
                original.entry().replace("BOOT-INF/lib/adapter-pdk-", "BOOT-INF/lib/foreign-"), original.classBytes(), original.applicationSha256());
        observations.set(index, new ObservedOrigin(foreignEntry, observed.runtime()));
        assertThatThrownBy(() -> fixture.withActual(actual(fixture, fixture.actual().files(), fixture.actual().sources(),
                fixture.actual().classes(), observations, fixture.actual().jdkFiles())).verify())
                .isInstanceOf(AssertionError.class).hasMessageContaining("artifact owner differs");
        var runtime = observed.runtime();
        var foreignProcess = new Runtime(runtime.owner(), runtime.runId(), runtime.pid() + 1, runtime.jvmStartTimeMillis(), runtime.javaHome(), runtime.javaVersion());
        observations.set(index, new ObservedOrigin(original, foreignProcess));
        assertThatThrownBy(() -> fixture.withActual(actual(fixture, fixture.actual().files(), fixture.actual().sources(),
                fixture.actual().classes(), observations, fixture.actual().jdkFiles())).verify())
                .isInstanceOf(AssertionError.class).hasMessageContaining("runtime identity differs");
        observations.set(index, new ObservedOrigin(original, new Runtime(runtime.owner(), runtime.runId(), runtime.pid(),
                runtime.jvmStartTimeMillis() + 1, runtime.javaHome(), runtime.javaVersion())));
        assertThatThrownBy(() -> fixture.withActual(actual(fixture, fixture.actual().files(), fixture.actual().sources(),
                fixture.actual().classes(), observations, fixture.actual().jdkFiles())).verify())
                .isInstanceOf(AssertionError.class).hasMessageContaining("runtime identity differs");
    }

    @Test void missing_or_repeated_owned_origin_is_not_repaired_by_complete_compiler_facts() {
        var fixture = fixture(false, false);
        var missing = new ArrayList<>(fixture.actual().origins()); missing.removeFirst();
        assertThatThrownBy(() -> fixture.withActual(actual(fixture, fixture.actual().files(), fixture.actual().sources(),
                fixture.actual().classes(), missing, fixture.actual().jdkFiles())).verify())
                .isInstanceOf(AssertionError.class).hasMessageContaining("required owned class origin missing");
        var observations = new ArrayList<>(fixture.actual().origins()); observations.set(1, observations.getFirst());
        var repeated = actual(fixture, fixture.actual().files(), fixture.actual().sources(), fixture.actual().classes(), observations, fixture.actual().jdkFiles());
        assertThatThrownBy(() -> fixture.withActual(repeated).verify()).isInstanceOf(AssertionError.class).hasMessageContaining("class origin repeated");
    }

    @Test void actual_file_size_digest_and_manifest_prior_material_cap_are_checked_separately() {
        var fixture = fixture(false, false); var files = new ArrayList<>(fixture.actual().files()); var first = files.getFirst();
        files.set(0, new FileFact(first.role(), first.path(), new Blob(first.blob().sha256(), first.blob().bytes() + 1)));
        assertThatThrownBy(() -> fixture.withActual(actual(fixture, files, fixture.actual().sources(), fixture.actual().classes(),
                fixture.actual().origins(), fixture.actual().jdkFiles())).verify())
                .isInstanceOf(AssertionError.class).hasMessageContaining("digest or prior size differs");
        files.set(0, new FileFact(first.role(), first.path(), new Blob(blob("other-file").sha256(), first.blob().bytes())));
        assertThatThrownBy(() -> fixture.withActual(actual(fixture, files, fixture.actual().sources(), fixture.actual().classes(),
                fixture.actual().origins(), fixture.actual().jdkFiles())).verify())
                .isInstanceOf(AssertionError.class).hasMessageContaining("digest or prior size differs");
        assertThatThrownBy(() -> fixture(false, true)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("manifest scalar exceeds its source cap");
    }

    @Test void jdk_artifact_roster_origin_and_conditional_cpu_provider_do_not_follow_file_labels_alone() {
        var fixture = fixture(false, false);
        var missing = fixture.actual().jdkFiles().stream().filter(f -> f.role() != JdkRole.MODULES).toList();
        assertThatThrownBy(() -> fixture.withActual(actual(fixture, fixture.actual().files(), fixture.actual().sources(),
                fixture.actual().classes(), fixture.actual().origins(), missing)).verify())
                .isInstanceOf(AssertionError.class).hasMessageContaining("required JDK artifact missing");
        var wrong = new ArrayList<>(fixture.actual().jdkFiles()); var first = wrong.getFirst();
        wrong.set(0, new JdkFile(first.role(), "/foreign/jdk/bin/java", first.blob()));
        assertThatThrownBy(() -> fixture.withActual(actual(fixture, fixture.actual().files(), fixture.actual().sources(),
                fixture.actual().classes(), fixture.actual().origins(), wrong)).verify())
                .isInstanceOf(AssertionError.class).hasMessageContaining("JDK path or role differs");
        var cpu = fixture(true, false);
        assertThat(cpu.verify()).containsEntry("jdkArtifacts", 5);
        var cpuFiles = new ArrayList<>(cpu.actual().jdkFiles());
        int cpuIndex = java.util.stream.IntStream.range(0, cpuFiles.size()).filter(i -> cpuFiles.get(i).role() == JdkRole.CPU_PROVIDER).findFirst().orElseThrow();
        var provider = cpuFiles.get(cpuIndex);
        cpuFiles.set(cpuIndex, new JdkFile(provider.role(), provider.path(), blob("different-provider")));
        assertThatThrownBy(() -> cpu.withActual(actual(cpu, cpu.actual().files(), cpu.actual().sources(), cpu.actual().classes(),
                cpu.actual().origins(), cpuFiles)).verify()).isInstanceOf(AssertionError.class).hasMessageContaining("actual JDK artifact differs");
    }

    @Test void claimed_qualification_cannot_authenticate_opaque_file_hashes() {
        var fixture = fixture(false, false); var a = fixture.actual();
        var claimed = new ActualFacts(a.runId(), a.inputSha256(), a.buildId(), a.lifecycle(), a.failsafeXml(),
                a.failsafeClasspath(), a.files(), a.sources(), a.classes(), a.origins(), a.jdkFiles(), Set.of("wholeMethodCostQualified"));
        assertThatThrownBy(() -> fixture.withActual(claimed).verify()).isInstanceOf(AssertionError.class).hasMessageContaining("caller supplied a qualification");
    }

    /** Synthetic typed facts exercise equality/refusal only; no file, process or VM proof is supplied. */
    private static Fixture fixture(boolean cpu, boolean oversizedManifest) {
        String run = "controlled-bindings", input = blob("controlled-input").sha256(), home = "/controlled/jdk";
        var files = new ArrayList<FileFact>(); var inputs = new ArrayList<BenchmarkReturnWorkBudgetProfile.FileInput>();
        for (var role : BenchmarkReturnWorkBudgetProfile.FileRole.values()) {
            if (role == BenchmarkReturnWorkBudgetProfile.FileRole.ROOT_CPU_PROVIDER_LIBRARY && !cpu) { continue; }
            Blob digest = blob(role == BenchmarkReturnWorkBudgetProfile.FileRole.ROOT_CPU_PROVIDER_LIBRARY ? "jdk-CPU_PROVIDER" : "file-" + role);
            long bytes = role == BenchmarkReturnWorkBudgetProfile.FileRole.COMMON_HELPER_MANIFEST && oversizedManifest ? MAX_MANIFEST_BYTES + 1 : digest.bytes();
            digest = new Blob(digest.sha256(), bytes);
            String path = role == BenchmarkReturnWorkBudgetProfile.FileRole.ROOT_CPU_PROVIDER_LIBRARY
                    ? home + "/" + JdkRole.CPU_PROVIDER.relativePath() : "/controlled/files/" + role + ".bin";
            files.add(new FileFact(role, path, digest)); inputs.add(new BenchmarkReturnWorkBudgetProfile.FileInput(role, digest.sha256(), digest.bytes()));
        }
        var profile = BenchmarkReturnWorkBudgetProfile.inspect(new BenchmarkReturnWorkBudgetProfile.Inputs(run, input,
                BenchmarkReturnWorkBudgetProfile.WorkloadInput.original("stateless"), inputs,
                new BenchmarkReturnWorkBudgetProfile.Flags("B", "PLAIN", "PERIODIC", "ON", true, true, true, true, cpu, Set.of())));
        var definition = profile.requireDefinition();
        var sources = new ArrayList<Source>(); var classes = new ArrayList<CompiledClass>();
        for (var role : SourceRole.values()) {
            sources.add(new Source(role, role.sourcePath(), blob("source-" + role)));
            if (role.binaryName() != null) { classes.add(new CompiledClass(role, role.binaryName(), blob("compiled-" + role))); }
        }
        for (String suffix : List.of("$Clock", "$JniAccess", "$NativeAccess")) {
            classes.add(new CompiledClass(SourceRole.PDK_CLOCK, SourceRole.PDK_CLOCK.binaryName() + suffix, blob("compiled-clock-" + suffix)));
        }
        var root = new Runtime(Owner.ROOT, run, 11, 1000, home, "21.0.9");
        var producer = new Runtime(Owner.PRODUCER, run, 17, 2000, home, "21.0.9");
        var owners = new OwnedRuntimes(root, producer);
        List<String> classpath = List.of("/controlled/e2e/test-classes", "/controlled/adapter-pdk.jar", "/controlled/core-common.jar");
        var origins = new ArrayList<Origin>(); var observed = new ArrayList<ObservedOrigin>();
        var application = files.stream().filter(f -> f.role() == BenchmarkReturnWorkBudgetProfile.FileRole.APP_JAR).findFirst().orElseThrow();
        for (var item : classes) {
            String entry = item.binaryName().replace('.', '/') + ".class";
            String container = item.role().module().equals("e2e") ? classpath.getFirst() : "/controlled/" + item.role().module() + ".jar";
            var origin = new Origin(Owner.ROOT, item.role(), item.binaryName(), container, entry, item.blob(), null);
            origins.add(origin); observed.add(new ObservedOrigin(origin, root));
            if (item.role().owners().contains(Owner.PRODUCER)) {
                origin = new Origin(Owner.PRODUCER, item.role(), item.binaryName(), application.path(),
                        "BOOT-INF/lib/" + item.role().module() + "-1.0.jar!/" + entry, item.blob(), application.blob().sha256());
                origins.add(origin); observed.add(new ObservedOrigin(origin, producer));
            }
        }
        var jdkFiles = new ArrayList<JdkFile>();
        for (var role : JdkRole.values()) {
            if (role == JdkRole.CPU_PROVIDER && !cpu) { continue; }
            jdkFiles.add(new JdkFile(role, home + "/" + role.relativePath(), blob("jdk-" + role)));
        }
        var xml = blob("fresh-failsafe-xml");
        var helper = new HelperManifest(1, run, input, "controlled-build", definition.definitionId(), definition.sourceFormulaSha256(),
                BenchmarkReturnWorkBudgetProducerAllocations.FORMULA_SHA256, xml, classpath, sources, classes, origins);
        var jdk = new JdkManifest(1, run, input, home, "21.0.9", jdkFiles);
        var actual = new ActualFacts(run, input, "controlled-build", Lifecycle.FAILSAFE_VERIFY, xml, classpath,
                files, sources, classes, observed, jdkFiles, Set.of());
        return new Fixture(profile, helper, jdk, actual, owners);
    }

    private static HelperManifest helper(Fixture f, List<Source> sources, List<CompiledClass> classes, List<Origin> origins) {
        var h = f.helper(); return new HelperManifest(h.schemaVersion(), h.runId(), h.inputSha256(), h.buildId(), h.rootDefinitionId(),
                h.rootFormulaSha256(), h.producerFormulaSha256(), h.failsafeXml(), h.failsafeClasspath(), sources, classes, origins);
    }
    private static ActualFacts actual(Fixture f, List<FileFact> files, List<Source> sources, List<CompiledClass> classes,
            List<ObservedOrigin> origins, List<JdkFile> jdk) {
        var a = f.actual(); return new ActualFacts(a.runId(), a.inputSha256(), a.buildId(), a.lifecycle(), a.failsafeXml(),
                a.failsafeClasspath(), files, sources, classes, origins, jdk, a.claimedQualifications());
    }
    private static Blob blob(String value) {
        byte[] bytes = value.getBytes(StandardCharsets.US_ASCII);
        try { return new Blob(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)), bytes.length); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    private record Fixture(BenchmarkReturnWorkBudgetProfile.Prepared prepared, HelperManifest helper, JdkManifest jdk,
                           ActualFacts actual, OwnedRuntimes owners) {
        Map<String, Object> verify() { return BenchmarkReturnWorkBudgetBindings.verify(prepared, helper, jdk, actual, owners); }
        Fixture withHelper(HelperManifest h) { return new Fixture(prepared, h, jdk, actual, owners); }
        Fixture withActual(ActualFacts a) { return new Fixture(prepared, helper, jdk, a, owners); }
    }
}
