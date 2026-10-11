package io.tapstate.e2e;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.CodeSource;
import java.security.MessageDigest;
import java.security.ProtectionDomain;
import java.util.EnumMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import io.tapstate.e2e.BenchmarkReturnWorkBudgetBindings.Runtime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static io.tapstate.e2e.BenchmarkReturnWorkBudgetBindings.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

class BenchmarkReturnRootOriginsTest {
    @TempDir Path temporary;

    @Test void real_package_class_binds_actual_runtime_code_source_and_resource_bytes_without_instruction_credit() throws Exception {
        var fixture = fixture(BenchmarkReturnRootOrigins.class);
        var result = fixture.observe();
        assertThat(result.observedOrigins()).containsExactly(new ObservedOrigin(fixture.origin(), fixture.runtime()));
        assertThat(result.evidence()).containsEntry("state", "ROOT_NAMED_CLASS_ORIGINS_RECORDED")
                .containsEntry("classCount", 1).containsEntry("consumedBytes", (long) fixture.bytes().length)
                .containsEntry("hashedBytes", (long) fixture.bytes().length)
                .containsEntry("sourceCoverageQualified", false).containsEntry("emittedMemberCompletenessQualified", false)
                .containsEntry("runtimeInstructionBytesQualified", false).containsEntry("wholeMethodCostQualified", false);
        assertThatThrownBy(result.observedOrigins()::clear).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(result.evidence()::clear).isInstanceOf(UnsupportedOperationException.class);
        var row = (Map<?, ?>) ((List<?>) result.evidence().get("classes")).getFirst();
        assertThat(row.get("loaderClass")).isEqualTo(BenchmarkReturnRootOrigins.class.getClassLoader().getClass().getName());
        assertThatThrownBy(row::clear).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void false_initialization_resolves_the_real_class_without_running_its_static_initializer() throws Exception {
        InitializationWitness.calls.set(0);
        var fixture = fixture(LazyClass.class);
        assertThat(InitializationWitness.calls).hasValue(0);
        var result = fixture.observe();
        assertThat(result.observedOrigins()).hasSize(1);
        assertThat(InitializationWitness.calls).hasValue(0);
        Class.forName(LazyClass.class.getName(), true, LazyClass.class.getClassLoader());
        assertThat(InitializationWitness.calls).hasValue(1);
    }

    @Test void a_local_jar_class_resource_is_bound_to_its_actual_owning_jar_and_closes_after_reading() throws Exception {
        var original = fixture(JarClass.class);
        Path jar = temporary.resolve("owned-classes.jar");
        try (var bytes = new ZipOutputStream(Files.newOutputStream(jar))) {
            bytes.putNextEntry(new ZipEntry(original.origin().entry())); bytes.write(original.bytes()); bytes.closeEntry();
        }
        jar = jar.toRealPath();
        try (var loader = new URLClassLoader(new URL[]{jar.toUri().toURL()}, null)) {
            var fixture = original.at(jar);
            var result = BenchmarkReturnRootOrigins.observeRoot(fixture.helper(), fixture.classpath(), fixture.runtime(), null,
                    name -> Class.forName(name, false, loader), connection -> connection.getInputStream());
            assertThat(result.observedOrigins()).containsExactly(new ObservedOrigin(fixture.origin(), fixture.runtime()));
            assertThat(result.evidence()).containsEntry("consumedBytes", (long) original.bytes().length);
        }
        Files.delete(jar);
        assertThat(Files.exists(jar)).isFalse();
    }

    @Test void wrong_actual_runtime_refuses_before_resolving_or_opening_any_class() throws Exception {
        var fixture = fixture(BenchmarkReturnRootOrigins.class); var runtime = fixture.runtime();
        var foreign = new Runtime(Owner.ROOT, runtime.runId(), runtime.pid() + 1, runtime.jvmStartTimeMillis(),
                runtime.javaHome(), runtime.javaVersion());
        var resolved = new AtomicInteger(); var opened = new AtomicInteger();
        assertThatThrownBy(() -> BenchmarkReturnRootOrigins.observeRoot(fixture.helper(), fixture.classpath(), foreign, null,
                name -> { resolved.incrementAndGet(); return BenchmarkReturnRootOrigins.class; },
                connection -> { opened.incrementAndGet(); return connection.getInputStream(); }))
                .isInstanceOf(AssertionError.class).hasMessageContaining("actual root runtime differs");
        assertThat(resolved).hasValue(0); assertThat(opened).hasValue(0);
    }

    @Test void matching_declared_hashes_do_not_repair_a_wrong_code_source_or_shadow_loader_resource() throws Exception {
        var original = fixture(ShadowClass.class);
        Path other = Files.createDirectory(temporary.resolve("foreign-code-source")).toRealPath();
        var foreign = original.at(other); var opened = new AtomicInteger();
        assertThatThrownBy(() -> BenchmarkReturnRootOrigins.observeRoot(foreign.helper(), foreign.classpath(), foreign.runtime(), null,
                name -> ShadowClass.class, connection -> { opened.incrementAndGet(); return connection.getInputStream(); }))
                .isInstanceOf(AssertionError.class).hasMessageContaining("actual code source differs");
        assertThat(opened).hasValue(0);
        Path shadow = other.resolve(original.origin().entry()); Files.createDirectories(shadow.getParent()); Files.write(shadow, original.bytes());
        var loader = new ShadowLoader(original.bytes(), original.origin().container(), shadow.toUri().toURL());
        assertThatThrownBy(() -> BenchmarkReturnRootOrigins.observeRoot(original.helper(), original.classpath(), original.runtime(), null,
                name -> loader.defined, connection -> { opened.incrementAndGet(); return connection.getInputStream(); }))
                .isInstanceOf(AssertionError.class).hasMessageContaining("shadow class resource differs");
        var network = new ShadowLoader(original.bytes(), original.origin().container(), new URL("https://example.invalid/foreign.class"));
        assertThatThrownBy(() -> BenchmarkReturnRootOrigins.observeRoot(original.helper(), original.classpath(), original.runtime(), null,
                name -> network.defined, connection -> { opened.incrementAndGet(); return connection.getInputStream(); }))
                .isInstanceOf(AssertionError.class).hasMessageContaining("resource protocol does not match");
        assertThat(opened).hasValue(0);
    }

    @Test void a_bad_hash_or_truncated_zero_progress_stream_cannot_create_an_observed_origin() throws Exception {
        var fixture = fixture(BenchmarkReturnRootOrigins.class);
        var bad = fixture.withBlob(new Blob("f".repeat(64), fixture.bytes().length));
        assertThatThrownBy(bad::observe).isInstanceOf(AssertionError.class).hasMessageContaining("actual class resource digest");
        for (InputStream input : List.of(new ByteArrayInputStream(new byte[0]), new InputStream() {
            @Override public int read() { return 0; }
            @Override public int read(byte[] bytes, int offset, int length) { return 0; }
        })) {
            assertThatThrownBy(() -> BenchmarkReturnRootOrigins.observeRoot(fixture.helper(), fixture.classpath(), fixture.runtime(), null,
                    name -> BenchmarkReturnRootOrigins.class, connection -> input))
                    .isInstanceOf(AssertionError.class).hasMessageContaining(input instanceof ByteArrayInputStream
                            ? "class stream length differs" : "makes no progress");
        }
    }

    @Test void prior_member_cap_duplicate_roster_and_nonlocal_packages_refuse_before_class_resolution() throws Exception {
        var fixture = fixture(BenchmarkReturnRootOrigins.class); var resolved = new AtomicInteger();
        var oversized = fixture.withBlob(new Blob(fixture.origin().classBytes().sha256(), MAX_MEMBER_BYTES + 1));
        assertThatThrownBy(() -> BenchmarkReturnRootOrigins.observeRoot(oversized.helper(), oversized.classpath(), oversized.runtime(), null,
                name -> { resolved.incrementAndGet(); return BenchmarkReturnRootOrigins.class; }, connection -> connection.getInputStream()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("prior material cap");
        var h = fixture.helper();
        var duplicate = helper(h, List.of(h.classes().getFirst(), h.classes().getFirst()), h.origins());
        assertThatThrownBy(() -> BenchmarkReturnRootOrigins.observeRoot(duplicate, fixture.classpath(), fixture.runtime(), null,
                name -> { resolved.incrementAndGet(); return BenchmarkReturnRootOrigins.class; }, connection -> connection.getInputStream()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("repeated");
        var outside = new CompiledClass(SourceRole.ROOT_ORIGINS, "java.lang.String", fixture.origin().classBytes());
        var foreign = helper(h, List.of(outside), h.origins());
        assertThatThrownBy(() -> BenchmarkReturnRootOrigins.observeRoot(foreign, fixture.classpath(), fixture.runtime(), null,
                name -> { resolved.incrementAndGet(); return String.class; }, connection -> connection.getInputStream()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("fixed root packages");
        assertThat(resolved).hasValue(0);
    }

    @Test void a_changed_runtime_after_resolution_is_refused_instead_of_reusing_the_before_identity() throws Exception {
        var fixture = fixture(BenchmarkReturnRootOrigins.class); String before = System.getProperty("java.version");
        try {
            assertThatThrownBy(() -> BenchmarkReturnRootOrigins.observeRoot(fixture.helper(), fixture.classpath(), fixture.runtime(), null,
                    name -> { System.setProperty("java.version", before + ".changed"); return BenchmarkReturnRootOrigins.class; },
                    connection -> connection.getInputStream()))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("actual root runtime changed");
        } finally { System.setProperty("java.version", before); }
    }

    @Test void original_read_failure_and_close_suppression_are_retained_with_no_pending_or_clock_requests() throws Exception {
        var fixture = fixture(BenchmarkReturnRootOrigins.class);
        var primary = new IOException("controlled class read"); var cleanup = new IOException("controlled class close");
        var input = new InputStream() {
            @Override public int read() throws IOException { throw primary; }
            @Override public int read(byte[] buffer, int offset, int length) throws IOException { throw primary; }
            @Override public void close() throws IOException { throw cleanup; }
        };
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), fixture.runtime().runId())) {
            Throwable failure = catchThrowable(() -> BenchmarkReturnRootOrigins.observeRoot(fixture.helper(), fixture.classpath(), fixture.runtime(), scope,
                    name -> BenchmarkReturnRootOrigins.class, connection -> input));
            assertThat(failure).isSameAs(primary); assertThat(primary.getSuppressed()).containsExactly(cleanup);
            assertThat(scope.primaryCause()).isSameAs(primary);
            var evidence = scope.snapshot();
            assertThat(evidence).containsEntry("state", "UNKNOWN").containsEntry("pendingOperations", 0L);
            assertThat(actual(evidence, "CLOCK_REQUEST", "requests")).isZero();
            assertThat(row(evidence, "INSTALL_BINDINGS").get("attempts")).isEqualTo(1L);
        }
    }

    @Test void actual_actor_counts_cover_each_class_and_runtime_reads_without_a_clock_or_workload_factory() throws Exception {
        var fixture = fixture(BenchmarkReturnRootOrigins.class);
        var second = fixture(ShadowClass.class); var h = fixture.helper();
        var combined = helper(h, List.of(h.classes().getFirst(), second.helper().classes().getFirst()),
                List.of(h.origins().getFirst(), second.helper().origins().getFirst()));
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), fixture.runtime().runId())) {
            var result = BenchmarkReturnRootOrigins.observeRoot(combined, fixture.classpath(), fixture.runtime(), scope);
            var evidence = scope.snapshot();
            assertThat(row(evidence, "INSTALL_BINDINGS").get("attempts")).isEqualTo(2L);
            assertThat(row(evidence, "INSTALL_BINDINGS").get("completedAttempts")).isEqualTo(2L);
            assertThat(actual(evidence, "RUNTIME_READ", "requests")).isEqualTo(12);
            assertThat(actual(evidence, "CLOCK_REQUEST", "requests")).isZero();
            assertThat(actual(evidence, "INSTALL_BINDINGS", "records")).isEqualTo(2);
            long consumed = (long) fixture.bytes().length + second.bytes().length;
            assertThat(actual(evidence, "DOCUMENT_READ", "bytes")).isEqualTo(consumed);
            assertThat(actual(evidence, "INPUT_FINGERPRINT", "bytes")).isEqualTo(consumed);
            assertThat(result.evidence()).containsEntry("serviceRequestUpper", BenchmarkReturnRootOrigins.serviceRequestUpper(2, consumed));
            assertThat((Long) result.evidence().get("serviceRequests")).isLessThanOrEqualTo(
                    BenchmarkReturnRootOrigins.serviceRequestUpper(2, consumed));
            assertThat(result.evidence()).containsEntry("performanceAcceptanceEligible", false).containsEntry("formalPerformance", false);
            assertThat(BenchmarkReturnRootOrigins.localUpper().bytes()).isGreaterThan(MAX_MEMBER_BYTES);
            assertThatThrownBy(() -> BenchmarkReturnRootOrigins.localUpper().times(Long.MAX_VALUE))
                    .isInstanceOf(ArithmeticException.class);
        }
    }

    // These classes exercise genuine JVM resolution. Their test source grouping is not source authentication.
    static final class LazyClass { static { InitializationWitness.calls.incrementAndGet(); } }
    static final class InitializationWitness { static final AtomicInteger calls = new AtomicInteger(); }
    static final class ShadowClass { }
    static final class JarClass { }

    private static final class ShadowLoader extends ClassLoader {
        final Class<?> defined; private final URL shadow;
        ShadowLoader(byte[] bytes, String codeSource, URL shadow) throws Exception {
            super(BenchmarkReturnRootOrigins.class.getClassLoader()); this.shadow = shadow;
            var domain = new ProtectionDomain(new CodeSource(Path.of(codeSource).toUri().toURL(), (java.security.cert.Certificate[]) null),
                    null, this, null);
            defined = defineClass(ShadowClass.class.getName(), bytes, 0, bytes.length, domain);
        }
        @Override public URL getResource(String name) { return shadow; }
    }

    private Fixture fixture(Class<?> type) throws Exception {
        Path container = Path.of(type.getProtectionDomain().getCodeSource().getLocation().toURI()).toRealPath();
        String entry = type.getName().replace('.', '/') + ".class";
        byte[] bytes;
        try (InputStream input = type.getResourceAsStream("/" + entry)) {
            bytes = input.readNBytes((int) MAX_MEMBER_BYTES + 1);
        }
        assertThat(bytes.length).isLessThanOrEqualTo((int) MAX_MEMBER_BYTES);
        var blob = new Blob(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)), bytes.length);
        var origin = new Origin(Owner.ROOT, SourceRole.ROOT_ORIGINS, type.getName(), container.toString(), entry, blob, null);
        var runtime = new Runtime(Owner.ROOT, "controlled-root-origins", ProcessHandle.current().pid(),
                ManagementFactory.getRuntimeMXBean().getStartTime(), Path.of(System.getProperty("java.home")).toRealPath().toString(),
                System.getProperty("java.version"));
        var h = new HelperManifest(1, runtime.runId(), "a".repeat(64), "controlled-build", "controlled-definition",
                "b".repeat(64), BenchmarkReturnWorkBudgetProducerAllocations.FORMULA_SHA256, new Blob("c".repeat(64), 1),
                List.of(container.toString()), List.of(), List.of(new CompiledClass(origin.role(), origin.binaryName(), blob)), List.of(origin));
        return new Fixture(h, runtime, origin, bytes);
    }

    private static HelperManifest helper(HelperManifest h, List<CompiledClass> classes, List<Origin> origins) {
        return new HelperManifest(h.schemaVersion(), h.runId(), h.inputSha256(), h.buildId(), h.rootDefinitionId(),
                h.rootFormulaSha256(), h.producerFormulaSha256(), h.failsafeXml(), h.failsafeClasspath(), h.sources(), classes, origins);
    }

    private record Fixture(HelperManifest helper, Runtime runtime, Origin origin, byte[] bytes) {
        List<String> classpath() { return helper.failsafeClasspath(); }
        BenchmarkReturnRootOrigins.RootOrigins observe() throws Exception {
            return BenchmarkReturnRootOrigins.observeRoot(helper, classpath(), runtime, null);
        }
        Fixture withBlob(Blob blob) {
            var changed = new Origin(origin.owner(), origin.role(), origin.binaryName(), origin.container(), origin.entry(), blob, null);
            return new Fixture(BenchmarkReturnRootOriginsTest.helper(helper,
                    List.of(new CompiledClass(changed.role(), changed.binaryName(), blob)), List.of(changed)), runtime, changed, bytes);
        }
        Fixture at(Path container) {
            var changed = new Origin(origin.owner(), origin.role(), origin.binaryName(), container.toString(), origin.entry(), origin.classBytes(), null);
            var h = helper;
            var registration = new HelperManifest(h.schemaVersion(), h.runId(), h.inputSha256(), h.buildId(), h.rootDefinitionId(),
                    h.rootFormulaSha256(), h.producerFormulaSha256(), h.failsafeXml(), List.of(container.toString()),
                    h.sources(), h.classes(), List.of(changed));
            return new Fixture(registration, runtime, changed, bytes);
        }
    }

    private static BenchmarkReturnWorkBudget.Definition definition() {
        var upper = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        for (var activity : BenchmarkReturnWorkBudget.Activity.values()) upper.put(activity,
                new BenchmarkReturnWorkBudget.Work(1_000_000, 1_000_000, 1_000_000, 1_000_000, 1_000_000_000,
                        1_000_000, 1_000_000, 1_000_000));
        return new BenchmarkReturnWorkBudget.Definition("controlled-root-origins", "d".repeat(64), Map.of(), upper, Set.of());
    }
    private static Map<?, ?> row(Map<String, Object> evidence, String activity) {
        return (Map<?, ?>) ((Map<?, ?>) evidence.get("activities")).get(activity);
    }
    private static long actual(Map<String, Object> evidence, String activity, String unit) {
        return ((Number) ((Map<?, ?>) row(evidence, activity).get("actual")).get(unit)).longValue();
    }
}
