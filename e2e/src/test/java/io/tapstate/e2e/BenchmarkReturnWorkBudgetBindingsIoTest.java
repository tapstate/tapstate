package io.tapstate.e2e;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.CRC32;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static io.tapstate.e2e.BenchmarkReturnWorkBudgetBindings.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

class BenchmarkReturnWorkBudgetBindingsIoTest {
    @TempDir Path temporary;

    @Test void actual_source_directory_jar_nested_class_manifest_and_jdk_bytes_are_authenticated_without_runtime_credit() throws Exception {
        var fixture = fixture(false, false);
        var result = fixture.authenticate();
        assertThat(result.files()).containsExactlyElementsOf(fixture.files());
        assertThat(result.sources()).containsExactlyElementsOf(fixture.helper().sources());
        assertThat(result.classes()).containsExactlyElementsOf(fixture.helper().classes());
        assertThat(result.origins()).containsExactlyInAnyOrderElementsOf(fixture.helper().origins());
        assertThat(result.jdkFiles()).containsExactlyElementsOf(fixture.jdk().files());
        assertThat(result.evidence()).containsEntry("state", "DECLARED_BYTE_FACTS_AUTHENTICATED")
                .containsEntry("wholeMethodCostQualified", false).containsEntry("runtimeOriginAuthenticationQualified", false)
                .containsEntry("runtimeInstructionBytesQualified", false).containsEntry("emittedMemberCompletenessQualified", false);
        assertThat((Long) result.evidence().get("consumedBytes")).isPositive();
        assertThat((Long) result.evidence().get("zipEntries")).isPositive();
        assertThatThrownBy(result.evidence()::clear).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void manifest_digest_alone_does_not_authenticate_a_different_typed_declaration() throws Exception {
        var fixture = fixture(false, false); var h = fixture.helper();
        var borrowed = new HelperManifest(h.schemaVersion(), h.runId(), h.inputSha256(), "another-build", h.rootDefinitionId(),
                h.rootFormulaSha256(), h.producerFormulaSha256(), h.failsafeXml(), h.failsafeClasspath(), h.sources(), h.classes(), h.origins());
        var opens = new AtomicInteger();
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetBindingsIo.authenticate(fixture.sourceRoot(), fixture.application(),
                fixture.files(), borrowed, fixture.jdk(), path -> { opens.incrementAndGet(); return Files.newInputStream(path); }))
                .isInstanceOf(AssertionError.class).hasMessageContaining("manifest object does not match registered bytes");
        assertThat(opens).hasValue(0);
    }

    @Test void source_digest_and_prior_length_refusals_use_real_files_and_do_not_infer_missing_bytes() throws Exception {
        var fixture = fixture(false, false);
        byte[] changed = Files.readAllBytes(fixture.firstSource()); changed[0] ^= 1; Files.write(fixture.firstSource(), changed);
        assertThatThrownBy(fixture::authenticate).isInstanceOf(AssertionError.class).hasMessageContaining("actual byte digest or length differs");
        var other = fixture(false, false); var first = other.helper().sources().getFirst();
        var prior = new Source(first.role(), first.path(), new Blob(first.blob().sha256(), first.blob().bytes() + 1));
        var wrongSize = source(other, prior); var opened = new AtomicInteger();
        assertThatThrownBy(() -> wrongSize.authenticate(path -> {
            if (path.equals(wrongSize.firstSource())) { opened.incrementAndGet(); }
            return Files.newInputStream(path);
        })).isInstanceOf(AssertionError.class).hasMessageContaining("actual file size differs from prior bytes");
        assertThat(opened).hasValue(0);
    }

    @Test void overflow_scale_prior_source_and_manifest_material_refuse_before_stream_access_or_large_buffer_allocation() throws Exception {
        var fixture = fixture(false, false); var first = fixture.helper().sources().getFirst();
        var extreme = source(fixture, new Source(first.role(), first.path(), new Blob(first.blob().sha256(), Long.MAX_VALUE - 1)));
        var opens = new AtomicInteger();
        assertThatThrownBy(() -> extreme.authenticate(path -> {
            if (path.equals(extreme.firstSource())) { opens.incrementAndGet(); }
            return Files.newInputStream(path);
        })).isInstanceOf(AssertionError.class).hasMessageContaining("exceeds prior byte cap");
        assertThat(opens).hasValue(0);
        var h = fixture.helper(); String longPath = "/" + "x".repeat(4095);
        var fields = java.util.Collections.nCopies(MAX_CLASSPATH, longPath);
        var oversized = new HelperManifest(h.schemaVersion(), h.runId(), h.inputSha256(), h.buildId(), h.rootDefinitionId(),
                h.rootFormulaSha256(), h.producerFormulaSha256(), h.failsafeXml(), fields, h.sources(), h.classes(), h.origins());
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetBindingsIo.helperBytes(oversized))
                .isInstanceOf(AssertionError.class).hasMessageContaining("manifest encoding exceeds prior byte cap");
    }

    @Test void linked_source_is_refused_before_open_even_when_the_target_bytes_match() throws Exception {
        var fixture = fixture(false, false); Path source = fixture.firstSource(); Path real = source.resolveSibling("owned-target.bin");
        Files.move(source, real); Files.createSymbolicLink(source, real.getFileName());
        var opens = new AtomicInteger();
        assertThatThrownBy(() -> fixture.authenticate(path -> {
            if (path.equals(source)) { opens.incrementAndGet(); }
            return Files.newInputStream(path);
        })).isInstanceOf(AssertionError.class).hasMessageContaining("linked file refused");
        assertThat(opens).hasValue(0);
    }

    @Test void foreign_archive_entry_and_nested_inflation_are_refused_without_unbounded_entry_copy() throws Exception {
        var foreign = fixture(true, false);
        assertThatThrownBy(foreign::authenticate).isInstanceOf(AssertionError.class).hasMessageContaining("foreign archive entry path");
        var inflated = fixture(false, true);
        assertThatThrownBy(inflated::authenticate).isInstanceOf(AssertionError.class).hasMessageContaining("stream exceeds prior byte cap");
    }

    @Test void live_scope_records_actual_io_hash_copy_and_close_requests_without_qualifying_the_method() throws Exception {
        var fixture = fixture(false, false);
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "controlled-io")) {
            var result = fixture.authenticate(); var receipt = scope.snapshot();
            long requests = actual(receipt, "DOCUMENT_READ", "requests") + actual(receipt, "INPUT_FINGERPRINT", "requests");
            assertThat(requests).isEqualTo(result.evidence().get("requests"));
            assertThat(actual(receipt, "DOCUMENT_READ", "bytes")).isEqualTo(result.evidence().get("consumedBytes"));
            assertThat(actual(receipt, "INPUT_FINGERPRINT", "bytes")).isEqualTo(result.evidence().get("hashedBytes"));
            assertThat(actual(receipt, "COPY", "bytes")).isPositive();
            assertThat(receipt).containsEntry("pendingOperations", 0L).containsEntry("wholeMethodCostQualified", false);
        }
    }

    @Test void zero_progress_and_read_close_failures_keep_exact_primary_suppressed_and_completed_request_facts() throws Exception {
        var fixture = fixture(false, false); var readFailure = new IOException("controlled read failure"); var closeFailure = new IOException("controlled close failure");
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "controlled-read-close")) {
            Throwable failure = catchThrowable(() -> fixture.authenticate(path -> {
                var actual = Files.newInputStream(path);
                if (!path.equals(fixture.firstSource())) { return actual; }
                return new FilterInputStream(actual) {
                    @Override public int read(byte[] buffer, int off, int length) throws IOException { throw readFailure; }
                    @Override public void close() throws IOException { super.close(); throw closeFailure; }
                };
            }));
            assertThat(failure).isSameAs(readFailure); assertThat(failure.getSuppressed()).containsExactly(closeFailure);
            assertThat(scope.primaryCause()).isSameAs(readFailure);
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN").containsEntry("pendingOperations", 0L);
        }
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "controlled-zero")) {
            Throwable failure = catchThrowable(() -> fixture.authenticate(path -> {
                var actual = Files.newInputStream(path);
                if (!path.equals(fixture.firstSource())) { return actual; }
                return new FilterInputStream(actual) { @Override public int read(byte[] buffer, int off, int length) { return 0; } };
            }));
            assertThat(failure).isInstanceOf(AssertionError.class).hasMessageContaining("stream read made no progress");
            assertThat(scope.primaryCause()).isSameAs(failure); assertThat(scope.snapshot()).containsEntry("pendingOperations", 0L);
        }
    }

    @Test void successful_read_with_failed_close_is_not_returned_as_authenticated_facts() throws Exception {
        var fixture = fixture(false, false); var closeFailure = new IOException("controlled close failure");
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "controlled-close")) {
            Throwable failure = catchThrowable(() -> fixture.authenticate(path -> {
                var actual = Files.newInputStream(path);
                if (!path.equals(fixture.firstSource())) { return actual; }
                return new FilterInputStream(actual) { @Override public void close() throws IOException { super.close(); throw closeFailure; } };
            }));
            assertThat(failure).isSameAs(closeFailure); assertThat(scope.primaryCause()).isSameAs(closeFailure);
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN").containsEntry("pendingOperations", 0L);
        }
    }

    @Test void zero_length_limited_read_has_one_owned_entry_and_no_underlying_request_then_uses_the_fixed_buffer_bound() throws Exception {
        var reads = new AtomicInteger(); var lastLength = new AtomicInteger();
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "controlled-limited-zero")) {
            var reader = new BenchmarkReturnWorkBudgetBindingsIo(path -> { throw new AssertionError("file open is outside this control"); });
            var original = new ByteArrayInputStream(new byte[8193]) {
                @Override public synchronized int read(byte[] buffer, int off, int length) {
                    reads.incrementAndGet(); lastLength.set(length); return super.read(buffer, off, length);
                }
            };
            try (var limited = reader.limitedStream(original, 8193)) {
                assertThat(limited.read(new byte[0], 0, 0)).isZero();
                assertThat(reads).hasValue(0);
                var entered = row(scope.snapshot(), "DOCUMENT_READ");
                assertThat(entered).containsEntry("attempts", 1L).containsEntry("completedAttempts", 1L)
                        .containsEntry("issuedRequests", 0L).containsEntry("failedAttempts", 0L);
                assertThat(((Map<?, ?>) entered.get("actual")).get("calls")).isEqualTo(1L);
                assertThat(limited.read(new byte[8193], 0, 8193)).isEqualTo(8192);
                assertThat(reads).hasValue(1); assertThat(lastLength).hasValue(8192);
                assertThat(actual(scope.snapshot(), "DOCUMENT_READ", "requests")).isEqualTo(1);
            }
            assertThat(actual(scope.snapshot(), "DOCUMENT_READ", "requests")).isEqualTo(2);
            assertThat(scope.snapshot()).containsEntry("pendingOperations", 0L);
        }
    }

    @Test void owned_limited_read_and_separate_read_close_services_keep_exact_failure_counts_and_primary() throws Exception {
        var read = new IOException("controlled limited read failure"); var close = new IOException("controlled limited close failure");
        var reads = new AtomicInteger(); var closes = new AtomicInteger();
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "controlled-limited-failure")) {
            var reader = new BenchmarkReturnWorkBudgetBindingsIo(path -> { throw new AssertionError("file open is outside this control"); });
            var underlying = new InputStream() {
                @Override public int read() throws IOException { reads.incrementAndGet(); throw read; }
                @Override public void close() throws IOException { closes.incrementAndGet(); throw close; }
            };
            Throwable failure = catchThrowable(() -> { try (var limited = reader.limitedStream(underlying, 1)) { limited.read(); } });
            assertThat(failure).isSameAs(read); assertThat(failure.getSuppressed()).containsExactly(close);
            assertThat(reads).hasValue(1); assertThat(closes).hasValue(1); assertThat(scope.primaryCause()).isSameAs(read);
            var receipt = scope.snapshot(); var entered = row(receipt, "DOCUMENT_READ");
            assertThat(entered).containsEntry("attempts", 3L).containsEntry("completedAttempts", 3L)
                    .containsEntry("failedAttempts", 3L).containsEntry("issuedRequests", 2L)
                    .containsEntry("completedRequests", 2L).containsEntry("failedRequests", 2L);
            assertThat(receipt).containsEntry("state", "UNKNOWN").containsEntry("pendingOperations", 0L);
        }
    }

    @Test void standalone_authentication_has_its_own_complete_or_failed_install_entry() throws Exception {
        var fixture = fixture(false, false);
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "controlled-install-success")) {
            fixture.authenticate();
            assertThat(row(scope.snapshot(), "INSTALL_BINDINGS")).containsEntry("attempts", 1L)
                    .containsEntry("completedAttempts", 1L).containsEntry("failedAttempts", 0L).containsEntry("issuedRequests", 0L);
        }
        var primary = new IOException("controlled authentication open failure");
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "controlled-install-failure")) {
            Throwable failure = catchThrowable(() -> fixture.authenticate(path -> { throw primary; }));
            assertThat(failure).isSameAs(primary); assertThat(scope.primaryCause()).isSameAs(primary);
            assertThat(row(scope.snapshot(), "INSTALL_BINDINGS")).containsEntry("attempts", 1L)
                    .containsEntry("completedAttempts", 1L).containsEntry("failedAttempts", 1L).containsEntry("issuedRequests", 0L);
            assertThat(scope.snapshot()).containsEntry("pendingOperations", 0L);
        }
    }

    @Test void local_source_upper_counts_request_predicate_and_material_allowances_separately_from_the_codec() {
        var upper = BenchmarkReturnWorkBudgetBindingsIo.localIoUpper();
        assertThat(upper.keySet()).containsExactlyInAnyOrder(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ,
                BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT, BenchmarkReturnWorkBudget.Activity.COPY,
                BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS);
        var document = upper.get(BenchmarkReturnWorkBudget.Activity.DOCUMENT_READ);
        long boundedOwnedVisits = 4096 + 1024 + 8192;
        long explicitPathEntryAndReadApiLower = 6 + 12 + 6;
        assertThat(document.requests()).isGreaterThanOrEqualTo(boundedOwnedVisits + explicitPathEntryAndReadApiLower);
        assertThat(document.calls()).isGreaterThanOrEqualTo(boundedOwnedVisits);
        assertThat(document.bytes()).isGreaterThanOrEqualTo(8192 + 2 * (4096 + 1024));
        var fingerprint = upper.get(BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT);
        assertThat(fingerprint.requests()).isGreaterThanOrEqualTo(3); assertThat(fingerprint.bytes()).isGreaterThanOrEqualTo(32 + 64);
        assertThat(upper.get(BenchmarkReturnWorkBudget.Activity.COPY).bytes()).isEqualTo(8192);
        long roster = BenchmarkReturnWorkBudgetProfile.FileRole.values().length + SourceRole.values().length
                + 512 + 1024 + JdkRole.values().length + 1024 + 32;
        assertThat(upper.get(BenchmarkReturnWorkBudget.Activity.INSTALL_BINDINGS).requests()).isGreaterThanOrEqualTo(64 * roster);
        assertThatThrownBy(upper::clear).isInstanceOf(UnsupportedOperationException.class);
    }

    /** Owned byte fixtures are never loaded or executed as classes or JDK artifacts. */
    private Fixture fixture(boolean foreign, boolean inflated) throws Exception {
        Path parent = Files.createTempDirectory(temporary, "binding-io-").toRealPath();
        Path sourceRoot = Files.createDirectory(parent.resolve("source")).toRealPath();
        Path rootClasses = Files.createDirectory(parent.resolve("classes")).toRealPath();
        Path home = Files.createDirectory(parent.resolve("jdk")).toRealPath();
        var sources = new ArrayList<Source>();
        for (var role : List.of(SourceRole.ROOT_BUDGET, SourceRole.PDK_CLOCK)) {
            byte[] bytes = ("controlled-source-" + role).getBytes(java.nio.charset.StandardCharsets.US_ASCII);
            Path path = sourceRoot.resolve(role.sourcePath()); Files.createDirectories(path.getParent()); Files.write(path, bytes);
            sources.add(new Source(role, role.sourcePath(), blob(bytes)));
        }
        var classes = new ArrayList<CompiledClass>(); var rootEntries = new LinkedHashMap<String, byte[]>();
        byte[] marker = {(byte) 0xca, (byte) 0xfe, (byte) 0xba, (byte) 0xbe, 0, 0, 0, 65};
        for (String suffix : List.of("", "$Clock", "$JniAccess", "$NativeAccess")) {
            String binary = SourceRole.PDK_CLOCK.binaryName() + suffix;
            classes.add(new CompiledClass(SourceRole.PDK_CLOCK, binary, blob(marker)));
            rootEntries.put(binary.replace('.', '/') + ".class", marker);
        }
        classes.add(new CompiledClass(SourceRole.ROOT_BUDGET, SourceRole.ROOT_BUDGET.binaryName(), blob(marker)));
        Path rootMember = rootClasses.resolve(SourceRole.ROOT_BUDGET.binaryName().replace('.', '/') + ".class");
        Files.createDirectories(rootMember.getParent()); Files.write(rootMember, marker);
        var sourceEntries = new LinkedHashMap<>(rootEntries);
        if (foreign) { sourceEntries.put("../escape.class", marker); }
        Path rootJar = parent.resolve("adapter-pdk.jar"); Files.write(rootJar, zip(sourceEntries, false));
        var moduleEntries = new LinkedHashMap<String, byte[]>(); moduleEntries.put("META-INF/", new byte[0]); moduleEntries.putAll(rootEntries);
        if (inflated) { moduleEntries.put("padding.bin", new byte[16_384]); }
        byte[] module = zip(moduleEntries, false);
        Path app = parent.resolve("application.jar");
        Files.write(app, zip(Map.of("BOOT-INF/lib/adapter-pdk-control.jar", module), true));
        Blob appBlob = blob(Files.readAllBytes(app));
        var origins = new ArrayList<Origin>();
        for (var item : classes) {
            String member = item.binaryName().replace('.', '/') + ".class";
            origins.add(new Origin(Owner.ROOT, item.role(), item.binaryName(), item.role() == SourceRole.ROOT_BUDGET
                    ? rootClasses.toString() : rootJar.toString(), member, item.blob(), null));
            if (item.role() == SourceRole.PDK_CLOCK) {
                origins.add(new Origin(Owner.PRODUCER, item.role(), item.binaryName(), app.toString(),
                        "BOOT-INF/lib/adapter-pdk-control.jar!/" + member, item.blob(), appBlob.sha256()));
            }
        }
        var jdkFiles = new ArrayList<JdkFile>();
        for (var role : List.of(JdkRole.JAVA, JdkRole.JVM, JdkRole.MODULES, JdkRole.RELEASE)) {
            byte[] bytes = ("controlled-jdk-" + role).getBytes(java.nio.charset.StandardCharsets.US_ASCII);
            Path path = home.resolve(role.relativePath()); Files.createDirectories(path.getParent()); Files.write(path, bytes);
            jdkFiles.add(new JdkFile(role, path.toString(), blob(bytes)));
        }
        String input = blob(new byte[] {1}).sha256();
        var helper = new HelperManifest(1, "controlled-io", input, "controlled-build", "controlled-definition",
                blob(new byte[] {2}).sha256(), BenchmarkReturnWorkBudgetProducerAllocations.FORMULA_SHA256,
                blob(new byte[] {3}), List.of(rootClasses.toString(), rootJar.toString()), sources, classes, origins);
        var jdk = new JdkManifest(1, helper.runId(), input, home.toString(), "21.0.9", jdkFiles);
        Path helperFile = parent.resolve("helper.manifest"), jdkFile = parent.resolve("jdk.manifest");
        byte[] helperRaw = BenchmarkReturnWorkBudgetBindingsIo.helperBytes(helper), jdkRaw = BenchmarkReturnWorkBudgetBindingsIo.jdkBytes(jdk);
        Files.write(helperFile, helperRaw); Files.write(jdkFile, jdkRaw);
        return new Fixture(sourceRoot, app, List.of(new FileFact(BenchmarkReturnWorkBudgetProfile.FileRole.APP_JAR, app.toString(), appBlob),
                new FileFact(BenchmarkReturnWorkBudgetProfile.FileRole.COMMON_HELPER_MANIFEST, helperFile.toString(), blob(helperRaw)),
                new FileFact(BenchmarkReturnWorkBudgetProfile.FileRole.JDK_INPUT_MANIFEST, jdkFile.toString(), blob(jdkRaw))), helper, jdk);
    }

    private static Fixture source(Fixture fixture, Source source) throws Exception {
        var h = fixture.helper(); var sources = new ArrayList<>(h.sources()); sources.set(0, source);
        var changed = new HelperManifest(h.schemaVersion(), h.runId(), h.inputSha256(), h.buildId(), h.rootDefinitionId(),
                h.rootFormulaSha256(), h.producerFormulaSha256(), h.failsafeXml(), h.failsafeClasspath(), sources, h.classes(), h.origins());
        byte[] bytes = BenchmarkReturnWorkBudgetBindingsIo.helperBytes(changed); var files = new ArrayList<>(fixture.files());
        int index = java.util.stream.IntStream.range(0, files.size()).filter(i -> files.get(i).role() == BenchmarkReturnWorkBudgetProfile.FileRole.COMMON_HELPER_MANIFEST).findFirst().orElseThrow();
        var original = files.get(index); Files.write(Path.of(original.path()), bytes);
        files.set(index, new FileFact(original.role(), original.path(), blob(bytes)));
        return new Fixture(fixture.sourceRoot(), fixture.application(), files, changed, fixture.jdk());
    }
    private static byte[] zip(Map<String, byte[]> entries, boolean stored) throws IOException {
        var buffer = new ByteArrayOutputStream();
        try (var zip = new ZipOutputStream(buffer)) {
            for (var item : entries.entrySet()) {
                var entry = new ZipEntry(item.getKey());
                if (stored) { var crc = new CRC32(); crc.update(item.getValue()); entry.setMethod(ZipEntry.STORED); entry.setSize(item.getValue().length); entry.setCompressedSize(item.getValue().length); entry.setCrc(crc.getValue()); }
                zip.putNextEntry(entry); zip.write(item.getValue()); zip.closeEntry();
            }
        }
        return buffer.toByteArray();
    }
    private static Blob blob(byte[] bytes) throws Exception {
        return new Blob(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)), bytes.length);
    }
    private static BenchmarkReturnWorkBudget.Definition definition() {
        var upper = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        for (var activity : BenchmarkReturnWorkBudget.Activity.values()) {
            upper.put(activity, new BenchmarkReturnWorkBudget.Work(1_000_000, 1_000_000, 1_000_000, 1_000_000,
                    1_000_000_000, 1_000_000, 1_000_000, 1_000_000));
        }
        return new BenchmarkReturnWorkBudget.Definition("controlled-io", "a".repeat(64), Map.of(), upper, Set.of());
    }
    private static long actual(Map<String, Object> receipt, String activity, String unit) {
        return ((Number) ((Map<?, ?>) row(receipt, activity).get("actual")).get(unit)).longValue();
    }
    @SuppressWarnings("unchecked")
    private static Map<String, Object> row(Map<String, Object> receipt, String activity) {
        return (Map<String, Object>) ((Map<?, ?>) receipt.get("activities")).get(activity);
    }
    private record Fixture(Path sourceRoot, Path application, List<FileFact> files, HelperManifest helper, JdkManifest jdk) {
        Path firstSource() { return sourceRoot.resolve(helper.sources().getFirst().path()); }
        BenchmarkReturnWorkBudgetBindingsIo.Result authenticate() throws IOException { return BenchmarkReturnWorkBudgetBindingsIo.authenticate(sourceRoot, application, files, helper, jdk); }
        BenchmarkReturnWorkBudgetBindingsIo.Result authenticate(BenchmarkReturnWorkBudgetBindingsIo.Streams streams) throws IOException {
            return BenchmarkReturnWorkBudgetBindingsIo.authenticate(sourceRoot, application, files, helper, jdk, streams);
        }
    }
}
