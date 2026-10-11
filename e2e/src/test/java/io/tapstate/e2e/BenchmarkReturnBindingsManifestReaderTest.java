package io.tapstate.e2e;

import java.io.FilterInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static io.tapstate.e2e.BenchmarkReturnWorkBudgetBindings.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

class BenchmarkReturnBindingsManifestReaderTest {
    @TempDir Path temporary;

    @Test void canonical_v2_roundtrip_keeps_root_writer_and_separate_producer_writer_bytes_distinct() throws Exception {
        var helper = separateWriterHelper(); byte[] bytes = BenchmarkReturnWorkBudgetBindingsIo.helperBytes(helper); var file = file(bytes);
        var decoded = BenchmarkReturnBindingsManifestReader.readHelper(file.path(), file.blob());
        assertThat(decoded).isEqualTo(helper); assertThat(BenchmarkReturnWorkBudgetBindingsIo.helperBytes(decoded)).containsExactly(bytes);
        assertThat(decoded.schemaVersion()).isEqualTo(PRODUCER_WRITER_SCHEMA_VERSION);
        assertThat(decoded.producerWriter()).isEqualTo(helper.producerWriter());
        var root = decoded.origins().stream().filter(o -> o.owner() == Owner.ROOT && o.role() == SourceRole.PDK_WRITER).findFirst().orElseThrow();
        var producer = decoded.origins().stream().filter(o -> o.owner() == Owner.PRODUCER).findFirst().orElseThrow();
        assertThat(root.classBytes()).isNotEqualTo(producer.classBytes());
        assertThat(producer.classBytes()).isEqualTo(decoded.producerWriter().compiled().blob());
        assertThat(BenchmarkReturnBindingsManifestReader.maximumFields(1)).isEqualTo(BenchmarkReturnBindingsManifestReader.maximumFields());
        assertThat(BenchmarkReturnBindingsManifestReader.maximumFields(2)).isGreaterThan(BenchmarkReturnBindingsManifestReader.maximumFields());
    }

    @Test void v2_missing_duplicate_or_foreign_writer_fields_refuse_before_an_origin_can_override_root_bytes() throws Exception {
        var helper = separateWriterHelper(); var values = fields(BenchmarkReturnWorkBudgetBindingsIo.helperBytes(helper));
        int at = values.indexOf(helper.producerWriter().sourceRoot());
        var absent = new ArrayList<>(values); absent.set(at - 2, "0"); refuse(absent, "producer writer tuple is missing");
        var repeated = new ArrayList<>(values); repeated.set(at - 2, "2"); refuse(repeated, "numeric value exceeds prior cap");
        var owner = new ArrayList<>(values); owner.set(at - 1, "ROOT"); refuse(owner, "producer writer owner");
        var role = new ArrayList<>(values); role.set(at + 1, "PDK_PROBE"); refuse(role, "role binary or source path differs");
        var path = new ArrayList<>(values); path.set(at + 2, "../PdkSinkWriter.java"); refuse(path, "role binary or source path differs");
        var member = new ArrayList<>(values); member.set(at + 6, SourceRole.PDK_WRITER.binaryName() + "$Unregistered");
        refuse(member, "role binary or source path differs");
        var excessive = new ArrayList<>(values); excessive.set(at + 4, Long.toString(MAX_MEMBER_BYTES + 1));
        refuse(excessive, "numeric value exceeds prior cap");
        var substituted = new ArrayList<>(values);
        for (int i = at + 9; i < substituted.size() - 6; i++) if (substituted.get(i).equals("ROOT") && substituted.get(i + 1).equals("PDK_WRITER")) {
            substituted.set(i + 5, helper.producerWriter().compiled().blob().sha256()); break;
        }
        refuse(substituted, "origin source owner or compiled bytes differ");
    }

    @Test void original_file_roundtrip_preserves_every_typed_role_blob_order_and_nullable_application_digest() throws Exception {
        var helper = helper(); byte[] raw = BenchmarkReturnWorkBudgetBindingsIo.helperBytes(helper); var file = file(raw);
        var decoded = BenchmarkReturnBindingsManifestReader.readHelper(file.path(), file.blob());
        assertThat(decoded).isEqualTo(helper);
        assertThat(BenchmarkReturnWorkBudgetBindingsIo.helperBytes(decoded)).containsExactly(raw);
        assertThat(decoded.origins().getFirst().applicationSha256()).isNull();
        assertThatThrownBy(() -> decoded.classes().clear()).isInstanceOf(UnsupportedOperationException.class);
        var jdk = jdk(); byte[] jdkRaw = BenchmarkReturnWorkBudgetBindingsIo.jdkBytes(jdk); var jdkFile = file(jdkRaw);
        var parsed = BenchmarkReturnBindingsManifestReader.readJdk(jdkFile.path(), jdkFile.blob());
        assertThat(parsed).isEqualTo(jdk);
        assertThat(BenchmarkReturnWorkBudgetBindingsIo.jdkBytes(parsed)).containsExactly(jdkRaw);
    }

    @Test void truncated_field_extra_field_and_trailing_bytes_are_not_partial_success() throws Exception {
        byte[] raw = BenchmarkReturnWorkBudgetBindingsIo.helperBytes(helper());
        for (byte[] invalid : List.of(Arrays.copyOf(raw, raw.length - 1), join(raw, "1:x\n"), join(raw, "\n"))) {
            var file = file(invalid);
            assertThatThrownBy(() -> BenchmarkReturnBindingsManifestReader.readHelper(file.path(), file.blob()))
                    .isInstanceOf(AssertionError.class);
        }
    }

    @Test void length_prefix_and_newline_ascii_guards_run_before_declared_field_allocation() throws Exception {
        byte[] raw = BenchmarkReturnWorkBudgetBindingsIo.helperBytes(helper()); String text = new String(raw, StandardCharsets.US_ASCII);
        var leading = file(text.replaceFirst("18:", "018:").getBytes(StandardCharsets.US_ASCII));
        assertThatThrownBy(() -> BenchmarkReturnBindingsManifestReader.readHelper(leading.path(), leading.blob()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("leading field length");
        var huge = file(("4097:" + text.substring(text.indexOf(':') + 1)).getBytes(StandardCharsets.US_ASCII));
        assertThatThrownBy(() -> BenchmarkReturnBindingsManifestReader.readHelper(huge.path(), huge.blob()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("field length exceeds prior cap");
        byte[] newline = raw.clone(); newline[2] = ';'; var delimiter = file(newline);
        assertThatThrownBy(() -> BenchmarkReturnBindingsManifestReader.readHelper(delimiter.path(), delimiter.blob()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("length delimiter");
        byte[] ascii = raw.clone(); ascii[3] = (byte) 0x80; var nonAscii = file(ascii);
        assertThatThrownBy(() -> BenchmarkReturnBindingsManifestReader.readHelper(nonAscii.path(), nonAscii.blob()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("printable ASCII");
    }

    @Test void unknown_enum_excess_count_numeric_overflow_and_wrong_positional_types_refuse() throws Exception {
        var fields = fields(BenchmarkReturnWorkBudgetBindingsIo.helperBytes(helper()));
        var badRole = new ArrayList<>(fields); badRole.set(13, "FOREIGN_SOURCE"); refuse(badRole, "source enum differs");
        var count = new ArrayList<>(fields); count.set(17, "513"); refuse(count, "numeric value exceeds prior cap");
        var overflow = new ArrayList<>(fields); overflow.set(9, "9223372036854775808"); refuse(overflow, "numeric value overflowed");
        var zero = new ArrayList<>(fields); zero.set(9, "0"); refuse(zero, "byte fact is not positive");
        var leading = new ArrayList<>(fields); leading.set(1, "01"); refuse(leading, "leading numeric value");
        var ordered = new ArrayList<>(fields); java.util.Collections.swap(ordered, 0, 1); refuse(ordered, "header or field order differs");
        var header = new ArrayList<>(fields); header.set(0, "HELPER_BINDINGS_V2"); refuse(header, "header or field order differs");
    }

    @Test void duplicate_source_class_origin_and_jdk_roles_cannot_survive_a_canonical_roundtrip() throws Exception {
        var h = helper();
        var source = new HelperManifest(h.schemaVersion(), h.runId(), h.inputSha256(), h.buildId(), h.rootDefinitionId(),
                h.rootFormulaSha256(), h.producerFormulaSha256(), h.failsafeXml(), h.failsafeClasspath(),
                List.of(h.sources().getFirst(), h.sources().getFirst()), h.classes(), h.origins());
        var file = file(BenchmarkReturnWorkBudgetBindingsIo.helperBytes(source));
        assertThatThrownBy(() -> BenchmarkReturnBindingsManifestReader.readHelper(file.path(), file.blob()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("source role repeated");
        var compiled = new HelperManifest(h.schemaVersion(), h.runId(), h.inputSha256(), h.buildId(), h.rootDefinitionId(),
                h.rootFormulaSha256(), h.producerFormulaSha256(), h.failsafeXml(), h.failsafeClasspath(), h.sources(),
                List.of(h.classes().getFirst(), h.classes().getFirst()), h.origins());
        var repeatedClass = file(BenchmarkReturnWorkBudgetBindingsIo.helperBytes(compiled));
        assertThatThrownBy(() -> BenchmarkReturnBindingsManifestReader.readHelper(repeatedClass.path(), repeatedClass.blob()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("compiled member repeated");
        var origin = new HelperManifest(h.schemaVersion(), h.runId(), h.inputSha256(), h.buildId(), h.rootDefinitionId(),
                h.rootFormulaSha256(), h.producerFormulaSha256(), h.failsafeXml(), h.failsafeClasspath(), h.sources(), h.classes(),
                List.of(h.origins().getFirst(), h.origins().getFirst()));
        var repeatedOrigin = file(BenchmarkReturnWorkBudgetBindingsIo.helperBytes(origin));
        assertThatThrownBy(() -> BenchmarkReturnBindingsManifestReader.readHelper(repeatedOrigin.path(), repeatedOrigin.blob()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("owned origin repeated");
        var jdk = jdk(); var repeated = new JdkManifest(jdk.schemaVersion(), jdk.runId(), jdk.inputSha256(), jdk.javaHome(),
                jdk.javaVersion(), List.of(jdk.files().getFirst(), jdk.files().getFirst()));
        var repeatedJdk = file(BenchmarkReturnWorkBudgetBindingsIo.jdkBytes(repeated));
        assertThatThrownBy(() -> BenchmarkReturnBindingsManifestReader.readJdk(repeatedJdk.path(), repeatedJdk.blob()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("JDK role repeated");
    }

    @Test void whole_file_prior_cap_hash_and_link_guards_do_not_use_an_observed_size_as_their_limit() throws Exception {
        var raw = BenchmarkReturnWorkBudgetBindingsIo.helperBytes(helper()); var file = file(raw); var opens = new AtomicInteger();
        var excessive = new Blob(file.blob().sha256(), MAX_MANIFEST_BYTES + 1);
        assertThatThrownBy(() -> BenchmarkReturnBindingsManifestReader.readHelper(file.path(), excessive, path -> {
            opens.incrementAndGet(); return Files.newInputStream(path);
        })).isInstanceOf(AssertionError.class).hasMessageContaining("prior manifest cap");
        assertThat(opens).hasValue(0);
        assertThatThrownBy(() -> BenchmarkReturnBindingsManifestReader.readHelper(file.path(), new Blob("b".repeat(64), raw.length)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("actual manifest digest");
        Path linked = file.path().resolveSibling("owned-link"); Files.createSymbolicLink(linked, file.path().getFileName());
        assertThatThrownBy(() -> BenchmarkReturnBindingsManifestReader.readHelper(linked, file.blob()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("linked manifest refused");
    }

    @Test void actual_read_consume_hash_close_and_decoder_entry_are_retained_under_the_captured_scope() throws Exception {
        var file = file(BenchmarkReturnWorkBudgetBindingsIo.helperBytes(helper()));
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "controlled-reader")) {
            var decoded = BenchmarkReturnBindingsManifestReader.readHelper(file.path(), file.blob());
            assertThat(decoded).isEqualTo(helper()); var receipt = scope.snapshot();
            assertThat(actual(receipt, "DOCUMENT_READ", "bytes")).isEqualTo(file.blob().bytes());
            assertThat(actual(receipt, "INPUT_FINGERPRINT", "bytes")).isEqualTo(file.blob().bytes());
            assertThat(actual(receipt, "COPY", "requests")).isEqualTo(1);
            assertThat(actual(receipt, "DECODE", "bytes")).isEqualTo(file.blob().bytes());
            assertThat(receipt).containsEntry("pendingOperations", 0L).containsEntry("wholeMethodCostQualified", false);
        }
    }

    @Test void read_close_and_zero_progress_refusals_preserve_primary_and_leave_no_pending_reader() throws Exception {
        var file = file(BenchmarkReturnWorkBudgetBindingsIo.helperBytes(helper()));
        var read = new IOException("controlled read failure"); var close = new IOException("controlled close failure");
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "controlled-reader-failure")) {
            Throwable failure = catchThrowable(() -> BenchmarkReturnBindingsManifestReader.readHelper(file.path(), file.blob(), path ->
                    new FilterInputStream(Files.newInputStream(path)) {
                        @Override public int read(byte[] data, int at, int length) throws IOException { throw read; }
                        @Override public void close() throws IOException { super.close(); throw close; }
                    }));
            assertThat(failure).isSameAs(read); assertThat(failure.getSuppressed()).containsExactly(close);
            assertThat(scope.primaryCause()).isSameAs(read); assertThat(scope.snapshot()).containsEntry("pendingOperations", 0L);
        }
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "controlled-reader-zero")) {
            Throwable failure = catchThrowable(() -> BenchmarkReturnBindingsManifestReader.readHelper(file.path(), file.blob(), path ->
                    new FilterInputStream(Files.newInputStream(path)) { @Override public int read(byte[] data, int at, int length) { return 0; } }));
            assertThat(failure).isInstanceOf(AssertionError.class).hasMessageContaining("no progress");
            assertThat(scope.primaryCause()).isSameAs(failure); assertThat(scope.snapshot()).containsEntry("pendingOperations", 0L);
        }
    }

    @Test void numeric_source_upper_covers_emitted_character_api_work_as_well_as_field_rosters() {
        var base = helper(); String longPath = "/" + "x".repeat(4095);
        var h = new HelperManifest(base.schemaVersion(), base.runId(), base.inputSha256(), base.buildId(), base.rootDefinitionId(),
                base.rootFormulaSha256(), base.producerFormulaSha256(), base.failsafeXml(), List.of(longPath), base.sources(), base.classes(), base.origins());
        byte[] encoded = BenchmarkReturnWorkBudgetBindingsIo.helperBytes(h); var fields = fields(encoded);
        long characters = fields.stream().mapToLong(String::length).sum();
        long explicitEmittedLengthAndCharAtLower = 2 * characters + 4L * fields.size();
        var upper = BenchmarkReturnBindingsManifestReader.encodingWorkUpper(encoded.length, fields.size());
        assertThat(upper.requests()).isGreaterThanOrEqualTo(explicitEmittedLengthAndCharAtLower).isGreaterThan(upper.elements());
        var decoding = BenchmarkReturnBindingsManifestReader.decodingWorkUpper(encoded.length, fields.size());
        assertThat(decoding.requests()).isGreaterThan(upper.requests());
        assertThatThrownBy(() -> BenchmarkReturnBindingsManifestReader.encodingWorkUpper(Long.MAX_VALUE, fields.size()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("source work caps");
    }

    private void refuse(List<String> fields, String reason) throws Exception {
        var file = file(frame(fields));
        assertThatThrownBy(() -> BenchmarkReturnBindingsManifestReader.readHelper(file.path(), file.blob()))
                .isInstanceOf(AssertionError.class).hasMessageContaining(reason);
    }
    private File file(byte[] bytes) throws Exception {
        Path path = Files.createTempFile(temporary, "manifest-", ".data").toRealPath(); Files.write(path, bytes);
        return new File(path, new Blob(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)), bytes.length));
    }
    private static byte[] join(byte[] raw, String suffix) {
        byte[] tail = suffix.getBytes(StandardCharsets.US_ASCII), out = Arrays.copyOf(raw, raw.length + tail.length);
        System.arraycopy(tail, 0, out, raw.length, tail.length); return out;
    }
    private static List<String> fields(byte[] raw) {
        var fields = new ArrayList<String>();
        for (String line : new String(raw, StandardCharsets.US_ASCII).split("\n")) { fields.add(line.substring(line.indexOf(':') + 1)); }
        return fields;
    }
    private static byte[] frame(List<String> fields) {
        var raw = new StringBuilder(); for (String value : fields) { raw.append(value.length()).append(':').append(value).append('\n'); }
        return raw.toString().getBytes(StandardCharsets.US_ASCII);
    }
    private static HelperManifest helper() {
        String sha = "a".repeat(64), binary = SourceRole.ROOT_BUDGET.binaryName(); Blob blob = new Blob(sha, 17);
        return new HelperManifest(1, "controlled-reader", sha, "controlled-build", "controlled-definition", sha,
                BenchmarkReturnWorkBudgetProducerAllocations.FORMULA_SHA256, blob, List.of("/controlled/classes"),
                List.of(new Source(SourceRole.ROOT_BUDGET, SourceRole.ROOT_BUDGET.sourcePath(), blob)),
                List.of(new CompiledClass(SourceRole.ROOT_BUDGET, binary, blob)),
                List.of(new Origin(Owner.ROOT, SourceRole.ROOT_BUDGET, binary, "/controlled/classes", binary.replace('.', '/') + ".class", blob, null)));
    }
    private static HelperManifest separateWriterHelper() {
        var h = helper(); var root = new CompiledClass(SourceRole.PDK_WRITER, SourceRole.PDK_WRITER.binaryName(), new Blob("b".repeat(64), 23));
        var writer = new ProducerWriterBinding(Owner.PRODUCER, "/controlled/producer-a",
                new Source(SourceRole.PDK_WRITER, SourceRole.PDK_WRITER.sourcePath(), new Blob("c".repeat(64), 41)),
                new CompiledClass(SourceRole.PDK_WRITER, SourceRole.PDK_WRITER.binaryName(), new Blob("d".repeat(64), 31)));
        var sources = new ArrayList<>(h.sources()); sources.add(new Source(SourceRole.PDK_WRITER, SourceRole.PDK_WRITER.sourcePath(), new Blob("e".repeat(64), 37)));
        var classes = new ArrayList<>(h.classes()); classes.add(root); var origins = new ArrayList<>(h.origins());
        String entry = root.binaryName().replace('.', '/') + ".class";
        origins.add(new Origin(Owner.ROOT, SourceRole.PDK_WRITER, root.binaryName(), "/controlled/classes", entry, root.blob(), null));
        origins.add(new Origin(Owner.PRODUCER, SourceRole.PDK_WRITER, root.binaryName(), "/controlled/app.jar",
                "BOOT-INF/lib/adapter-pdk-control.jar!/" + entry, writer.compiled().blob(), "f".repeat(64)));
        return new HelperManifest(PRODUCER_WRITER_SCHEMA_VERSION, h.runId(), h.inputSha256(), h.buildId(), h.rootDefinitionId(),
                h.rootFormulaSha256(), h.producerFormulaSha256(), h.failsafeXml(), h.failsafeClasspath(), sources, classes, origins, writer);
    }
    private static JdkManifest jdk() {
        String home = "/controlled/jdk"; var files = new ArrayList<JdkFile>();
        for (var role : List.of(JdkRole.JAVA, JdkRole.JVM, JdkRole.MODULES, JdkRole.RELEASE)) {
            files.add(new JdkFile(role, home + "/" + role.relativePath(), new Blob("b".repeat(64), 31)));
        }
        return new JdkManifest(1, "controlled-reader", "a".repeat(64), home, "21.0.9", files);
    }
    private static BenchmarkReturnWorkBudget.Definition definition() {
        var limits = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        for (var activity : BenchmarkReturnWorkBudget.Activity.values()) { limits.put(activity,
                new BenchmarkReturnWorkBudget.Work(1_000_000, 1_000_000, 1_000_000, 1_000_000, 1_000_000_000, 1_000_000, 1_000_000, 1_000_000)); }
        return new BenchmarkReturnWorkBudget.Definition("controlled-reader", "a".repeat(64), Map.of(), limits, Set.of());
    }
    private static long actual(Map<String, Object> receipt, String activity, String unit) {
        var row = (Map<?, ?>) ((Map<?, ?>) receipt.get("activities")).get(activity);
        return ((Number) ((Map<?, ?>) row.get("actual")).get(unit)).longValue();
    }
    private record File(Path path, Blob blob) { }
}
