package io.tapstate.e2e;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static io.tapstate.e2e.BenchmarkReturnWorkBudgetBindings.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

class BenchmarkReturnEmittedRosterTest {
    @TempDir Path temporary;

    @Test void actual_source_files_and_outer_and_inner_class_bytes_form_an_immutable_compiler_snapshot() throws Exception {
        var fixture = fixture();
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "emitted-control")) {
            var observed = BenchmarkReturnEmittedRoster.observe(fixture.root(), fixture.helper());
            assertThat(observed.sources()).containsExactlyInAnyOrderElementsOf(fixture.helper().sources());
            assertThat(observed.classes()).containsExactlyInAnyOrderElementsOf(fixture.helper().classes());
            assertThat(observed.members()).anySatisfy(member -> {
                assertThat(member.binaryName()).isEqualTo(SourceRole.ROOT_BUDGET.binaryName() + "$Work");
                assertThat(member.role()).isEqualTo(SourceRole.ROOT_BUDGET);
                assertThat(member.sourceFile()).isEqualTo("BenchmarkReturnWorkBudget.java");
                assertThat(member.compilerContainer()).isEqualTo(fixture.root().resolve("e2e/target/test-classes").toString());
            });
            assertThat(observed.evidence()).containsEntry("state", "COMPILER_SOURCE_ROSTER_OBSERVED")
                    .containsEntry("sourceCoverageQualified", false).containsEntry("emittedMemberCompletenessQualified", false)
                    .containsEntry("loadedRuntimeOriginsQualified", false).containsEntry("runtimeInstructionBytesQualified", false)
                    .containsEntry("wholeMethodCostQualified", false).containsEntry("formalPerformance", false);
            assertThat(io.tapstate.core.common.JsonWriter.write(observed.evidence())).contains("selectedMembers");
            assertThatThrownBy(observed.members()::clear).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(observed.evidence()::clear).isInstanceOf(UnsupportedOperationException.class);
            var row = (Map<?, ?>) ((List<?>) observed.evidence().get("sources")).getFirst();
            assertThatThrownBy(row::clear).isInstanceOf(UnsupportedOperationException.class);
            assertThat(scope.snapshot()).containsEntry("pendingOperations", 0L);
            assertThat(actual(scope.snapshot(), "CLOCK_REQUEST", "requests")).isZero();
            long consumed = ((Number) observed.evidence().get("consumedBytes")).longValue();
            assertThat(actual(scope.snapshot(), "DOCUMENT_READ", "bytes")).isEqualTo(consumed);
            assertThat(actual(scope.snapshot(), "INPUT_FINGERPRINT", "bytes")).isEqualTo(consumed);
            assertThat(row(scope.snapshot(), "INSTALL_BINDINGS")).containsEntry("attempts", 1L).containsEntry("completedAttempts", 1L);
            assertThat(((Number) observed.evidence().get("serviceRequests")).longValue())
                    .isLessThanOrEqualTo(BenchmarkReturnEmittedRoster.serviceRequestUpper());
        }
    }

    @Test void jointly_omitted_inner_registration_does_not_remove_the_actual_member_from_observation() throws Exception {
        var fixture = fixture(); String inner = SourceRole.ROOT_BUDGET.binaryName() + "$Work";
        var declared = fixture.withClasses(fixture.helper().classes().stream().filter(c -> !c.binaryName().equals(inner)).toList());
        var refusal = refusal(() -> BenchmarkReturnEmittedRoster.observe(fixture.root(), declared));
        assertThat(refusal.getCause()).hasMessageContaining("complete compiler-emitted role binary or digest set differs");
        assertThat((List<?>) refusal.retainedEvidence().get("selectedMembers")).anySatisfy(value -> {
            assertThat(((Map<?, ?>) value).get("binaryName")).isEqualTo(inner);
        });
        assertThat(refusal.retainedEvidence()).containsEntry("state", "UNKNOWN").containsEntry("formalPerformance", false);
    }

    @Test void jointly_omitted_source_and_all_its_declared_members_refuse_before_opening_compiler_outputs() throws Exception {
        var fixture = fixture(); var h = fixture.helper();
        var missing = helper(h, h.sources().stream().filter(s -> s.role() != SourceRole.PDK_WRITER).toList(),
                h.classes().stream().filter(c -> c.role() != SourceRole.PDK_WRITER).toList());
        var walks = new AtomicInteger();
        var refusal = refusal(() -> BenchmarkReturnEmittedRoster.observe(fixture.root(), missing, Files::newInputStream,
                path -> { walks.incrementAndGet(); return Files.walk(path); }));
        assertThat(refusal.getCause()).hasMessageContaining("complete named Java source roster missing");
        assertThat(walks).hasValue(0);
    }

    @Test void a_changed_declared_digest_or_actual_source_digest_cannot_grant_roster_credit() throws Exception {
        var fixture = fixture(); var h = fixture.helper(); var first = h.classes().getFirst();
        var classes = new ArrayList<>(h.classes()); classes.set(0,
                new CompiledClass(first.role(), first.binaryName(), new Blob("f".repeat(64), first.blob().bytes())));
        var refusal = refusal(() -> BenchmarkReturnEmittedRoster.observe(fixture.root(), fixture.withClasses(classes)));
        assertThat(refusal.getCause()).hasMessageContaining("complete compiler-emitted role binary or digest set differs");
        Path source = fixture.root().resolve(h.sources().getFirst().path()); byte[] changed = Files.readAllBytes(source);
        changed[0] = (byte) 'X'; Files.write(source, changed);
        refusal = refusal(() -> BenchmarkReturnEmittedRoster.observe(fixture.root(), h));
        assertThat(refusal.getCause()).hasMessageContaining("actual source digest differs");
        assertThat((List<?>) refusal.retainedEvidence().get("selectedMembers")).isEmpty();
    }

    @Test void a_rehashed_class_with_a_different_sourcefile_is_rejected_by_binary_owner_independently_of_digest() throws Exception {
        var fixture = fixture(); String binary = SourceRole.ROOT_BUDGET.binaryName(); Path file = fixture.classFile(SourceRole.ROOT_BUDGET, binary);
        byte[] raw = Files.readAllBytes(file), token = "BenchmarkReturnWorkBudget.java".getBytes(StandardCharsets.US_ASCII);
        int found = indexOf(raw, token); assertThat(found).isGreaterThanOrEqualTo(0);
        raw[found] = (byte) 'X'; Files.write(file, raw);
        var classes = fixture.helper().classes().stream().map(c -> c.binaryName().equals(binary)
                ? new CompiledClass(c.role(), c.binaryName(), blob(raw)) : c).toList();
        var refusal = refusal(() -> BenchmarkReturnEmittedRoster.observe(fixture.root(), fixture.withClasses(classes)));
        assertThat(refusal.getCause()).hasMessageContaining("SourceFile and binary source owner differ");
    }

    @Test void prior_file_size_and_canonical_link_guards_run_before_opening_the_target_stream() throws Exception {
        var fixture = fixture(); Path target = fixture.classFile(SourceRole.ROOT_BUDGET, SourceRole.ROOT_BUDGET.binaryName());
        byte[] original = Files.readAllBytes(target); Files.write(target, new byte[BenchmarkReturnEmittedRoster.MEMBER_BYTES + 1]);
        var opens = new AtomicInteger();
        BenchmarkReturnEmittedRoster.Streams streams = path -> {
            if (path.equals(target)) opens.incrementAndGet(); return Files.newInputStream(path);
        };
        var refusal = refusal(() -> BenchmarkReturnEmittedRoster.observe(fixture.root(), fixture.helper(), streams, Files::walk));
        assertThat(refusal.getCause()).hasMessageContaining("exceeds prior member cap"); assertThat(opens).hasValue(0);
        Files.delete(target); Path foreign = temporary.resolve("linked-class-bytes"); Files.write(foreign, original);
        Files.createSymbolicLink(target, foreign);
        refusal = refusal(() -> BenchmarkReturnEmittedRoster.observe(fixture.root(), fixture.helper(), streams, Files::walk));
        assertThat(refusal.getCause()).hasMessageContaining("linked compiler or source entry refused"); assertThat(opens).hasValue(0);
    }

    @Test void truncated_or_extra_class_bytes_refuse_even_when_their_new_sha_is_registered() throws Exception {
        for (boolean truncated : List.of(false, true)) {
            var fixture = fixture(); String binary = SourceRole.ROOT_BUDGET.binaryName(); Path file = fixture.classFile(SourceRole.ROOT_BUDGET, binary);
            byte[] original = Files.readAllBytes(file); byte[] raw = java.util.Arrays.copyOf(original, truncated ? 9 : original.length + 1);
            Files.write(file, raw);
            var classes = fixture.helper().classes().stream().map(c -> c.binaryName().equals(binary)
                    ? new CompiledClass(c.role(), c.binaryName(), blob(raw)) : c).toList();
            var refusal = refusal(() -> BenchmarkReturnEmittedRoster.observe(fixture.root(), fixture.withClasses(classes)));
            assertThat(refusal.getCause()).isInstanceOf(AssertionError.class)
                    .hasMessageContaining(truncated ? "class field is truncated" : "class has trailing bytes");
        }
    }

    @Test void read_and_close_failures_retain_the_actual_prefix_primary_and_suppressed_with_no_pending_or_clock_requests() throws Exception {
        var fixture = fixture(); Path target = fixture.classFile(SourceRole.JSON_READER, SourceRole.JSON_READER.binaryName());
        var primary = new IOException("controlled emitted read"); var cleanup = new IOException("controlled emitted close");
        var opens = new AtomicInteger();
        BenchmarkReturnEmittedRoster.Streams streams = path -> {
            if (!path.equals(target)) return Files.newInputStream(path);
            opens.incrementAndGet(); return new InputStream() {
                @Override public int read() throws IOException { throw primary; }
                @Override public int read(byte[] bytes, int offset, int length) throws IOException { throw primary; }
                @Override public void close() throws IOException { throw cleanup; }
            };
        };
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "emitted-control")) {
            var refusal = refusal(() -> BenchmarkReturnEmittedRoster.observe(fixture.root(), fixture.helper(), streams, Files::walk));
            assertThat(refusal.getCause()).isSameAs(primary); assertThat(primary.getSuppressed()).containsExactly(cleanup);
            assertThat(scope.primaryCause()).isSameAs(primary); assertThat(opens).hasValue(1);
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN").containsEntry("pendingOperations", 0L);
            assertThat(actual(scope.snapshot(), "CLOCK_REQUEST", "requests")).isZero();
            assertThat((List<?>) refusal.retainedEvidence().get("sources")).hasSize(fixture.helper().sources().size());
            assertThat((List<?>) refusal.retainedEvidence().get("selectedMembers")).isNotEmpty();
            assertThat(row(scope.snapshot(), "INSTALL_BINDINGS")).containsEntry("failedAttempts", 1L);
        }
    }

    @Test void actual_walk_close_failure_does_not_return_an_observation_or_erase_already_read_members() throws Exception {
        var fixture = fixture(); var cleanup = new IllegalStateException("controlled traversal close");
        var closed = new AtomicInteger();
        var refusal = refusal(() -> BenchmarkReturnEmittedRoster.observe(fixture.root(), fixture.helper(), Files::newInputStream,
                path -> Files.walk(path).onClose(() -> { closed.incrementAndGet(); throw cleanup; })));
        assertThat(refusal.getCause()).isSameAs(cleanup); assertThat(closed).hasValue(1);
        assertThat((List<?>) refusal.retainedEvidence().get("selectedMembers")).isNotEmpty();
    }

    @Test void an_infinite_controlled_walk_stops_at_the_prior_visit_sentinel_without_reading_a_class() throws Exception {
        var fixture = fixture(); var emitted = new AtomicInteger();
        var refusal = refusal(() -> BenchmarkReturnEmittedRoster.observe(fixture.root(), fixture.helper(), Files::newInputStream,
                output -> Stream.generate(() -> { emitted.incrementAndGet(); return output; })));
        assertThat(refusal.getCause()).hasMessageContaining("directory visits exceed prior cap");
        assertThat(emitted).hasValue(BenchmarkReturnEmittedRoster.DIRECTORY_VISITS + 1);
        assertThat(refusal.retainedEvidence()).containsEntry("directoryVisits", (long) emitted.get());
        assertThat((List<?>) refusal.retainedEvidence().get("selectedMembers")).isEmpty();
        assertThat(BenchmarkReturnEmittedRoster.localUpper().requests()).isGreaterThan(BenchmarkReturnEmittedRoster.serviceRequestUpper());
        assertThatThrownBy(() -> BenchmarkReturnEmittedRoster.localUpper().times(Long.MAX_VALUE)).isInstanceOf(ArithmeticException.class);
    }

    private Fixture fixture() throws Exception {
        Path root = Files.createTempDirectory(temporary, "owned-emitted-").toRealPath();
        var sources = new ArrayList<Source>(); var classes = new ArrayList<CompiledClass>();
        for (var role : SourceRole.values()) if (role != SourceRole.JNI_SOURCE) {
            // These source materials test byte registration, without claiming a compiler ran on the fixture.
            byte[] text = ("// Controlled source bytes for " + role.name() + "\n").getBytes(StandardCharsets.US_ASCII);
            Path source = root.resolve(role.sourcePath()); Files.createDirectories(source.getParent()); Files.write(source, text);
            sources.add(new Source(role, role.sourcePath(), blob(text)));
            addClass(root, classes, role, role.binaryName());
        }
        addClass(root, classes, SourceRole.ROOT_BUDGET, SourceRole.ROOT_BUDGET.binaryName() + "$Work");
        addClass(root, classes, SourceRole.JSON_READER, SourceRole.JSON_READER.binaryName() + "$Parser");
        var helper = new HelperManifest(1, "emitted-control", "a".repeat(64), "controlled-build", "controlled-definition",
                "b".repeat(64), "c".repeat(64), new Blob("d".repeat(64), 1), List.of(), sources, classes, List.of());
        return new Fixture(root, helper);
    }
    private static void addClass(Path root, List<CompiledClass> classes, SourceRole role, String binary) throws Exception {
        String entry = binary.replace('.', '/') + ".class"; byte[] bytes;
        try (InputStream input = BenchmarkReturnEmittedRosterTest.class.getClassLoader().getResourceAsStream(entry)) {
            assertThat(input).as("real compiled fixture resource %s", binary).isNotNull();
            bytes = input.readNBytes(BenchmarkReturnEmittedRoster.MEMBER_BYTES + 1);
        }
        assertThat(bytes.length).isBetween(1, BenchmarkReturnEmittedRoster.MEMBER_BYTES);
        Path file = output(root, role).resolve(entry); Files.createDirectories(file.getParent()); Files.write(file, bytes);
        classes.add(new CompiledClass(role, binary, blob(bytes)));
    }
    private static Path output(Path root, SourceRole role) {
        return root.resolve(switch (role.module()) {
            case "e2e" -> "e2e/target/test-classes";
            case "adapter-pdk" -> "adapters/adapter-pdk/target/classes";
            case "core-common" -> "core/core-common/target/classes";
            case "test-support" -> "test-support/target/test-classes";
            default -> throw new AssertionError("controlled Java module differs");
        });
    }
    private static HelperManifest helper(HelperManifest h, List<Source> sources, List<CompiledClass> classes) {
        return new HelperManifest(h.schemaVersion(), h.runId(), h.inputSha256(), h.buildId(), h.rootDefinitionId(),
                h.rootFormulaSha256(), h.producerFormulaSha256(), h.failsafeXml(), h.failsafeClasspath(), sources, classes, h.origins());
    }
    private record Fixture(Path root, HelperManifest helper) {
        Path classFile(SourceRole role, String binary) { return output(root, role).resolve(binary.replace('.', '/') + ".class"); }
        HelperManifest withClasses(List<CompiledClass> classes) { return BenchmarkReturnEmittedRosterTest.helper(helper, helper.sources(), classes); }
    }
    private static int indexOf(byte[] raw, byte[] needle) {
        for (int at = 0; at <= raw.length - needle.length; at++) {
            int i = 0; while (i < needle.length && raw[at + i] == needle[i]) i++; if (i == needle.length) return at;
        }
        return -1;
    }
    private static Blob blob(byte[] bytes) {
        try { return new Blob(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)), bytes.length); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    private static BenchmarkReturnEmittedRoster.Refusal refusal(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        Throwable failure = catchThrowable(action); assertThat(failure).isInstanceOf(BenchmarkReturnEmittedRoster.Refusal.class);
        return (BenchmarkReturnEmittedRoster.Refusal) failure;
    }
    private static BenchmarkReturnWorkBudget.Definition definition() {
        var upper = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        for (var activity : BenchmarkReturnWorkBudget.Activity.values()) upper.put(activity,
                new BenchmarkReturnWorkBudget.Work(1_000_000, 1_000_000, 1_000_000, 1_000_000, 1_000_000_000,
                        1_000_000, 1_000_000, 1_000_000));
        return new BenchmarkReturnWorkBudget.Definition("controlled-emitted", "a".repeat(64), Map.of(), upper, Set.of());
    }
    private static long actual(Map<String, Object> receipt, String activity, String unit) {
        return ((Number) ((Map<?, ?>) row(receipt, activity).get("actual")).get(unit)).longValue();
    }
    @SuppressWarnings("unchecked") private static Map<String, Object> row(Map<String, Object> receipt, String activity) {
        return (Map<String, Object>) ((Map<?, ?>) receipt.get("activities")).get(activity);
    }
}
