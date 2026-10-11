package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkReturnWorkBudgetProducerSealTest {
    private static final BenchmarkReturnWorkBudgetProducerSeal.Registration REGISTERED =
            new BenchmarkReturnWorkBudgetProducerSeal.Registration("controlled-run",
                    new BenchmarkCausalClock.Identity(17, 23), "b".repeat(64), "controlled-nonce", "d".repeat(64));

    @Test void body_and_companion_binding_is_immutable_without_claiming_process_exit(@TempDir Path directory) throws Exception {
        var fixture = fixture(directory); var bound = verify(fixture);
        assertThat(bound.body()).containsEntry("stage", "SHUTDOWN_HOOK").containsEntry("processExitObserved", false);
        assertThat(bound.generation()).isEqualTo(9); assertThat(bound.mappingForceAttempts()).isEqualTo(2);
        assertThat(bound.bodySha256()).isEqualTo(hash(Files.readAllBytes(fixture.body())));
        assertThatThrownBy(() -> bound.body().put("state", "QUALIFIED")).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void incomplete_status_identity_late_force_close_and_reserved_bytes_all_refuse(@TempDir Path directory) throws Exception {
        int[] offsets = {12, 16, 40, 112, 184, 192, 200, 208, 216, 224};
        for (int i = 0; i < offsets.length; i++) {
            Path child = Files.createDirectory(directory.resolve("case" + i)); var fixture = fixture(child);
            byte[] bytes = Files.readAllBytes(fixture.companion());
            bytes[offsets[i]] ^= 1; Files.write(fixture.companion(), bytes);
            assertThatThrownBy(() -> verify(fixture)).as("companion offset %s", offsets[i]).isInstanceOf(AssertionError.class);
        }
    }

    @Test void a_changed_body_cannot_reuse_the_old_companion_hash(@TempDir Path directory) throws Exception {
        var fixture = fixture(directory); byte[] body = Files.readAllBytes(fixture.body());
        body[body.length - 1] = ' '; Files.write(fixture.body(), body);
        assertThatThrownBy(() -> verify(fixture)).isInstanceOf(AssertionError.class).hasMessageContaining("content binding");
    }

    @Test void rehashed_wrong_run_nonce_stage_and_open_lifecycle_still_refuse(@TempDir Path directory) throws Exception {
        List<Map<String, Object>> replacements = List.of(Map.of("runId", "foreign"), Map.of("nonceSha256", "c".repeat(64)),
                Map.of("stage", "AFTER_PROCESS_EXIT"), Map.of("pending", 1L), Map.of("openWriters", 1L),
                Map.of("gaps", List.of("UNACCOUNTED_WORK")), Map.of("formalPerformance", true));
        for (int i = 0; i < replacements.size(); i++) {
            Path child = Files.createDirectory(directory.resolve("case" + i)); var fixture = fixture(child);
            var body = body(); body.putAll(replacements.get(i)); rewrite(fixture, JsonWriter.write(body).getBytes(StandardCharsets.US_ASCII));
            assertThatThrownBy(() -> verify(fixture)).as("body mutation %s", replacements.get(i)).isInstanceOf(AssertionError.class);
        }
    }

    @Test void oversized_linked_missing_and_deep_json_refuse_before_unbounded_consumption(@TempDir Path directory) throws Exception {
        var fixture = fixture(directory); byte[] oversized = new byte[65537]; java.util.Arrays.fill(oversized, (byte) 'a');
        Files.write(fixture.body(), oversized);
        assertThatThrownBy(() -> verify(fixture)).isInstanceOf(AssertionError.class).hasMessageContaining("prior byte bound");
        rewrite(fixture, ("[".repeat(13) + "0" + "]".repeat(13)).getBytes(StandardCharsets.US_ASCII));
        assertThatThrownBy(() -> verify(fixture)).isInstanceOf(AssertionError.class).hasMessageContaining("prior JSON depth");
        Path linked = directory.toRealPath().resolve("linked.json"); Files.createSymbolicLink(linked, fixture.body());
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProducerSeal.verifyFiles(linked.toAbsolutePath(), REGISTERED))
                .isInstanceOf(AssertionError.class).hasMessageContaining("missing or linked");
        Files.delete(fixture.companion());
        assertThatThrownBy(() -> verify(fixture)).isInstanceOf(AssertionError.class).hasMessageContaining("missing or linked");
    }

    @Test void a_missing_pid_or_qualified_label_cannot_replace_the_original_process_outcome() {
        Map<String, Object> exited = exit();
        BenchmarkReturnWorkBudgetProducerSeal.requireExited(exited, REGISTERED);
        for (var mutation : List.<Map<String, Object>>of(Map.of("ownedAliveAfterClose", true), Map.of("ownedExitCode", 137),
                Map.of("exitEvidenceSource", "PID_NOT_FOUND"), Map.of("reasons", List.of("ACTUAL_EXIT_UNKNOWN")),
                Map.of("applicationSha256", "e".repeat(64)),
                Map.of("childJvmRuntime", List.of(Map.of("actualPid", 17L, "jvmStartTimeMillis", 24L))))) {
            var wrong = new LinkedHashMap<>(exited); wrong.putAll(mutation);
            assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProducerSeal.requireExited(wrong, REGISTERED)).isInstanceOf(AssertionError.class);
        }
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetProducerSeal.requireExited(Map.of("status", "QUALIFIED"), REGISTERED))
                .isInstanceOf(AssertionError.class);
    }

    @Test void numeric_and_string_tokens_are_bounded_before_the_general_json_parser(@TempDir Path directory) throws Exception {
        var fixture = fixture(directory);
        for (String raw : List.of("{\"x\":1e999999999}", "{\"x\":" + "9".repeat(21) + "}",
                "{\"x\":\"" + "x".repeat(6145) + "\"}")) {
            rewrite(fixture, raw.getBytes(StandardCharsets.US_ASCII));
            assertThatThrownBy(() -> verify(fixture)).isInstanceOf(AssertionError.class)
                    .hasMessageContaining("token");
        }
    }

    @Test void rehashed_missing_or_failed_activity_cannot_be_hidden_by_a_clean_prefix(@TempDir Path directory) throws Exception {
        var fixture = fixture(directory); var body = body();
        @SuppressWarnings("unchecked") var activities = new LinkedHashMap<>((Map<String, Object>) body.get("activities"));
        activities.put("CLOCK_READ", Map.of("invocations", 1L, "completed", 0L, "failed", 1L, "pending", 0L));
        body.put("activities", activities); rewrite(fixture, JsonWriter.write(body).getBytes(StandardCharsets.US_ASCII));
        assertThatThrownBy(() -> verify(fixture)).isInstanceOf(AssertionError.class).hasMessageContaining("failed or pending");
        activities.remove("CLOCK_READ"); rewrite(fixture, JsonWriter.write(body).getBytes(StandardCharsets.US_ASCII));
        assertThatThrownBy(() -> verify(fixture)).isInstanceOf(AssertionError.class).hasMessageContaining("activity roster");
    }

    private record Fixture(Path body, Path companion) { }
    private static Fixture fixture(Path directory) throws Exception {
        Path body = directory.toRealPath().resolve("producer.json"), companion = Path.of(body + ".seal");
        byte[] raw = JsonWriter.write(body()).getBytes(StandardCharsets.US_ASCII);
        var seal = ByteBuffer.allocate(256).order(ByteOrder.BIG_ENDIAN);
        seal.putLong(0, 0x5752425345414c31L); seal.putInt(8, 1); seal.putInt(12, 1);
        seal.putLong(16, 17); seal.putLong(24, 23); seal.putLong(32, raw.length);
        seal.put(48, digest(REGISTERED.nonce().getBytes(StandardCharsets.US_ASCII)));
        seal.put(80, digest(raw)); seal.put(112, HexFormat.of().parseHex(BenchmarkReturnWorkBudgetProducerSeal.FORMULA_SHA256));
        seal.put(144, HexFormat.of().parseHex(REGISTERED.inputSha256())); seal.putLong(176, 9);
        seal.putLong(184, 2); seal.putLong(200, 1); seal.putLong(208, 1); seal.putLong(216, 1);
        Files.write(body, raw); Files.write(companion, seal.array()); return new Fixture(body, companion);
    }

    private static LinkedHashMap<String, Object> body() throws Exception {
        var result = new LinkedHashMap<String, Object>();
        result.put("schemaVersion", 1); result.put("enabled", true); result.put("definitionId", "WRITE_RETURN_PRODUCER_WORK_V1");
        result.put("sourceFormulaSha256", BenchmarkReturnWorkBudgetProducerSeal.FORMULA_SHA256);
        result.put("inputSha256", REGISTERED.inputSha256()); result.put("runId", REGISTERED.runId());
        result.put("nonceSha256", hash(REGISTERED.nonce().getBytes(StandardCharsets.US_ASCII)));
        result.put("pid", 17L); result.put("jvmStartTimeMillis", 23L); result.put("generation", 9L);
        result.put("stage", "SHUTDOWN_HOOK"); result.put("lifecycle", "SHUTDOWN_HOOK");
        result.put("state", "RECORDED_STRUCTURAL_DIAGNOSTIC"); result.put("processExitObserved", false);
        result.put("sealTailIncludedInSourceUpper", true); result.put("actualSealTailInCompanion", true);
        result.put("sourceUpperIsActualWork", false); result.put("limitsIndependentlyVerified", false);
        result.put("crashDurabilityQualified", false);
        for (String key : List.of("pending", "openWriters", "omitted", "lateMutations")) { result.put(key, 0L); }
        result.put("overflow", false); result.put("overBudget", false); result.put("gaps", List.of());
        var activities = new LinkedHashMap<String, Object>();
        for (String name : BenchmarkReturnWorkBudgetProducerSeal.ACTIVITY_NAMES) {
            activities.put(name, Map.of("invocations", 0L, "completed", 0L, "failed", 0L, "pending", 0L));
        }
        result.put("activities", activities);
        for (String flag : List.of("structuralBudgetQualified", "accountingErrorBoundQualified", "wholeMethodCostQualified",
                "collectionCostUpperBoundQualified", "samplingCostQualified", "causalOverheadQualified",
                "costAcceptanceEligible", "performanceAcceptanceEligible", "formalPerformance")) { result.put(flag, false); }
        return result;
    }

    private static Map<String, Object> exit() {
        return Map.of("finishedAfterCloseAt", "2026-10-11T00:00:00Z", "exitEvidenceSource", "ORIGINAL_OWNED_PROCESS_OBJECT",
                "ownedAliveAfterClose", false, "ownedExitCode", 0, "reasons", List.of(), "productAtCapture", Map.of("pid", 17L),
                "childJvmRuntime", List.of(Map.of("actualPid", 17L, "jvmStartTimeMillis", 23L)),
                "applicationSha256", REGISTERED.applicationSha256());
    }
    private static BenchmarkReturnWorkBudgetProducerSeal.Bound verify(Fixture fixture) throws Exception {
        return BenchmarkReturnWorkBudgetProducerSeal.verifyFiles(fixture.body(), REGISTERED);
    }
    private static void rewrite(Fixture fixture, byte[] body) throws Exception {
        Files.write(fixture.body(), body); byte[] bytes = Files.readAllBytes(fixture.companion());
        var seal = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN); seal.putLong(32, body.length); seal.put(80, digest(body));
        Files.write(fixture.companion(), bytes);
    }
    private static byte[] digest(byte[] bytes) throws Exception { return MessageDigest.getInstance("SHA-256").digest(bytes); }
    private static String hash(byte[] bytes) throws Exception { return HexFormat.of().formatHex(digest(bytes)); }
}
