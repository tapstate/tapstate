package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/** Binds a producer shutdown body to its companion after the original owned process has exited. */
final class BenchmarkReturnWorkBudgetProducerSeal {
    static final String FORMULA_SHA256 = "66db340eda98ee81382063031665532e1348d93ff0e5de90a67a39b674d9bada";
    static final int MAX_BODY_BYTES = 65536;
    static final int COMPANION_BYTES = 256;
    static final java.util.Set<String> ACTIVITY_NAMES = java.util.Set.of("CONFIG", "INSTALL", "WRITER_INSTALL", "WRITER_ROSTER",
            "WRITER_CLOSE_ATTEMPT", "WRITER_CLOSE", "WRITE_WRAPPER", "BEGIN", "ACTIVE_BEGIN", "INACTIVE_BEGIN", "REJECTION",
            "IDENTITY_ROW", "IDENTITY_PIECE", "KEY_SHAPE", "CALLBACK_WRAPPER", "CALLBACK", "CALLBACK_ERROR", "CALLBACK_EXIT",
            "RETURN_OBSERVATION", "COMPLETE", "RECEIPT_PIECE", "PUBLICATION", "COST_AGGREGATION", "CLOCK_READ",
            "CLOCK_INITIALIZATION", "CLOCK_METADATA", "SCOPE_INSTALL", "SCOPE_CACHE_ENTRY", "SCOPE_BEFORE", "SCOPE_AFTER",
            "TRANSACTION_WRAPPER", "HASH_READ", "HASH_MATERIAL", "ARGUMENT", "ARGUMENT_MATERIAL", "MBEAN_SCALAR", "MBEAN_START",
            "MBEAN_STOP", "MBEAN_PAGE", "PAGE_FRAME_VISIT", "MBEAN_COST", "MBEAN_BUDGET", "ARM", "SHUTDOWN", "FINAL_OWNER",
            "FINAL_WAIT_ITERATION", "BOOKKEEPING");
    private BenchmarkReturnWorkBudgetProducerSeal() { }

    record Registration(String runId, BenchmarkCausalClock.Identity runtime, String inputSha256, String nonce,
                        String applicationSha256) {
        Registration {
            require(runId != null && !runId.isBlank() && runId.length() <= 128 && runtime != null,
                    "registered runtime is missing");
            digest(inputSha256);
            digest(applicationSha256);
            require(nonce != null && !nonce.isEmpty() && nonce.length() <= 64
                            && nonce.chars().allMatch(c -> c >= 32 && c <= 126), "registered nonce is outside its bound");
        }
    }

    record Bound(Map<String, Object> body, String bodySha256, String companionSha256,
                 long bodyBytes, long generation, long mappingForceAttempts) {
        Bound { body = Map.copyOf(body); }
    }

    /** The typed receipt retains the original Process outcome; a missing PID alone never supplies exit evidence. */
    static Bound verifyAfterExit(Path bodyPath, Registration registration, BenchmarkOwnedProcessReceipt process) throws IOException {
        require(process != null, "original owned process receipt is missing");
        requireExited(process.evidence(), registration);
        return verifyFiles(bodyPath, registration);
    }

    static void requireExited(Map<String, Object> actual, Registration expected) {
        require(actual != null && expected != null && actual.get("finishedAfterCloseAt") instanceof String
                        && "ORIGINAL_OWNED_PROCESS_OBJECT".equals(actual.get("exitEvidenceSource"))
                        && Boolean.FALSE.equals(actual.get("ownedAliveAfterClose"))
                        && number(actual, "ownedExitCode") == 0
                        && actual.get("reasons") instanceof List<?> reasons && reasons.isEmpty(),
                "actual owned exit is unavailable or refused");
        var captured = object(actual.get("productAtCapture"));
        require(number(captured, "pid") == expected.runtime().pid(), "owned PID differs");
        require(expected.applicationSha256().equals(actual.get("applicationSha256")), "owned application bytes differ");
        require(actual.get("childJvmRuntime") instanceof List<?> packets && !packets.isEmpty(),
                "actual child JVM identity is missing");
        for (Object raw : (List<?>) actual.get("childJvmRuntime")) {
            var packet = object(raw);
            require(number(packet, "actualPid") == expected.runtime().pid()
                            && number(packet, "jvmStartTimeMillis") == expected.runtime().jvmStartTimeMillis(),
                    "actual child JVM identity differs");
        }
    }

    /** Byte binding is a proof component; source work allocations and complete run coverage are checked separately. */
    static Bound verifyFiles(Path bodyPath, Registration registration) throws IOException {
        require(bodyPath != null && bodyPath.isAbsolute() && bodyPath.equals(bodyPath.normalize()) && registration != null,
                "registered body path is missing or unnormalized");
        var scope = BenchmarkReturnWorkBudget.current();
        Path realParent = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.PRODUCER_FINAL_SEAL,
                () -> bodyPath.getParent().toRealPath());
        require(bodyPath.getParent().equals(realParent), "registered parent path is not canonical");
        byte[] body = readBounded(bodyPath, MAX_BODY_BYTES);
        byte[] companion = readBounded(Path.of(bodyPath + ".seal"), COMPANION_BYTES);
        require(companion.length == COMPANION_BYTES && body.length > 0, "final body or companion size differs");
        ByteBuffer seal = ByteBuffer.wrap(companion).order(ByteOrder.BIG_ENDIAN);
        require(seal.getLong(0) == 0x5752425345414c31L && seal.getInt(8) == 1 && seal.getInt(12) == 1,
                "companion is not completed");
        require(seal.getLong(16) == registration.runtime().pid()
                        && seal.getLong(24) == registration.runtime().jvmStartTimeMillis(), "companion runtime differs");
        require(seal.getLong(32) == body.length && seal.getLong(40) == 0,
                "companion body length or post-body activity differs");
        String bodyHash = hash(body), companionHash = hash(companion);
        require(hex(companion, 48).equals(hash(registration.nonce().getBytes(StandardCharsets.US_ASCII)))
                        && hex(companion, 80).equals(bodyHash) && hex(companion, 112).equals(FORMULA_SHA256)
                        && hex(companion, 144).equals(registration.inputSha256()), "companion content binding differs");
        require(seal.getLong(176) >= 0 && seal.getLong(184) == 2 && seal.getLong(192) == 0
                        && seal.getLong(200) == 1 && seal.getLong(208) == 1 && seal.getLong(216) == 1,
                "final write, force or shutdown evidence is incomplete");
        for (int i = 224; i < COMPANION_BYTES; i++) { require(companion[i] == 0, "companion reserved bytes differ"); }
        require(java.util.Arrays.equals(companion, readBounded(Path.of(bodyPath + ".seal"), COMPANION_BYTES)),
                "companion changed during post-exit validation");

        require(java.util.stream.IntStream.range(0, body.length).allMatch(i -> body[i] >= 32 && body[i] <= 126),
                "body is not compact ASCII");
        String raw = new String(body, StandardCharsets.US_ASCII);
        requireJsonDepth(raw);
        BenchmarkReturnWorkBudget.add(scope, BenchmarkReturnWorkBudget.Activity.RAW_RECHECK,
                new BenchmarkReturnWorkBudget.Work(1, 0, 0, 0, body.length, 1, body.length, 0));
        Object decoded;
        try { decoded = JsonReader.parse(raw); }
        catch (IllegalArgumentException invalid) { throw new AssertionError("producer work seal body JSON is malformed", invalid); }
        var frozen = BenchmarkReturnWorkBudgetDefinitions.freezeOutputTree(decoded,
                new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(MAX_BODY_BYTES, 12, 1024, MAX_BODY_BYTES, 128, 24));
        BenchmarkReturnWorkBudget.add(scope, BenchmarkReturnWorkBudget.Activity.COPY,
                new BenchmarkReturnWorkBudget.Work(1, 0, 0, 0, frozen.work().transientUtf8CopyUpperBytes(),
                        1, frozen.work().nodes(), 0));
        require(raw.equals(JsonWriter.write(frozen.value())), "body is noncanonical or duplicates fields");
        var data = object(frozen.value());
        require(number(data, "schemaVersion") == 1 && Boolean.TRUE.equals(data.get("enabled"))
                        && "WRITE_RETURN_PRODUCER_WORK_V1".equals(data.get("definitionId"))
                        && FORMULA_SHA256.equals(data.get("sourceFormulaSha256"))
                        && registration.inputSha256().equals(data.get("inputSha256"))
                        && registration.runId().equals(data.get("runId"))
                        && hash(registration.nonce().getBytes(StandardCharsets.US_ASCII)).equals(data.get("nonceSha256")),
                "body registration differs");
        require(number(data, "pid") == registration.runtime().pid()
                        && number(data, "jvmStartTimeMillis") == registration.runtime().jvmStartTimeMillis()
                        && number(data, "generation") == seal.getLong(176), "body runtime or generation differs");
        require("SHUTDOWN_HOOK".equals(data.get("stage")) && "SHUTDOWN_HOOK".equals(data.get("lifecycle"))
                        && "RECORDED_STRUCTURAL_DIAGNOSTIC".equals(data.get("state"))
                        && Boolean.FALSE.equals(data.get("processExitObserved")), "body claims an unsupported stage");
        require(Boolean.TRUE.equals(data.get("sealTailIncludedInSourceUpper"))
                        && Boolean.TRUE.equals(data.get("actualSealTailInCompanion"))
                        && Boolean.FALSE.equals(data.get("sourceUpperIsActualWork"))
                        && Boolean.FALSE.equals(data.get("limitsIndependentlyVerified"))
                        && Boolean.FALSE.equals(data.get("crashDurabilityQualified"))
                        && number(data, "pending") == 0 && number(data, "openWriters") == 0
                        && number(data, "omitted") == 0 && number(data, "lateMutations") == 0
                        && Boolean.FALSE.equals(data.get("overflow")) && Boolean.FALSE.equals(data.get("overBudget"))
                        && data.get("gaps") instanceof List<?> gaps && gaps.isEmpty(), "producer lifecycle is incomplete");
        for (String flag : List.of("structuralBudgetQualified", "accountingErrorBoundQualified", "wholeMethodCostQualified",
                "collectionCostUpperBoundQualified", "samplingCostQualified", "causalOverheadQualified",
                "costAcceptanceEligible", "performanceAcceptanceEligible", "formalPerformance")) {
            require(Boolean.FALSE.equals(data.get(flag)), "body claimed a qualification");
        }
        var activities = object(data.get("activities"));
        require(activities.keySet().equals(ACTIVITY_NAMES), "producer activity roster differs");
        for (Object value : activities.values()) {
            var row = object(value);
            require(row.keySet().equals(java.util.Set.of("invocations", "completed", "failed", "pending"))
                            && number(row, "invocations") == number(row, "completed")
                            && number(row, "failed") == 0 && number(row, "pending") == 0,
                    "producer activity has failed or pending work");
        }
        @SuppressWarnings("unchecked") Map<String, Object> typed = (Map<String, Object>) frozen.value();
        return new Bound(typed, bodyHash, companionHash, body.length, seal.getLong(176), seal.getLong(184));
    }

    private static byte[] readBounded(Path path, int upper) throws IOException {
        var scope = BenchmarkReturnWorkBudget.current();
        boolean regular = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.PRODUCER_FINAL_SEAL,
                () -> !Files.isSymbolicLink(path) && Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS));
        require(regular, "final file is missing or linked");
        var input = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.PRODUCER_FINAL_SEAL,
                () -> Files.newInputStream(path));
        Throwable primary = null;
        try {
            byte[] bytes = BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.PRODUCER_FINAL_SEAL,
                    () -> input.readNBytes(upper + 1));
            BenchmarkReturnWorkBudget.add(scope, BenchmarkReturnWorkBudget.Activity.COPY,
                    BenchmarkReturnWorkBudget.Work.bytes(bytes.length));
            require(bytes.length <= upper, "final file exceeds its prior byte bound");
            return bytes;
        } catch (IOException | RuntimeException | Error failure) { primary = failure; throw failure; }
        finally {
            try {
                BenchmarkReturnWorkBudget.external(scope, BenchmarkReturnWorkBudget.Activity.CLEANUP,
                        () -> { input.close(); return null; });
            } catch (IOException | RuntimeException | Error closeFailure) {
                if (primary == null) { throw closeFailure; }
                primary.addSuppressed(closeFailure);
            }
        }
    }

    private static void requireJsonDepth(String raw) {
        int depth = 0, token = 0; boolean quoted = false, escape = false;
        for (int i = 0; i < raw.length(); i++) {
            char c = raw.charAt(i);
            if (quoted) {
                require(++token <= 6144, "body string token exceeds its prior bound");
                if (escape) escape = false; else if (c == '\\') escape = true; else if (c == '"') quoted = false;
            }
            else if (c == '"') { quoted = true; token = 0; }
            else if (c == '{' || c == '[') { require(++depth <= 12, "body exceeds its prior JSON depth"); }
            else if (c == '}' || c == ']') { require(--depth >= 0, "body JSON nesting is malformed"); }
            else if (c == '-' || c >= '0' && c <= '9') {
                int first = i;
                while (i + 1 < raw.length() && raw.charAt(i + 1) >= '0' && raw.charAt(i + 1) <= '9') { i++; }
                require(i - first + 1 <= 20, "body integer token exceeds its prior bound");
            } else if (raw.startsWith("true", i)) { i += 3; }
            else if (raw.startsWith("false", i)) { i += 4; }
            else if (raw.startsWith("null", i)) { i += 3; }
            else { require(c == ':' || c == ',' || c == ' ', "body contains an unsupported scalar token"); }
        }
        require(!quoted && depth == 0, "body JSON is incomplete");
    }

    private static Map<?, ?> object(Object value) { require(value instanceof Map<?, ?>, "required body object is missing"); return (Map<?, ?>) value; }
    private static long number(Map<?, ?> value, String key) {
        Object raw = value.get(key); require(raw instanceof Long || raw instanceof Integer, "body integer is missing");
        long result = ((Number) raw).longValue(); require(result >= 0, "body integer is negative"); return result;
    }
    private static String hex(byte[] value, int offset) { return HexFormat.of().formatHex(value, offset, offset + 32); }
    private static void digest(String value) { require(value != null && value.matches("[a-f0-9]{64}"), "registered digest is invalid"); }
    private static String hash(byte[] value) {
        try {
            BenchmarkReturnWorkBudget.add(BenchmarkReturnWorkBudget.current(), BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT,
                    new BenchmarkReturnWorkBudget.Work(1, 0, 0, 0, value.length, 1, value.length, 0));
            return BenchmarkReturnWorkBudget.external(BenchmarkReturnWorkBudget.current(), BenchmarkReturnWorkBudget.Activity.INPUT_FINGERPRINT,
                    () -> HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value)));
        }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    private static void require(boolean condition, String reason) { if (!condition) { throw new AssertionError("producer work seal " + reason); } }
}
