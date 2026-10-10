package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Exact bounded elapsed-stage observations; overlapping sums never establish CPU or causal cost. */
final class BenchmarkWriteReturnCostStages {
    static final int MAX_BYTES = 8 * 1024;
    static final List<String> STAGES = List.of("BEGIN_LOCK_WAIT", "IDENTITY_ENCODING", "SCOPE_BEFORE",
            "SCOPE_AFTER", "COMPLETE_LOCK_WAIT", "RECEIPT_ENCODING_PUBLICATION");
    private static final Set<String> FIELDS = Set.of("schemaVersion", "enabled", "pid", "jvmStartTimeMillis",
            "epoch", "windowBase64", "fullCallCount", "completeTimedCalls", "state", "reason", "timeUnit",
            "timeScope", "performanceAcceptanceEligible", "samplingCostQualified", "costAcceptanceEligible",
            "formalPerformance", "causalOverheadQualified", "stages");
    private static final Set<String> UNKNOWN_REASONS = Set.of("CLOCK_UNAVAILABLE", "TIMING_EPOCH_CHANGED",
            "CLOCK_ORDER_OR_OVERFLOW", "STAGES_MISSING", "FAILED_CALL", "RETURN_RECEIPT_UNQUALIFIED",
            "CALL_CAPACITY_EXCEEDED", "CALL_COUNT_OVERFLOW", "STAGE_SUM_OVERFLOW");

    private BenchmarkWriteReturnCostStages() { }

    static Map<String, Object> parse(String raw, BenchmarkCausalClock.Identity owner,
            long epoch, String window, long completedCalls) {
        require(raw != null && raw.length() <= MAX_BYTES && raw.chars().allMatch(c -> c >= 32 && c <= 126),
                "raw summary is missing or exceeds its compact ASCII bound");
        require(owner != null && epoch > 0 && completedCalls >= 0 && completedCalls <= 512
                && window != null && !window.isBlank() && window.getBytes(StandardCharsets.UTF_8).length <= 512,
                "actual capture binding is missing or exceeds its bound");
        Object decoded;
        try { decoded = JsonReader.parse(raw); }
        catch (IllegalArgumentException malformed) { throw new AssertionError("return cost stages JSON is malformed", malformed); }
        // The producer emits compact canonical JSON. A round trip also rejects overwritten duplicate keys.
        require(raw.equals(JsonWriter.write(decoded)), "JSON is noncanonical or contains duplicate fields");
        Map<?, ?> root = object(decoded, FIELDS, "summary");
        require(number(root, "schemaVersion") == 1 && Boolean.TRUE.equals(root.get("enabled")), "schema or enabled state differs");
        require(number(root, "pid") == owner.pid() && number(root, "jvmStartTimeMillis") == owner.jvmStartTimeMillis(),
                "summary has another owned runtime identity");
        require(number(root, "epoch") == epoch && number(root, "fullCallCount") == completedCalls,
                "summary differs from the actual capture epoch or full-call roster");
        require(window.equals(window(root.get("windowBase64"))), "summary belongs to another actual capture window");
        require("ns".equals(root.get("timeUnit")) && "ELAPSED_NOT_CPU".equals(root.get("timeScope")), "elapsed time scope differs");
        for (String flag : List.of("performanceAcceptanceEligible", "samplingCostQualified", "costAcceptanceEligible",
                "formalPerformance", "causalOverheadQualified")) {
            require(Boolean.FALSE.equals(root.get(flag)), "summary supplied an acceptance or causal qualification");
        }
        long complete = number(root, "completeTimedCalls");
        require(complete <= completedCalls, "complete timing count exceeds its full-call roster");
        String state = text(root.get("state")), reason = text(root.get("reason"));
        require(state.equals("RECORDED") && reason.equals("NONE") && complete == completedCalls
                || state.equals("UNKNOWN") && UNKNOWN_REASONS.contains(reason), "state, reason or complete timing count is unsupported");
        Map<?, ?> stages = object(root.get("stages"), Set.copyOf(STAGES), "stage roster");
        var retainedStages = new LinkedHashMap<String, Object>();
        for (String stage : STAGES) {
            Map<?, ?> counters = object(stages.get(stage), Set.of("count", "sumNanos", "maxNanos"), "stage counters");
            long count = number(counters, "count"), sum = number(counters, "sumNanos"), maximum = number(counters, "maxNanos");
            require(count == complete && maximum <= sum && (count != 0 || sum == 0 && maximum == 0)
                    && BigInteger.valueOf(sum).compareTo(BigInteger.valueOf(maximum).multiply(BigInteger.valueOf(count))) <= 0,
                    "stage count, sum or maximum contradicts its finite domain");
            retainedStages.put(stage, Map.of("count", count, "sumNanos", sum, "maxNanos", maximum));
        }
        var parsed = new LinkedHashMap<String, Object>();
        root.forEach((key, value) -> parsed.put((String) key, value));
        parsed.put("stages", Map.copyOf(retainedStages));
        Map<String, Object> snapshot = Map.copyOf(parsed);
        var result = new LinkedHashMap<>(snapshot);
        result.putAll(rawEvidence(raw)); result.put("parsed", snapshot);
        result.put("stageSumScope", "REPORTED_STAGE_SUMS_MAY_OVERLAP_NOT_EXCLUSIVE_CPU_OR_CAUSAL_OVERHEAD");
        return Map.copyOf(result);
    }

    static Map<String, Object> rawEvidence(String raw) {
        var result = new LinkedHashMap<String, Object>();
        result.put("rawAvailable", raw != null); result.put("rawRetained", false);
        if (raw != null) {
            result.put("rawLengthChars", raw.length());
            if (raw.length() <= MAX_BYTES) {
                int bytes = raw.getBytes(StandardCharsets.UTF_8).length;
                result.put("rawUtf8Bytes", bytes);
                if (bytes <= MAX_BYTES) { result.put("raw", raw); result.put("rawRetained", true); }
            }
        }
        return Map.copyOf(result);
    }

    private static String window(Object value) {
        String encoded = text(value);
        require(encoded.length() <= 4 * ((512 + 2) / 3), "window encoding exceeds its bound");
        try {
            byte[] bytes = Base64.getDecoder().decode(encoded);
            require(bytes.length <= 512 && Base64.getEncoder().encodeToString(bytes).equals(encoded), "window Base64 is noncanonical");
            return StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(bytes)).toString();
        } catch (IllegalArgumentException | java.nio.charset.CharacterCodingException malformed) {
            throw new AssertionError("return cost stages window encoding is malformed", malformed);
        }
    }
    private static Map<?, ?> object(Object value, Set<String> fields, String name) {
        require(value instanceof Map<?, ?>, name + " is not an object");
        var result = (Map<?, ?>) value;
        require(result.keySet().equals(fields), name + " has missing or extra fields"); return result;
    }
    private static long number(Map<?, ?> values, String field) {
        require(values.get(field) instanceof Long, field + " is not an exact finite long");
        long value = (Long) values.get(field); require(value >= 0, field + " is negative"); return value;
    }
    private static String text(Object value) { require(value instanceof String, "text field is missing or untyped"); return (String) value; }
    private static void require(boolean condition, String reason) {
        if (!condition) { throw new AssertionError("return cost stages " + reason); }
    }
}
