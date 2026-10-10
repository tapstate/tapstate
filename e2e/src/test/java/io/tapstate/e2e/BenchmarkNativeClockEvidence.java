package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Bounded native route facts; matching provenance never establishes clock or performance accuracy. */
final class BenchmarkNativeClockEvidence {
    static final List<String> FLAGS = List.of("clockQualified", "siTimeQualified", "utcAccuracyQualified",
            "samplingAgeQualified", "callBoundaryQualified", "loadingQualified", "classIntrinsicQualified",
            "completeOsFunctionBodyQualified", "samplingCostQualified", "costAcceptanceEligible",
            "performanceAcceptanceEligible", "formalPerformance");
    private static final Set<String> FIELDS = Set.of("schemaVersion", "state", "reason", "failureType", "configured",
            "pid", "jvmStartTimeMillis", "provider", "activeProvider", "counterUnit", "mixedDomainPossible", "before", "after",
            "clockQualified", "siTimeQualified", "utcAccuracyQualified", "samplingAgeQualified", "callBoundaryQualified",
            "loadingQualified", "classIntrinsicQualified", "completeOsFunctionBodyQualified", "samplingCostQualified",
            "costAcceptanceEligible", "performanceAcceptanceEligible", "formalPerformance");
    private static final Set<String> SNAPSHOT = Set.of("bootUuid", "timebaseNumer", "timebaseDenom", "conversion",
            "loadedJniPath", "loadedJniSha256", "classSha256", "classHashes", "osFunction", "osImagePath", "osImageUuid",
            "osFunctionImageOffsetUnsigned", "osFunctionCodePrefixBytes", "osFunctionCodePrefixSha256", "userTimebaseSelector",
            "timebaseOffsetUnsigned");
    private static final Set<String> CLASSES = Set.of("PdkBenchmarkClock", "PdkBenchmarkClock$Clock",
            "PdkBenchmarkClock$JniAccess", "PdkBenchmarkClock$NativeAccess");
    private static final Set<String> UNKNOWN_REASONS = Set.of("CONFIGURATION_UNAVAILABLE", "NATIVE_SETUP_UNAVAILABLE",
            "NATIVE_READ_UNAVAILABLE", "NATIVE_IDENTITY_CHANGED", "NATIVE_METADATA_UNAVAILABLE",
            "CLASS_IDENTITY_CHANGED", "CLASS_IDENTITY_UNAVAILABLE");
    private static final String NATIVE = "DIRECT_MACH_ABSOLUTE_TIME_JNI";
    private static final String SYSTEM = "SYSTEM_NANO_TIME";

    private BenchmarkNativeClockEvidence() { }

    static Map<String, Object> parse(String raw, BenchmarkCausalClock.Identity owner, String library) {
        require(raw != null && raw.length() <= 8192 && raw.chars().allMatch(c -> c >= 32 && c <= 126),
                "metadata is missing or exceeds its compact ASCII bound");
        require(owner != null, "owned runtime identity is missing"); path(library);
        Object decoded;
        try { decoded = JsonReader.parse(raw); }
        catch (IllegalArgumentException malformed) { throw new AssertionError("native clock metadata JSON is malformed", malformed); }
        require(raw.equals(JsonWriter.write(decoded)), "JSON is noncanonical or contains duplicate fields");
        Map<?, ?> root = object(decoded, FIELDS, "metadata");
        require(number(root, "schemaVersion") == 1 && number(root, "pid") == owner.pid()
                && number(root, "jvmStartTimeMillis") == owner.jvmStartTimeMillis(), "metadata has another schema or runtime identity");
        for (String flag : FLAGS) { require(Boolean.FALSE.equals(root.get(flag)), "metadata claimed a qualification"); }
        require("nominal-ns".equals(root.get("counterUnit")), "counter unit differs");
        boolean configured = bool(root, "configured"), mixed = bool(root, "mixedDomainPossible");
        String state = text(root.get("state"), 32), reason = text(root.get("reason"), 64), failure = text(root.get("failureType"), 128);
        require((configured ? NATIVE : SYSTEM).equals(root.get("provider")), "declared provider differs");
        if (state.equals("NATIVE_RECORDED")) {
            require(configured && !mixed && NATIVE.equals(root.get("activeProvider")) && reason.equals("NONE") && failure.equals("NONE"),
                    "native provider is inactive or mixed");
        } else if (state.equals("SYSTEM_UNQUALIFIED")) {
            require(!configured && !mixed && SYSTEM.equals(root.get("activeProvider")) && reason.equals("NONE") && failure.equals("NONE"),
                    "system fallback state differs");
        } else {
            require(state.equals("UNKNOWN") && UNKNOWN_REASONS.contains(reason) && !failure.equals("NONE")
                    && SYSTEM.equals(root.get("activeProvider")) && mixed == configured, "unknown state is malformed");
        }
        Map<String, Object> before = snapshot(root.get("before"), library, configured, state);
        Map<String, Object> after = snapshot(root.get("after"), library, configured, state);
        if (!state.equals("UNKNOWN")) { require(before.equals(after), "before and after native identities changed"); }
        var result = new LinkedHashMap<String, Object>();
        root.forEach((key, value) -> result.put((String) key, value));
        result.put("before", before); result.put("after", after);
        return Map.copyOf(result);
    }

    static void requireMatchingNative(Map<String, Object> owned, Map<String, Object> root) {
        require("NATIVE_RECORDED".equals(owned.get("state")) && "NATIVE_RECORDED".equals(root.get("state")),
                "root or owned provider is unknown or a system fallback");
        require(owned.get("before").equals(root.get("before")), "root and owned native route identities differ");
    }

    private static Map<String, Object> snapshot(Object value, String library, boolean configured, String state) {
        require(value instanceof Map<?, ?>, "snapshot is not an object");
        Map<?, ?> map = (Map<?, ?>) value;
        if (state.equals("UNKNOWN") && map.isEmpty()) { return Map.of(); }
        if (!configured) {
            object(value, Set.of("classSha256", "classHashes"), "system class snapshot");
        } else { object(value, SNAPSHOT, "native snapshot"); }
        Map<?, ?> classes = object(map.get("classHashes"), CLASSES, "class hash roster");
        var hashes = new LinkedHashMap<String, String>();
        CLASSES.forEach(name -> hashes.put(name, hash(classes.get(name))));
        require(hash(map.get("classSha256")).equals(hashes.get("PdkBenchmarkClock")), "primary class hash differs");
        if (configured) {
            require(text(map.get("bootUuid"), 36).matches("[A-Fa-f0-9]{8}(?:-[A-Fa-f0-9]{4}){3}-[A-Fa-f0-9]{12}"), "boot identity is malformed");
            for (String scale : List.of("timebaseNumer", "timebaseDenom")) {
                long valueOfScale = number(map, scale); require(valueOfScale > 0 && valueOfScale <= 0xffff_ffffL, "timebase scale is invalid");
            }
            require("UNSIGNED_128_MULTIPLY_INTEGER_DIVIDE_TRUNCATE".equals(map.get("conversion")), "integer conversion differs");
            require(library.equals(path(text(map.get("loadedJniPath"), 512))), "actual JNI path differs");
            hash(map.get("loadedJniSha256"));
            require("mach_absolute_time".equals(map.get("osFunction")), "OS function differs");
            path(text(map.get("osImagePath"), 512));
            require(text(map.get("osImageUuid"), 32).matches("[A-Fa-f0-9]{32}"), "OS image identity is malformed");
            unsigned(map.get("osFunctionImageOffsetUnsigned")); unsigned(map.get("timebaseOffsetUnsigned"));
            require(number(map, "osFunctionCodePrefixBytes") == 144, "OS code-prefix byte count differs");
            hash(map.get("osFunctionCodePrefixSha256"));
            require(number(map, "userTimebaseSelector") <= 255, "selected counter is invalid");
        }
        var result = new LinkedHashMap<String, Object>();
        map.forEach((key, entry) -> result.put((String) key, entry));
        result.put("classHashes", Map.copyOf(hashes)); return Map.copyOf(result);
    }
    private static String path(String value) {
        String result = text(value, 512); Path path = Path.of(result);
        require(path.isAbsolute() && path.normalize().toString().equals(result), "path is not an exact absolute route"); return result;
    }
    private static String hash(Object value) { String result = text(value, 64); require(result.matches("[0-9a-f]{64}"), "SHA-256 is malformed"); return result; }
    private static void unsigned(Object value) {
        String result = text(value, 20); require(result.matches("0|[1-9][0-9]{0,19}")
                && new BigInteger(result).bitLength() <= 64, "unsigned offset is malformed");
    }
    private static Map<?, ?> object(Object value, Set<String> fields, String name) {
        require(value instanceof Map<?, ?> && ((Map<?, ?>) value).keySet().equals(fields), name + " has missing or extra fields"); return (Map<?, ?>) value;
    }
    private static long number(Map<?, ?> values, String field) {
        require(values.get(field) instanceof Long && (Long) values.get(field) >= 0, field + " is not an exact nonnegative long"); return (Long) values.get(field);
    }
    private static boolean bool(Map<?, ?> values, String field) { require(values.get(field) instanceof Boolean, field + " is not boolean"); return (Boolean) values.get(field); }
    private static String text(Object value, int bound) {
        require(value instanceof String && !((String) value).isEmpty() && ((String) value).length() <= bound
                && ((String) value).chars().allMatch(c -> c >= 32 && c <= 126), "text is missing or unbounded"); return (String) value;
    }
    private static void require(boolean condition, String reason) { if (!condition) { throw new AssertionError("native clock evidence " + reason); } }
}
