package io.tapstate.e2e;

import java.util.List;
import java.util.Map;

/** Conditional nominal-counter diagnostics; binding facts never establish formal clock or cost qualification. */
final class BenchmarkNativeReturnClock implements BenchmarkReturnPointClock {
    private static final String JNI_SHA256 = "4ce1204977af372080e3a3a8ada2c03ce678a15b337e3201f13d30b22a4700c9";
    private static final String JNI_SOURCE_SHA256 = "f25f42fdf196d1af702782cd6a87a86f7b696f10689cf27e768b377a19cff4a1";
    private static final Map<String, String> CLASS_HASHES = Map.of(
            "PdkBenchmarkClock", "42fffc3e10a70ae503e85c909cac3d5f437c07c2310115a59db875124c53b1f2",
            "PdkBenchmarkClock$Clock", "82c52a87d590e4ac400af03e51827a892d7988e27496883169f817fc83a89751",
            "PdkBenchmarkClock$JniAccess", "2ccedc46ae98b4ea4e6c79c112a53fc3953efc3c344d2f3e6ba1e4d78eabaeb2",
            "PdkBenchmarkClock$NativeAccess", "4b67c17ed28c1d341d9e27819563d1d89eb017073b8492f808e35da31f5ddfb7");
    private final BenchmarkCausalClock coverage;
    private final Map<String, Object> snapshot;

    BenchmarkNativeReturnClock(BenchmarkCausalClock.Identity owned, BenchmarkCausalClock.Identity root,
            String library, List<BenchmarkCausalClock.Sample> samples,
            Map<String, Object> before, Map<String, Object> after) {
        require(owned != null && root != null && library != null && before != null && after != null,
                "actual owned/root identity, library and both cold receipts are required");
        require(owned.pid() != root.pid(), "owned and root must be distinct actual processes");
        coverage = new BenchmarkCausalClock(owned, samples);
        Map<String, Object> first = cold(before, owned, root, library);
        Map<String, Object> last = cold(after, owned, root, library);
        require(first.equals(last), "native route changed across the measured phase");
        for (var sample : coverage.samples()) {
            require(sample.driverBeforeNanos() >= 0 && sample.ownedNanos() >= 0
                    && sample.driverBeforeNanos() <= sample.ownedNanos()
                    && sample.ownedNanos() <= sample.driverAfterNanos(),
                    "owned sample is outside its actual common-counter root bracket");
        }
        snapshot = first;
    }

    @Override public BenchmarkCausalClock.Interval map(BenchmarkCausalClock.Identity identity, long pointNanos) {
        require(pointNanos >= 0, "native point is outside the unsigned conversion's signed bound");
        coverage.map(identity, pointNanos);
        return new BenchmarkCausalClock.Interval(pointNanos, pointNanos);
    }

    Map<String, Object> evidence() {
        var result = new java.util.LinkedHashMap<String, Object>();
        result.putAll(Map.of("state", "CONDITIONAL_COMMON_NATIVE_COUNTER_DIAGNOSTIC",
                "mapping", "CAPTURED_POINTS_SHARE_THE_BOUND_NOMINAL_COUNTER_COORDINATE",
                "counterUnit", "nominal-ns", "snapshot", snapshot,
                "conditions", List.of("ACTUAL_IN_INVOCATION_DIRECT_OS_READ", "MONOTONE_OS_COUNTER_OVER_ACCEPTED_WINDOW",
                        "BOUND_COMMON_BOOT_CONVERSION_ROUTE_AND_NO_FALLBACK"),
                "literalReturn", "SAME_CALL_CALLBACK_EXIT_TO_AFTER_NORMAL_RETURN_OBSERVATION_CLOSED_INTERVAL",
                "clockQualified", false, "formalPerformance", false,
                "performanceAcceptanceEligible", false, "samplingCostQualified", false));
        BenchmarkNativeClockEvidence.FLAGS.forEach(flag -> result.put(flag, false));
        result.put("reviewedJniSourceSha256", JNI_SOURCE_SHA256);
        result.put("bindingScope", "ONE_INSPECTED_NATIVE_ARTIFACT_AND_LOADED_OS_ROUTE");
        return Map.copyOf(result);
    }

    private static Map<String, Object> cold(Map<String, Object> receipt, BenchmarkCausalClock.Identity owned,
            BenchmarkCausalClock.Identity root, String library) {
        require("NATIVE_RECORDED".equals(receipt.get("state")) && Boolean.TRUE.equals(receipt.get("rawRetained")),
                "cold native receipt is unknown or missing its original raw metadata");
        for (String flag : BenchmarkNativeClockEvidence.FLAGS) {
            require(Boolean.FALSE.equals(receipt.get(flag)), "cold metadata claimed a qualification");
        }
        require("-Dtapstate.benchmark.write-return=true".equals(receipt.get("matchedRuntimeFlag"))
                && ("-Dtapstate.benchmark.native-clock-library=" + library).equals(receipt.get("matchedNativeClockFlag")),
                "actual runtime flags differ from the native diagnostic");
        require(owned.pid() == number(receipt, "expectedPid") && owned.jvmStartTimeMillis() == number(receipt, "expectedJvmStartTimeMillis")
                && root.pid() == number(receipt, "expectedRootPid") && root.jvmStartTimeMillis() == number(receipt, "expectedRootJvmStartTimeMillis"),
                "cold receipt belongs to another owned or root runtime");
        Map<String, Object> ownedMetadata = BenchmarkNativeClockEvidence.parse(text(receipt.get("raw")), owned, library);
        require(receipt.get("rootMetadata") instanceof Map<?, ?>, "actual root raw metadata is missing");
        Map<?, ?> rootRaw = (Map<?, ?>) receipt.get("rootMetadata");
        require(Boolean.TRUE.equals(rootRaw.get("rawRetained")), "actual root raw metadata was not retained");
        Map<String, Object> rootMetadata = BenchmarkNativeClockEvidence.parse(text(rootRaw.get("raw")), root, library);
        BenchmarkNativeClockEvidence.requireMatchingNative(ownedMetadata, rootMetadata);
        require(ownedMetadata.equals(receipt.get("ownedMetadata")) && rootMetadata.equals(receipt.get("parsedRootMetadata")),
                "retained parsed metadata differs from its actual raw bytes");
        for (var field : ownedMetadata.entrySet()) {
            require(field.getValue().equals(receipt.get(field.getKey())), "cold receipt duplicated metadata differs from its raw bytes");
        }
        Object value = ownedMetadata.get("before");
        require(value instanceof Map<?, ?>, "native route snapshot is missing");
        @SuppressWarnings("unchecked") Map<String, Object> result = (Map<String, Object>) value;
        supported(result);
        return result;
    }

    static Map<String, Object> supportedRootSnapshot(Map<String, Object> parsed) {
        BenchmarkNativeClockEvidence.requireMatchingNative(parsed, parsed);
        require(parsed.get("before") instanceof Map<?, ?>, "actual root snapshot is missing");
        @SuppressWarnings("unchecked") Map<String, Object> snapshot = (Map<String, Object>) parsed.get("before");
        supported(snapshot);
        return snapshot;
    }

    /** Different binaries or counter routes require fresh validation; hashes are not accuracy proofs. */
    private static void supported(Map<String, Object> snapshot) {
        require(JNI_SHA256.equals(snapshot.get("loadedJniSha256")) && CLASS_HASHES.equals(snapshot.get("classHashes")),
                "native getter artifact is outside the inspected diagnostic binding");
        require("mach_absolute_time".equals(snapshot.get("osFunction"))
                && "/usr/lib/system/libsystem_kernel.dylib".equals(snapshot.get("osImagePath"))
                && "f63bf4188f7534a4aaf18caf2b9b6a05".equals(snapshot.get("osImageUuid"))
                && "4236".equals(snapshot.get("osFunctionImageOffsetUnsigned"))
                && Long.valueOf(144).equals(snapshot.get("osFunctionCodePrefixBytes"))
                && "c615a84440f8ae7325e75d236ace6e038838f5cf634f9bba912d2904c99ff2fd".equals(snapshot.get("osFunctionCodePrefixSha256"))
                && Long.valueOf(3).equals(snapshot.get("userTimebaseSelector"))
                && Long.valueOf(125).equals(snapshot.get("timebaseNumer")) && Long.valueOf(3).equals(snapshot.get("timebaseDenom")),
                "loaded OS counter route is outside the inspected diagnostic binding");
    }

    private static long number(Map<String, Object> map, String field) {
        require(map.get(field) instanceof Long, "actual runtime identity field is malformed"); return (Long) map.get(field);
    }
    private static String text(Object value) {
        require(value instanceof String, "raw metadata is missing"); return (String) value;
    }
    private static void require(boolean condition, String reason) {
        if (!condition) { throw new AssertionError("native return clock " + reason); }
    }
}
