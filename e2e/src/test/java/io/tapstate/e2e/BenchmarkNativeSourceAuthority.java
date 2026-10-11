package io.tapstate.e2e;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Immutable source/API recipe authority; it neither acquires runtime facts nor grants metric credit. */
final class BenchmarkNativeSourceAuthority implements BenchmarkReturnMethodAdmission.IndependentSemanticReview {
    private static final BenchmarkNativeSourceAuthority INSTANCE = new BenchmarkNativeSourceAuthority();
    private static final String RECIPE = "DIRECT_MACH_CURRENT_NOMINAL_COUNTER_FLOOR_V1";
    private static final String JNI_SOURCE = "f25f42fdf196d1af702782cd6a87a86f7b696f10689cf27e768b377a19cff4a1";
    private static final String JNI_BINARY = "4ce1204977af372080e3a3a8ada2c03ce678a15b337e3201f13d30b22a4700c9";
    private static final String CONVERSION = "UNSIGNED_128_MULTIPLY_INTEGER_DIVIDE_TRUNCATE";
    private static final List<String> PATH = List.of("PdkBenchmarkClock.nanoTime/source", "Clock.read", "JniAccess.read",
            "nativeNanoTime", "mach_absolute_time", "unsigned128 multiply/divide");
    private static final Map<String, String> CLASSES = Map.of(
            "PdkBenchmarkClock", "a1bdb9f101060206e73b28fd521063bf59789f56fbd9ad78fe82ffed593e9e35",
            "PdkBenchmarkClock$Clock", "82c52a87d590e4ac400af03e51827a892d7988e27496883169f817fc83a89751",
            "PdkBenchmarkClock$JniAccess", "61795d9670600c61b523c4611cbe8600b17e05c156ffebd582f08bd723a209c2",
            "PdkBenchmarkClock$NativeAccess", "4b67c17ed28c1d341d9e27819563d1d89eb017073b8492f808e35da31f5ddfb7");
    private static final Set<String> ROUTE_FIELDS = Set.of("bootUuid", "timebaseNumer", "timebaseDenom", "conversion",
            "loadedJniPath", "loadedJniSha256", "classSha256", "classHashes", "osFunction", "osImagePath", "osImageUuid",
            "osFunctionImageOffsetUnsigned", "osFunctionCodePrefixBytes", "osFunctionCodePrefixSha256",
            "userTimebaseSelector", "timebaseOffsetUnsigned");

    private BenchmarkNativeSourceAuthority() { }
    static BenchmarkNativeSourceAuthority instance() { return INSTANCE; }

    @Override public void requireInspectedDirectInvocationAndProviderContract(BenchmarkReturnMethodAdmission.InspectionRequest request) {
        BenchmarkReturnComponentSourceWork.entered(BenchmarkReturnComponentSourceWork.Kind.AUTHORITY_CHECK,
                () -> { requireBounded(request); return null; });
    }
    private void requireBounded(BenchmarkReturnMethodAdmission.InspectionRequest request) {
        require(request != null && BenchmarkReturnMethodAdmission.METHOD.equals(request.method()), "METHOD_OR_INSPECTION_MISSING");
        require(request.providerContract() == BenchmarkReturnMethodAdmission.ProviderContract.MACH_ABSOLUTE_TIME_NOMINAL_MONOTONIC_EXCLUDING_SLEEP,
                "PROVIDER_SEMANTICS_UNSUPPORTED");
        require(request.invocationPath().size() == PATH.size() && PATH.equals(request.invocationPath()),
                "DIRECT_CURRENT_INVOCATION_PATH_DIFFERS");
        require(JNI_SOURCE.equals(request.jniSourceSha256()) && JNI_BINARY.equals(request.jniBinarySha256()),
                "INSPECTED_NATIVE_MATERIAL_DIFFERS");
        var route = request.selectedRootRoute();
        require(route != null && route.size() == ROUTE_FIELDS.size() && route.keySet().equals(ROUTE_FIELDS), "SELECTED_ROUTE_ROSTER_DIFFERS");
        require(JNI_BINARY.equals(route.get("loadedJniSha256")) && CONVERSION.equals(route.get("conversion")), "SELECTED_NATIVE_CONVERSION_DIFFERS");
        require(route.get("classHashes") instanceof Map<?, ?> hashes && hashes.size() == 4
                && CLASSES.equals(hashes) && CLASSES.get("PdkBenchmarkClock").equals(route.get("classSha256")),
                "INSPECTED_JAVA_CALL_PATH_MATERIAL_DIFFERS");
        require(number(route, "timebaseNumer") == 125 && number(route, "timebaseDenom") == 3, "POSITIVE_INSPECTED_TIMEBASE_DIFFERS");
        require("mach_absolute_time".equals(route.get("osFunction"))
                && "/usr/lib/system/libsystem_kernel.dylib".equals(route.get("osImagePath"))
                && "f63bf4188f7534a4aaf18caf2b9b6a05".equals(route.get("osImageUuid"))
                && "4236".equals(route.get("osFunctionImageOffsetUnsigned"))
                && number(route, "osFunctionCodePrefixBytes") == 144
                && "c615a84440f8ae7325e75d236ace6e038838f5cf634f9bba912d2904c99ff2fd".equals(route.get("osFunctionCodePrefixSha256"))
                && number(route, "userTimebaseSelector") == 3, "INSPECTED_CURRENT_COUNTER_BRANCH_DIFFERS");
        String library = text(route, "loadedJniPath", 512);
        Path path = Path.of(library);
        require(path.isAbsolute() && path.equals(path.normalize()), "SELECTED_NATIVE_PATH_DIFFERS");
        String boot = text(route, "bootUuid", 36);
        require(boot.matches("[A-Fa-f0-9]{8}(?:-[A-Fa-f0-9]{4}){3}-[A-Fa-f0-9]{12}"), "BOOT_IDENTITY_SHAPE_DIFFERS");
        String offset = text(route, "timebaseOffsetUnsigned", 20);
        require(offset.matches("0|[1-9][0-9]{0,19}"), "TIMEBASE_OFFSET_SHAPE_DIFFERS");
        try { Long.parseUnsignedLong(offset); }
        catch (NumberFormatException overflow) { throw new AssertionError("native source authority TIMEBASE_OFFSET_OVERFLOW", overflow); }
        // The frozen recipe, rather than hash equality, supplies the behavioral implication:
        // the selected Java supplier reaches its private JNI entry once in the invocation;
        // JNI reads current mach ticks and caches only the positive conversion ratio. The
        // observed selector-3 branch reads the nonspeculative current counter, retries an
        // offset change, adds that stable offset and returns. The conformant provider API
        // defines monotonic ticks excluding sleep; positive uint128 scale/floor preserves
        // order and valid ties. Matching material selects this recipe; it does not prove
        // SI/UTC accuracy, all OS instructions, arbitrary VM transformation or future facts.
    }

    static Map<String, Object> sourceRecipeEvidence() {
        return BenchmarkReturnComponentSourceWork.entered(BenchmarkReturnComponentSourceWork.Kind.AUTHORITY_EVIDENCE,
                BenchmarkNativeSourceAuthority::sourceRecipeEvidenceBounded);
    }
    private static Map<String, Object> sourceRecipeEvidenceBounded() {
        var out = new LinkedHashMap<String, Object>();
        out.put("state", "IMMUTABLE_REVIEWED_DIRECT_SOURCE_API_RECIPE_ONLY"); out.put("recipe", RECIPE);
        out.put("invocationPath", PATH); out.put("inspectedJavaClassHashes", CLASSES);
        out.put("jniSourceSha256", JNI_SOURCE); out.put("jniBinarySha256", JNI_BINARY);
        out.put("valueOrigin", "ONE_CURRENT_MONOTONIC_TICK_READ_IN_SELECTED_INVOCATION");
        out.put("cachedState", "CONVERSION_RATIO_ONLY_NOT_A_RETURNED_TIMESTAMP");
        out.put("providerContract", "MACH_ABSOLUTE_TIME_NOMINAL_MONOTONIC_EXCLUDING_SLEEP_ARBITRARY_ORIGIN");
        out.put("selectedOsBranch", "ACNTVCT_EL0_CURRENT_NONSPECULATIVE_READ_OFFSET_DOUBLE_READ_RETRY_ADD");
        out.put("conversion", "q=floor(uint128(ticks)*125/3); reject signed-long overflow");
        out.put("runtimeScope", "REGISTERED_CONFORMANT_RUNTIME_EXECUTES_THE_BOUND_SOURCE_AND_PROVIDER_ROUTE");
        out.put("futureForkObligations", List.of("TRUSTED_ACTUAL_ROOT_AND_CHILD_OWNERS", "ACTUAL_COLD_BEFORE_AFTER_ROUTE_BINDING",
                "ORIGINAL_FULL_SOURCE_AND_RAW_CALL_COHORT", "STRICT_SAMPLE_COVERAGE_AND_CAUSAL_ORDER", "NO_STICKY_NATIVE_FALLBACK"));
        out.put("authorityScope", "SEMANTIC_RECIPE_SELECTION_NOT_ACQUISITION_NOT_AN_ADMISSION_TOKEN");
        out.put("costScope", "NEW_BOUNDED_CHECK_WORK_NOT_YET_IN_COMPLETE_STRUCTURAL_RECIPE");
        BenchmarkReturnProducerOrigins.FLAGS.forEach(flag -> out.put(flag, false));
        return Map.copyOf(out);
    }

    private static long number(Map<String, Object> route, String field) {
        Object value = route.get(field); require(value instanceof Long, "EXACT_ROUTE_INTEGER_MISSING");
        return (Long) value;
    }
    private static String text(Map<String, Object> route, String field, int maximum) {
        Object value = route.get(field); require(value instanceof String, "ROUTE_TEXT_MISSING");
        String result = (String) value;
        require(result.length() <= maximum, "ROUTE_TEXT_EXCEEDS_PRIOR_BOUND");
        require(!result.isBlank() && result.chars().allMatch(c -> c >= 32 && c <= 126), "ROUTE_TEXT_EXCEEDS_PRIOR_BOUND");
        return result;
    }
    private static void require(boolean value, String reason) { if (!value) throw new AssertionError("native source authority " + reason); }
}
