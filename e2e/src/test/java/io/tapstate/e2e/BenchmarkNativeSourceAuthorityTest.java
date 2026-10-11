package io.tapstate.e2e;

import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkNativeSourceAuthorityTest {
    private static final String SOURCE = "f25f42fdf196d1af702782cd6a87a86f7b696f10689cf27e768b377a19cff4a1";
    private static final String BINARY = "4ce1204977af372080e3a3a8ada2c03ce678a15b337e3201f13d30b22a4700c9";
    private static final List<String> PATH = List.of("PdkBenchmarkClock.nanoTime/source", "Clock.read", "JniAccess.read",
            "nativeNanoTime", "mach_absolute_time", "unsigned128 multiply/divide");

    @Test void the_fixed_source_and_current_supported_route_select_only_the_reviewed_nominal_recipe() {
        var authority = BenchmarkNativeSourceAuthority.instance();
        assertThat(authority).isSameAs(BenchmarkNativeSourceAuthority.instance());
        authority.requireInspectedDirectInvocationAndProviderContract(request(route(), PATH, SOURCE, BINARY));
        var facts = BenchmarkNativeSourceAuthority.sourceRecipeEvidence();
        assertThat(facts).containsEntry("valueOrigin", "ONE_CURRENT_MONOTONIC_TICK_READ_IN_SELECTED_INVOCATION")
                .containsEntry("cachedState", "CONVERSION_RATIO_ONLY_NOT_A_RETURNED_TIMESTAMP")
                .containsEntry("authorityScope", "SEMANTIC_RECIPE_SELECTION_NOT_ACQUISITION_NOT_AN_ADMISSION_TOKEN");
        for (String flag : BenchmarkReturnProducerOrigins.FLAGS) assertThat(facts).containsEntry(flag, false);
    }

    @Test void matching_hashes_cannot_replace_the_inspected_current_read_invocation_path() {
        for (String different : List.of("Clock.cachedRead", "nativeApproximateTime", "System.nanoTime")) {
            var path = new java.util.ArrayList<>(PATH); path.set(1, different);
            assertThatThrownBy(() -> check(route(), path, SOURCE, BINARY)).hasMessageContaining("DIRECT_CURRENT_INVOCATION_PATH_DIFFERS");
        }
        assertThatThrownBy(() -> check(route(), PATH, "0".repeat(64), BINARY)).hasMessageContaining("INSPECTED_NATIVE_MATERIAL_DIFFERS");
        assertThatThrownBy(() -> check(route(), PATH, SOURCE, "0".repeat(64))).hasMessageContaining("INSPECTED_NATIVE_MATERIAL_DIFFERS");
    }

    @Test void current_class_group_is_exact_and_cannot_be_replaced_by_legacy_or_mixed_members() {
        var base = route();
        @SuppressWarnings("unchecked") var hashes = new LinkedHashMap<>((Map<String, String>) base.get("classHashes"));
        hashes.put("PdkBenchmarkClock", "42fffc3e10a70ae503e85c909cac3d5f437c07c2310115a59db875124c53b1f2");
        base.put("classSha256", hashes.get("PdkBenchmarkClock")); base.put("classHashes", hashes);
        assertThatThrownBy(() -> check(base, PATH, SOURCE, BINARY)).hasMessageContaining("INSPECTED_JAVA_CALL_PATH_MATERIAL_DIFFERS");
        hashes.put("PdkBenchmarkClock", "a1bdb9f101060206e73b28fd521063bf59789f56fbd9ad78fe82ffed593e9e35");
        base.put("classSha256", hashes.get("PdkBenchmarkClock")); hashes.put("PdkBenchmarkClock$JniAccess", "0".repeat(64));
        assertThatThrownBy(() -> check(base, PATH, SOURCE, BINARY)).hasMessageContaining("INSPECTED_JAVA_CALL_PATH_MATERIAL_DIFFERS");
    }

    @Test void cached_approximate_selector_scale_or_conversion_routes_are_refused() {
        for (Map.Entry<String, Object> change : List.<Map.Entry<String, Object>>of(Map.entry("userTimebaseSelector", 1L),
                Map.entry("timebaseNumer", 0L), Map.entry("timebaseDenom", 1L),
                Map.entry("conversion", "DOUBLE_ROUND"), Map.entry("osFunction", "mach_approximate_time"),
                Map.entry("osFunctionCodePrefixBytes", 143L), Map.entry("osImageUuid", "0".repeat(32)))) {
            var altered = route(); altered.put(change.getKey(), change.getValue());
            assertThatThrownBy(() -> check(altered, PATH, SOURCE, BINARY)).isInstanceOf(AssertionError.class);
        }
    }

    @Test void historical_boot_and_offset_are_not_future_owner_requirements_but_shape_and_unsigned_range_are() {
        var current = route(); current.put("bootUuid", "11111111-2222-3333-4444-555555555555");
        current.put("timebaseOffsetUnsigned", "18446744073709551615");
        check(current, PATH, SOURCE, BINARY);
        for (String invalid : List.of("-1", "01", "18446744073709551616", "1".repeat(21))) {
            var altered = route(); altered.put("timebaseOffsetUnsigned", invalid);
            assertThatThrownBy(() -> check(altered, PATH, SOURCE, BINARY)).isInstanceOf(AssertionError.class);
        }
    }

    @Test void caller_qualification_or_extra_route_fields_do_not_select_a_recipe() {
        for (String marker : List.of("QUALIFIED", "clockQualified", "formalPerformance")) {
            var altered = route(); altered.put(marker, true);
            assertThatThrownBy(() -> check(altered, PATH, SOURCE, BINARY)).hasMessageContaining("SELECTED_ROUTE_ROSTER_DIFFERS");
        }
        assertThatThrownBy(() -> BenchmarkNativeSourceAuthority.instance().requireInspectedDirectInvocationAndProviderContract(null))
                .hasMessageContaining("METHOD_OR_INSPECTION_MISSING");
        var incomplete = route(); incomplete.remove("classHashes");
        assertThatThrownBy(() -> check(incomplete, PATH, SOURCE, BINARY)).hasMessageContaining("SELECTED_ROUTE_ROSTER_DIFFERS");
    }

    @Test void positive_floor_scaling_preserves_order_and_ties_without_claiming_physical_time() {
        BigInteger numerator = BigInteger.valueOf(125), denominator = BigInteger.valueOf(3);
        long previous = -1;
        for (long tick : new long[]{0, 0, 1, 2, 2, 3, 10, 10}) {
            long floor = BigInteger.valueOf(tick).multiply(numerator).divide(denominator).longValueExact();
            assertThat(floor).isGreaterThanOrEqualTo(previous);
            var interval = BenchmarkNominalCounterEnclosures.point(floor);
            BigInteger scaledTick = BigInteger.valueOf(tick).multiply(numerator);
            assertThat(BigInteger.valueOf(interval.lowerNanos()).multiply(denominator)).isLessThanOrEqualTo(scaledTick);
            assertThat(BigInteger.valueOf(interval.upperNanos()).multiply(denominator)).isGreaterThanOrEqualTo(scaledTick);
            previous = floor;
        }
        assertThat(BenchmarkNativeSourceAuthority.sourceRecipeEvidence()).containsEntry("siTimeQualified", false)
                .containsEntry("utcAccuracyQualified", false).containsEntry("wholeMethodCostQualified", false);
    }

    @Test void evidence_and_inspection_inputs_are_immutable_and_no_runtime_is_acquired() {
        var route = route(); var request = request(route, PATH, SOURCE, BINARY);
        route.put("timebaseDenom", 99L);
        BenchmarkNativeSourceAuthority.instance().requireInspectedDirectInvocationAndProviderContract(request);
        assertThatThrownBy(request.selectedRootRoute()::clear).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(BenchmarkNativeSourceAuthority.sourceRecipeEvidence()::clear).isInstanceOf(UnsupportedOperationException.class);
        assertThat(BenchmarkReturnWorkBudget.current()).isNull();
    }

    private static void check(Map<String, Object> route, List<String> path, String source, String binary) {
        BenchmarkNativeSourceAuthority.instance().requireInspectedDirectInvocationAndProviderContract(request(route, path, source, binary));
    }
    private static BenchmarkReturnMethodAdmission.InspectionRequest request(Map<String, Object> route, List<String> path, String source, String binary) {
        return new BenchmarkReturnMethodAdmission.InspectionRequest(BenchmarkReturnMethodAdmission.METHOD,
                BenchmarkReturnMethodAdmission.ProviderContract.MACH_ABSOLUTE_TIME_NOMINAL_MONOTONIC_EXCLUDING_SLEEP, path, source, binary, route);
    }
    private static LinkedHashMap<String, Object> route() {
        return new LinkedHashMap<>(Map.ofEntries(Map.entry("bootUuid", "00000000-0000-0000-0000-000000000000"),
                Map.entry("timebaseNumer", 125L), Map.entry("timebaseDenom", 3L),
                Map.entry("conversion", "UNSIGNED_128_MULTIPLY_INTEGER_DIVIDE_TRUNCATE"), Map.entry("loadedJniPath", "/controlled/native-clock.dylib"),
                Map.entry("loadedJniSha256", BINARY),
                Map.entry("classSha256", "a1bdb9f101060206e73b28fd521063bf59789f56fbd9ad78fe82ffed593e9e35"),
                Map.entry("classHashes", Map.of("PdkBenchmarkClock", "a1bdb9f101060206e73b28fd521063bf59789f56fbd9ad78fe82ffed593e9e35",
                        "PdkBenchmarkClock$Clock", "82c52a87d590e4ac400af03e51827a892d7988e27496883169f817fc83a89751",
                        "PdkBenchmarkClock$JniAccess", "61795d9670600c61b523c4611cbe8600b17e05c156ffebd582f08bd723a209c2",
                        "PdkBenchmarkClock$NativeAccess", "4b67c17ed28c1d341d9e27819563d1d89eb017073b8492f808e35da31f5ddfb7")),
                Map.entry("osFunction", "mach_absolute_time"), Map.entry("osImagePath", "/usr/lib/system/libsystem_kernel.dylib"),
                Map.entry("osImageUuid", "f63bf4188f7534a4aaf18caf2b9b6a05"), Map.entry("osFunctionImageOffsetUnsigned", "4236"),
                Map.entry("osFunctionCodePrefixBytes", 144L), Map.entry("osFunctionCodePrefixSha256", "c615a84440f8ae7325e75d236ace6e038838f5cf634f9bba912d2904c99ff2fd"),
                Map.entry("userTimebaseSelector", 3L), Map.entry("timebaseOffsetUnsigned", "17")));
    }
}
