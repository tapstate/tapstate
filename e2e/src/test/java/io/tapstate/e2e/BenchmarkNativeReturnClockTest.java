package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkNativeReturnClockTest {
    private static final String LIBRARY = BenchmarkNativeClockEvidenceTest.LIBRARY;
    private static final BenchmarkCausalClock.Identity OWNED = new BenchmarkCausalClock.Identity(17, 1000);
    private static final BenchmarkCausalClock.Identity ROOT = new BenchmarkCausalClock.Identity(29, 2000);

    @Test void an_actual_shared_coordinate_keeps_the_logged_point_without_changing_causal_bounds() {
        var causal = new BenchmarkCausalClock(OWNED, samples());
        var nativeClock = clock(receipt(), receipt(), samples());
        assertThat(causal.map(OWNED, 150)).isEqualTo(new BenchmarkCausalClock.Interval(100, 210));
        assertThat(nativeClock.map(OWNED, 150)).isEqualTo(new BenchmarkCausalClock.Interval(150, 150));
        assertThat(causal.map(OWNED, 150)).isEqualTo(new BenchmarkCausalClock.Interval(100, 210));
        for (String flag : BenchmarkNativeClockEvidence.FLAGS) { assertThat(nativeClock.evidence()).containsEntry(flag, false); }
        assertThat(nativeClock.evidence()).containsEntry("state", "CONDITIONAL_COMMON_NATIVE_COUNTER_DIAGNOSTIC")
                .containsEntry("counterUnit", "nominal-ns");
    }

    @Test void rows_share_the_full_calls_variable_return_interval_and_never_a_commit_point() {
        var target = BenchmarkWorkloadDefinitions.byId("copy").phase("cdc-update").targets().getFirst();
        var one = new BenchmarkWriteReturnExpectations.Association(target, "1", 0, 100, 120,
                7, 1, "pdk.state.bench_copy.sink", "orders", 125, 150, 170, 2, true);
        var two = new BenchmarkWriteReturnExpectations.Association(target, "2", 1, 110, 120,
                7, 1, "pdk.state.bench_copy.sink", "orders", 125, 150, 170, 2, true);
        var mapped = BenchmarkReturnTimeBounds.map(List.of(one, two), OWNED, clock(receipt(), receipt(), samples()));
        assertThat(mapped).hasSize(2);
        assertThat(mapped.get(0).callSequence()).isEqualTo(7); assertThat(mapped.get(1).callSequence()).isEqualTo(7);
        assertThat(mapped.get(0).capturedPoint()).isEqualTo(new BenchmarkCausalClock.Interval(170, 170));
        assertThat(mapped.get(0).literalReturn()).isEqualTo(new BenchmarkCausalClock.Interval(150, 170));
        assertThat(mapped.get(1).literalReturn()).isEqualTo(mapped.get(0).literalReturn());
        assertThat(mapped.get(0).literalReturn().widthNanos()).isEqualTo(20);
        assertThat(mapped.get(0).latency()).isEqualTo(new BenchmarkCausalClock.Interval(50, 70));
        assertThat(mapped.get(1).latency()).isEqualTo(new BenchmarkCausalClock.Interval(40, 60));
    }

    @Test void matching_metadata_cannot_hide_an_owned_value_outside_its_root_request_bracket() {
        var unlike = List.of(new BenchmarkCausalClock.Sample(0, OWNED, 100, 110, 150),
                new BenchmarkCausalClock.Sample(1, OWNED, 200, 210, 205));
        assertThat(new BenchmarkCausalClock(OWNED, unlike).map(OWNED, 170))
                .isEqualTo(new BenchmarkCausalClock.Interval(100, 210));
        assertThatThrownBy(() -> clock(receipt(), receipt(), unlike)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("root bracket");
    }

    @Test void cold_proofs_must_bind_the_exact_distinct_runtime_library_and_original_raw_metadata() {
        for (Consumer<Map<String, Object>> corrupt : List.<Consumer<Map<String, Object>>>of(
                map -> map.put("expectedPid", 18L), map -> map.put("expectedJvmStartTimeMillis", 1001L),
                map -> map.put("expectedRootPid", 30L), map -> map.put("expectedRootJvmStartTimeMillis", 2001L),
                map -> map.put("pid", 18L), map -> map.put("jvmStartTimeMillis", 1001L),
                map -> map.put("rawRetained", false), map -> map.put("matchedRuntimeFlag", "-Dtapstate.benchmark.write-return=false"),
                map -> map.put("matchedNativeClockFlag", "-Dtapstate.benchmark.native-clock-library=/other.dylib"),
                map -> map.put("raw", "{}"), map -> map.put("ownedMetadata", Map.of()),
                map -> map.put("parsedRootMetadata", Map.of()),
                map -> map.put("rootMetadata", Map.of("rawRetained", false, "raw", "{}")))) {
            var before = receipt(); corrupt.accept(before);
            assertThatThrownBy(() -> clock(before, receipt(), samples())).isInstanceOf(AssertionError.class);
        }
        assertThatThrownBy(() -> new BenchmarkNativeReturnClock(OWNED, ROOT, "/other.dylib", samples(), receipt(), receipt()))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> new BenchmarkNativeReturnClock(OWNED, OWNED, LIBRARY, samples(), receipt(), receipt()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("distinct");
    }

    @Test void equality_across_all_cold_snapshots_does_not_admit_an_unreviewed_binary_or_route() {
        for (Map.Entry<String, Object> change : List.of(
                Map.entry("loadedJniSha256", (Object) "0".repeat(64)),
                Map.entry("classHashes", (Object) Map.of("PdkBenchmarkClock", "42fffc3e10a70ae503e85c909cac3d5f437c07c2310115a59db875124c53b1f2",
                        "PdkBenchmarkClock$Clock", "0".repeat(64),
                        "PdkBenchmarkClock$JniAccess", "2ccedc46ae98b4ea4e6c79c112a53fc3953efc3c344d2f3e6ba1e4d78eabaeb2",
                        "PdkBenchmarkClock$NativeAccess", "4b67c17ed28c1d341d9e27819563d1d89eb017073b8492f808e35da31f5ddfb7")),
                Map.entry("osImageUuid", (Object) "0".repeat(32)),
                Map.entry("osImagePath", (Object) "/other/system.dylib"),
                Map.entry("osFunctionImageOffsetUnsigned", (Object) "4237"),
                Map.entry("osFunctionCodePrefixSha256", (Object) "0".repeat(64)),
                Map.entry("osFunctionCodePrefixBytes", (Object) 143L),
                Map.entry("userTimebaseSelector", (Object) 1L), Map.entry("timebaseNumer", (Object) 1L))) {
            var changed = receipt(snapshot -> snapshot.put(change.getKey(), change.getValue()), metadata -> { });
            assertThatThrownBy(() -> clock(changed, changed, samples())).isInstanceOf(AssertionError.class);
        }
    }

    @Test void changed_boot_offset_scale_fallback_or_qualification_claims_refuse_mapping() {
        for (Map.Entry<String, Object> change : List.of(
                Map.entry("bootUuid", (Object) "11111111-1111-1111-1111-111111111111"),
                Map.entry("timebaseOffsetUnsigned", (Object) "18"), Map.entry("timebaseDenom", (Object) 4L))) {
            var changed = receipt(snapshot -> snapshot.put(change.getKey(), change.getValue()), metadata -> { });
            assertThatThrownBy(() -> clock(receipt(), changed, samples())).isInstanceOf(AssertionError.class);
        }
        var fallback = receipt(snapshot -> { }, metadata -> {
            metadata.put("state", "UNKNOWN"); metadata.put("reason", "NATIVE_READ_UNAVAILABLE");
            metadata.put("failureType", "java.lang.ArithmeticException"); metadata.put("activeProvider", "SYSTEM_NANO_TIME");
            metadata.put("mixedDomainPossible", true);
        });
        assertThatThrownBy(() -> clock(fallback, fallback, samples())).isInstanceOf(AssertionError.class);
        for (String flag : BenchmarkNativeClockEvidence.FLAGS) {
            var claimed = receipt(snapshot -> { }, metadata -> metadata.put(flag, true));
            assertThatThrownBy(() -> clock(claimed, claimed, samples())).isInstanceOf(AssertionError.class);
        }
    }

    @Test void strict_coverage_negative_native_points_and_overflow_still_fail_closed() {
        var nativeClock = clock(receipt(), receipt(), samples());
        for (long point : new long[]{-1, 100, 105, 305, 310, Long.MAX_VALUE}) {
            assertThatThrownBy(() -> nativeClock.map(OWNED, point)).isInstanceOf(AssertionError.class);
        }
        assertThatThrownBy(() -> nativeClock.map(new BenchmarkCausalClock.Identity(17, 1001), 150))
                .isInstanceOf(AssertionError.class).hasMessageContaining("identity");
        assertThatThrownBy(() -> clock(receipt(), receipt(), samples().subList(0, 1))).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> clock(receipt(), receipt(), List.of(
                new BenchmarkCausalClock.Sample(0, OWNED, -1, 110, 105), samples().get(1))))
                .isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> clock(receipt(), receipt(), List.of(
                new BenchmarkCausalClock.Sample(0, OWNED, Long.MIN_VALUE, Long.MIN_VALUE + 1, 105),
                new BenchmarkCausalClock.Sample(1, OWNED, Long.MAX_VALUE - 1, Long.MAX_VALUE, 205))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("overflow");
        var target = BenchmarkWorkloadDefinitions.byId("copy").phase("cdc-update").targets().getFirst();
        var invalid = new BenchmarkWriteReturnExpectations.Association(target, "1", 0, Long.MIN_VALUE, 100,
                7, 1, "pdk.state.bench_copy.sink", "orders", 125, 150, 170, 2, true);
        assertThatThrownBy(() -> BenchmarkReturnTimeBounds.map(List.of(invalid), OWNED, nativeClock))
                .isInstanceOf(AssertionError.class).hasMessageContaining("overflow");
    }

    private static BenchmarkNativeReturnClock clock(Map<String, Object> before, Map<String, Object> after,
            List<BenchmarkCausalClock.Sample> samples) {
        return new BenchmarkNativeReturnClock(OWNED, ROOT, LIBRARY, samples, before, after);
    }
    private static List<BenchmarkCausalClock.Sample> samples() {
        return List.of(new BenchmarkCausalClock.Sample(0, OWNED, 100, 110, 105),
                new BenchmarkCausalClock.Sample(1, OWNED, 200, 210, 205),
                new BenchmarkCausalClock.Sample(2, OWNED, 300, 310, 305));
    }
    private static Map<String, Object> receipt() { return receipt(snapshot -> { }, metadata -> { }); }
    private static Map<String, Object> receipt(Consumer<Map<String, Object>> changeSnapshot, Consumer<Map<String, Object>> changeMetadata) {
        var owned = metadata(OWNED, changeSnapshot); var root = metadata(ROOT, changeSnapshot);
        changeMetadata.accept(owned); changeMetadata.accept(root);
        var result = new LinkedHashMap<String, Object>(owned);
        result.put("rawRetained", true); result.put("raw", JsonWriter.write(owned));
        result.put("expectedPid", OWNED.pid()); result.put("expectedJvmStartTimeMillis", OWNED.jvmStartTimeMillis());
        result.put("expectedRootPid", ROOT.pid()); result.put("expectedRootJvmStartTimeMillis", ROOT.jvmStartTimeMillis());
        result.put("matchedRuntimeFlag", "-Dtapstate.benchmark.write-return=true");
        result.put("matchedNativeClockFlag", "-Dtapstate.benchmark.native-clock-library=" + LIBRARY);
        result.put("ownedMetadata", owned); result.put("parsedRootMetadata", root);
        result.put("rootMetadata", Map.of("rawRetained", true, "raw", JsonWriter.write(root)));
        return result;
    }
    private static Map<String, Object> metadata(BenchmarkCausalClock.Identity owner, Consumer<Map<String, Object>> change) {
        var result = BenchmarkNativeClockEvidenceTest.metadataMap(owner);
        var snapshot = new LinkedHashMap<String, Object>();
        snapshot.put("bootUuid", "00000000-0000-0000-0000-000000000000"); snapshot.put("timebaseNumer", 125L); snapshot.put("timebaseDenom", 3L);
        snapshot.put("conversion", "UNSIGNED_128_MULTIPLY_INTEGER_DIVIDE_TRUNCATE"); snapshot.put("loadedJniPath", LIBRARY);
        snapshot.put("loadedJniSha256", "4ce1204977af372080e3a3a8ada2c03ce678a15b337e3201f13d30b22a4700c9");
        snapshot.put("classSha256", "42fffc3e10a70ae503e85c909cac3d5f437c07c2310115a59db875124c53b1f2");
        snapshot.put("classHashes", Map.of("PdkBenchmarkClock", "42fffc3e10a70ae503e85c909cac3d5f437c07c2310115a59db875124c53b1f2",
                "PdkBenchmarkClock$Clock", "82c52a87d590e4ac400af03e51827a892d7988e27496883169f817fc83a89751",
                "PdkBenchmarkClock$JniAccess", "2ccedc46ae98b4ea4e6c79c112a53fc3953efc3c344d2f3e6ba1e4d78eabaeb2",
                "PdkBenchmarkClock$NativeAccess", "4b67c17ed28c1d341d9e27819563d1d89eb017073b8492f808e35da31f5ddfb7"));
        snapshot.put("osFunction", "mach_absolute_time"); snapshot.put("osImagePath", "/usr/lib/system/libsystem_kernel.dylib");
        snapshot.put("osImageUuid", "f63bf4188f7534a4aaf18caf2b9b6a05"); snapshot.put("osFunctionImageOffsetUnsigned", "4236");
        snapshot.put("osFunctionCodePrefixBytes", 144L);
        snapshot.put("osFunctionCodePrefixSha256", "c615a84440f8ae7325e75d236ace6e038838f5cf634f9bba912d2904c99ff2fd");
        snapshot.put("userTimebaseSelector", 3L); snapshot.put("timebaseOffsetUnsigned", "17");
        change.accept(snapshot); result.put("before", snapshot); result.put("after", snapshot); return result;
    }
}
