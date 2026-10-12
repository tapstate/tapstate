package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkNativeNominalReturnClockTest {
    private static final String LIBRARY = BenchmarkNativeClockEvidenceTest.LIBRARY;
    private static final BenchmarkCausalClock.Identity OWNED = new BenchmarkCausalClock.Identity(17, 1000);
    private static final BenchmarkCausalClock.Identity ROOT = new BenchmarkCausalClock.Identity(29, 2000);

    @Test void unrounded_points_are_enclosed_for_all_three_actual_conversion_residues() {
        var clock = clock(receipt(), receipt(), samples());
        for (long ticks = 4; ticks <= 6; ticks++) {
            long exactScaled = ticks * 125, floor = exactScaled / 3;
            var point = clock.map(OWNED, floor);
            assertThat(point.lowerNanos() * 3).isLessThanOrEqualTo(exactScaled);
            assertThat(point.upperNanos() * 3).isGreaterThanOrEqualTo(exactScaled);
            assertThat(point.widthNanos()).isEqualTo(1);
        }
        assertThat(clock.map(OWNED, 166)).isEqualTo(new BenchmarkCausalClock.Interval(166, 167));
    }

    @Test void root_source_fraction_and_same_call_return_uncertainty_are_both_retained() {
        var clock = clock(receipt(), receipt(), samples());
        var source = clock.sourcePoint(41);
        var callback = clock.map(OWNED, 166);
        var observed = clock.map(OWNED, 208);
        var returned = new BenchmarkCausalClock.Interval(callback.lowerNanos(), observed.upperNanos());
        var latency = BenchmarkCausalClock.elapsed(source, returned);
        assertThat(source).isEqualTo(new BenchmarkCausalClock.Interval(41, 42));
        assertThat(returned).isEqualTo(new BenchmarkCausalClock.Interval(166, 209));
        assertThat(latency).isEqualTo(new BenchmarkCausalClock.Interval(124, 168));
        for (long returnedTicks = 4; returnedTicks <= 5; returnedTicks++) {
            long exactScaledLatency = (returnedTicks - 1) * 125;
            assertThat(latency.lowerNanos() * 3).isLessThanOrEqualTo(exactScaledLatency);
            assertThat(latency.upperNanos() * 3).isGreaterThanOrEqualTo(exactScaledLatency);
        }
    }

    @Test void existing_causal_and_integer_native_source_defaults_do_not_gain_fractional_width() {
        var causal = new BenchmarkCausalClock(OWNED, samples());
        var integer = new BenchmarkNativeReturnClock(OWNED, ROOT, LIBRARY, samples(), receipt(), receipt());
        var nominal = clock(receipt(), receipt(), samples());
        assertThat(causal.map(OWNED, 166)).isEqualTo(new BenchmarkCausalClock.Interval(100, 210));
        assertThat(integer.map(OWNED, 166)).isEqualTo(new BenchmarkCausalClock.Interval(166, 166));
        for (BenchmarkReturnPointClock existing : List.of(causal, integer)) {
            assertThat(existing.sourcePoint(41)).isEqualTo(new BenchmarkCausalClock.Interval(41, 41));
            assertThat(existing.sourcePoint(-1)).isEqualTo(new BenchmarkCausalClock.Interval(-1, -1));
            assertThat(existing.sourcePoint(Long.MAX_VALUE)).isEqualTo(new BenchmarkCausalClock.Interval(Long.MAX_VALUE, Long.MAX_VALUE));
        }
        assertThat(nominal.sourcePoint(0)).isEqualTo(new BenchmarkCausalClock.Interval(0, 1));
        assertThatThrownBy(() -> nominal.sourcePoint(-1)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> nominal.sourcePoint(Long.MAX_VALUE)).isInstanceOf(AssertionError.class).hasMessageContaining("overflow");
    }

    @Test void fractional_widening_cannot_bypass_owned_identity_or_strict_serial_coverage() {
        var clock = clock(receipt(), receipt(), samples());
        assertThatThrownBy(() -> clock.map(new BenchmarkCausalClock.Identity(17, 1001), 166))
                .isInstanceOf(AssertionError.class).hasMessageContaining("identity");
        for (long point : new long[]{-1, 100, 105, 305, 310, Long.MAX_VALUE}) {
            assertThatThrownBy(() -> clock.map(OWNED, point)).isInstanceOf(AssertionError.class);
        }
        assertThatThrownBy(() -> clock(receipt(), receipt(), samples().subList(0, 1))).isInstanceOf(AssertionError.class);
        var outside = List.of(new BenchmarkCausalClock.Sample(0, OWNED, 100, 110, 150), samples().get(1));
        assertThatThrownBy(() -> clock(receipt(), receipt(), outside)).isInstanceOf(AssertionError.class).hasMessageContaining("root bracket");
    }

    @Test void actual_delegate_binding_checks_refuse_wrong_cold_identities_raw_bytes_and_libraries() {
        for (Consumer<Map<String, Object>> corrupt : List.<Consumer<Map<String, Object>>>of(
                map -> map.put("expectedPid", 18L), map -> map.put("expectedRootJvmStartTimeMillis", 2001L),
                map -> map.put("raw", "{}"), map -> map.put("ownedMetadata", Map.of()),
                map -> map.put("matchedNativeClockFlag", "-Dtapstate.benchmark.native-clock-library=/other.dylib"))) {
            var before = receipt(); corrupt.accept(before);
            assertThatThrownBy(() -> clock(before, receipt(), samples())).isInstanceOf(AssertionError.class);
        }
        assertThatThrownBy(() -> new BenchmarkNativeNominalReturnClock(OWNED, ROOT, "/other.dylib", samples(), receipt(), receipt()))
                .isInstanceOf(AssertionError.class);
    }

    @Test void matching_unsupported_routes_fallback_and_claimed_qualification_remain_refused() {
        for (Map.Entry<String, Object> change : List.of(
                Map.entry("loadedJniSha256", (Object) "0".repeat(64)),
                Map.entry("osImageUuid", (Object) "0".repeat(32)), Map.entry("userTimebaseSelector", (Object) 1L))) {
            var changed = receipt(snapshot -> snapshot.put(change.getKey(), change.getValue()), metadata -> { });
            assertThatThrownBy(() -> clock(changed, changed, samples())).isInstanceOf(AssertionError.class);
        }
        var changedBoot = receipt(snapshot -> snapshot.put("bootUuid", "11111111-1111-1111-1111-111111111111"), metadata -> { });
        assertThatThrownBy(() -> clock(receipt(), changedBoot, samples())).isInstanceOf(AssertionError.class);
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

    @Test void evidence_preserves_binding_conditions_and_limits_its_new_error_claim_to_floor_conversion() {
        var nominal = clock(receipt(), receipt(), samples()).evidence();
        var integer = new BenchmarkNativeReturnClock(OWNED, ROOT, LIBRARY, samples(), receipt(), receipt()).evidence();
        assertThat(nominal).containsEntry("state", "CONDITIONAL_UNROUNDED_NOMINAL_COUNTER")
                .containsEntry("fractionalFloorUpperErrorNominalNanos", 1L)
                .containsEntry("integerBindingState", integer.get("state"))
                .containsEntry("snapshot", integer.get("snapshot"))
                .containsEntry("conditions", integer.get("conditions"))
                .containsEntry("reviewedJniSourceSha256", integer.get("reviewedJniSourceSha256"));
        for (String flag : BenchmarkNativeClockEvidence.FLAGS) { assertThat(nominal).containsEntry(flag, false); }
        assertThatThrownBy(() -> nominal.put("clockQualified", true)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void serial_calibration_bounds_mapping_without_using_request_or_literal_return_width_as_error() {
        var retained = List.of(new BenchmarkCausalClock.Sample(0, OWNED, 0, 20_000_000, 10_000_000),
                new BenchmarkCausalClock.Sample(1, OWNED, 30_000_000, 50_000_000, 40_000_000),
                new BenchmarkCausalClock.Sample(2, OWNED, 60_000_000, 80_000_000, 70_000_000));
        var clock = clock(receipt(), receipt(), retained);
        var evidence = clock.serialCalibrationEvidence(retained);
        assertThat(evidence).containsEntry("mappingUncertaintyUpperNominalNanos", 1L)
                .containsEntry("mappingToleranceNominalNanos", 2_000_000L)
                .containsEntry("maximumActualRequestBracketNominalNanos", 20_000_000L)
                .containsEntry("rootPid", 29L).containsEntry("ownedPid", 17L);
        var lower = clock.map(OWNED, 20_000_000);
        var upper = clock.map(OWNED, 60_000_000);
        assertThat(new BenchmarkCausalClock.Interval(lower.lowerNanos(), upper.upperNanos()).widthNanos())
                .isEqualTo(40_000_001L).isGreaterThan(2_000_000L);
        for (String flag : BenchmarkReturnProducerOrigins.FLAGS) { assertThat(evidence).containsEntry(flag, false); }
        assertThatThrownBy(() -> evidence.put("mappingUncertaintyUpperNominalNanos", 0L))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void calibration_requires_the_exact_retained_capture_roster_and_refuses_borrowed_owners() {
        var retained = samples();
        var clock = clock(receipt(), receipt(), retained);
        var changed = List.of(retained.getFirst(), retained.get(1), new BenchmarkCausalClock.Sample(2, OWNED, 300, 310, 306));
        assertThatThrownBy(() -> clock.serialCalibrationEvidence(changed)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("capture sample roster differs");
        var borrowed = List.of(new BenchmarkCausalClock.Sample(0, ROOT, 100, 110, 105), retained.get(1), retained.getLast());
        assertThatThrownBy(() -> clock(receipt(), receipt(), borrowed)).isInstanceOf(AssertionError.class).hasMessageContaining("identity");
        assertThatThrownBy(() -> clock.serialCalibrationEvidence(null)).isInstanceOf(AssertionError.class);
    }

    @Test void serial_mapping_records_one_actual_entry_and_fits_its_prior_report_shape() {
        var retained = samples(); var clock = clock(receipt(), receipt(), retained);
        var upper = new java.util.EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        upper.put(BenchmarkReturnWorkBudget.Activity.ASSOCIATION, BenchmarkReturnComponentSourceWork.ackCalibrationUpper());
        var excluded = java.util.EnumSet.allOf(BenchmarkReturnWorkBudget.Activity.class); excluded.removeAll(upper.keySet());
        var definition = new BenchmarkReturnWorkBudget.Definition("mapping-entry-control", "a".repeat(64), Map.of(), upper, excluded);
        Map<String, Object> evidence;
        try (var scope = BenchmarkReturnWorkBudget.activate(definition, "mapping-entry")) {
            scope.bindRoot(ROOT.pid(), ROOT.jvmStartTimeMillis());
            evidence = clock.serialCalibrationEvidence(retained);
            @SuppressWarnings("unchecked") var activities = (Map<String, Object>) scope.snapshot().get("activities");
            @SuppressWarnings("unchecked") var mapping = (Map<String, Object>) activities.get("ASSOCIATION");
            assertThat(mapping).containsEntry("attempts", 1L).containsEntry("completedAttempts", 1L)
                    .containsEntry("issuedRequests", 0L).containsEntry("failedAttempts", 0L);
            assertThat(scope.snapshot()).containsEntry("state", "OPEN").containsEntry("pendingOperations", 0L);
        }
        var descriptor = BenchmarkReturnWorkBudgetReportPlan.prepareWithReturnComponents(
                BenchmarkReturnWorkBudgetProfile.WorkloadInput.original("stateless"), false).components().stream()
                .filter(part -> part.id().equals("METHOD_SERIAL_MAPPING_CALIBRATION")).findFirst().orElseThrow();
        var plan = new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(descriptor.nodesPerRow(), descriptor.localDepthUpper(),
                descriptor.leafUtf8Upper(), descriptor.stringsPerRow(), 128, 24);
        var frozen = BenchmarkReturnWorkBudgetDefinitions.freezeOutputTree(evidence, plan);
        assertThat(evidence).hasSize(33);
        assertThat(frozen.work().nodes()).isLessThanOrEqualTo(descriptor.nodesPerRow());
        assertThat(frozen.work().stringUtf8Bytes()).isLessThanOrEqualTo(descriptor.stringsPerRow());
    }

    @Test void two_millisecond_mapping_tolerance_does_not_relax_the_actual_integer_request_bracket() {
        for (long point : new long[]{99, 111}) {
            var outside = List.of(new BenchmarkCausalClock.Sample(0, OWNED, 100, 110, point), samples().get(1), samples().getLast());
            assertThatThrownBy(() -> clock(receipt(), receipt(), outside)).isInstanceOf(AssertionError.class)
                    .hasMessageContaining("root bracket");
        }
        var changedRoute = receipt(snapshot -> snapshot.put("timebaseDenom", 4L), metadata -> { });
        assertThatThrownBy(() -> clock(changedRoute, changedRoute, samples())).isInstanceOf(AssertionError.class);
    }

    @Test void calibration_refuses_an_unrepresentable_floor_successor_without_inventing_a_wider_mapping() {
        var nearLimit = List.of(new BenchmarkCausalClock.Sample(0, OWNED, Long.MAX_VALUE - 20, Long.MAX_VALUE - 10, Long.MAX_VALUE - 15),
                new BenchmarkCausalClock.Sample(1, OWNED, Long.MAX_VALUE - 5, Long.MAX_VALUE, Long.MAX_VALUE));
        var clock = clock(receipt(), receipt(), nearLimit);
        assertThatThrownBy(() -> clock.serialCalibrationEvidence(nearLimit)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("enclosure overflow");
    }

    @Test void expanded_complete_serial_mapping_uses_the_shared_bounded_sample_roster() {
        var retained = java.util.stream.LongStream.range(0, 600).mapToObj(index -> new BenchmarkCausalClock.Sample(
                index, OWNED, index * 1000, index * 1000 + 10, index * 1000 + 5)).toList();
        var evidence = clock(receipt(), receipt(), retained).serialCalibrationEvidence(retained);
        assertThat(evidence).containsEntry("samples", 600).containsEntry("mappingUncertaintyUpperNominalNanos", 1L);
        for (String flag : BenchmarkReturnProducerOrigins.FLAGS) assertThat(evidence).containsEntry(flag, false);
        var hostile = new java.util.AbstractList<BenchmarkCausalClock.Sample>() {
            @Override public int size() { return BenchmarkCausalClock.MAX_SAMPLES + 1; }
            @Override public BenchmarkCausalClock.Sample get(int index) { throw new AssertionError("oversized clock roster was traversed"); }
        };
        assertThatThrownBy(() -> clock(receipt(), receipt(), hostile)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("bounded");
    }

    private static BenchmarkNativeNominalReturnClock clock(Map<String, Object> before, Map<String, Object> after,
            List<BenchmarkCausalClock.Sample> samples) {
        return new BenchmarkNativeNominalReturnClock(OWNED, ROOT, LIBRARY, samples, before, after);
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
    /** Synthetic cold facts exercise parser and mapper behavior, never live clock qualification. */
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
