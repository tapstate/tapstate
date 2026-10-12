package io.tapstate.e2e;

import java.util.AbstractList;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkAckCalibrationBudgetTest {
    @Test void fixed_source_decomposition_has_an_independent_eight_unit_numeric_witness() {
        var actual = BenchmarkReturnComponentSourceWork.ackCalibrationUpper();
        // Four local helper bodies; request witness includes every fixed map/guard API and the full
        // clock-sample/6000 input envelopes, independently of observed material or RPC time. Each
        // retained clock sample costs 18 requests, one record, 32 bytes and 146 elements.
        long samples = BenchmarkCausalClock.MAX_SAMPLES;
        var expected = new BenchmarkReturnWorkBudget.Work(4, 1110556 + 18 * samples, 12002 + samples, 0,
                7687512 + 32 * samples, 17, 9713084 + 146 * samples, 9713084 + 146 * samples);
        assertThat(actual).isEqualTo(expected);
        assertThat(BenchmarkReturnProducerOrigins.FLAGS).hasSize(18);
        var phase = BenchmarkReturnComponentSourceWork.upper(BenchmarkReturnComponentSourceWork.Kind.VERIFY_PHASE, limits());
        long calls = BenchmarkReturnLocalSourceWork.MAX_STORED_CALLS;
        long legacyRequests = 1024 + 8 * calls * calls + 64 * calls + 32L * samples + 32L * 960
                + 128L * 6000 + 4 * (64L * 8192 + 64L * 512 + 128) + 64L * (6 * 2 + 4) + 16;
        assertThat(phase.requests()).isEqualTo(legacyRequests + expected.requests());
        assertThat(BenchmarkReturnComponentSourceWork.ACK_FORMULA_SLOTS).isEqualTo(202);
        assertThatThrownBy(() -> actual.times(Long.MAX_VALUE)).isInstanceOf(ArithmeticException.class);
    }

    @Test void actual_pair_and_coverage_entries_count_nested_series_without_charging_upper_as_actual() {
        var readings = readings("owned");
        var definition = definition();
        try (var scope = BenchmarkReturnWorkBudget.activate(definition, "ack-helper-entries")) {
            scope.bindRoot(17, 23);
            var pair = BenchmarkTargetClock.validateAckPair(readings.getFirst(), readings.getLast());
            var hello = BenchmarkTargetClock.verifyAckHello(readings, 2000000, 398000000);
            assertThat(pair).containsEntry("state", "ACK_HELLO_PAIR_IDENTITY_AND_ORDER_VERIFIED");
            assertThat(hello).containsEntry("state", "ACK_HELLO_IDENTITY_ORDER_AND_COVERAGE_VERIFIED");
            assertThat(pair).hasSize(24); assertThat(hello).hasSize(31);
            assertThat(readings.getFirst().evidence()).hasSize(9);
            var row = activity(scope.snapshot());
            assertThat(row).containsEntry("attempts", 3L).containsEntry("completedAttempts", 3L)
                    .containsEntry("failedAttempts", 0L).containsEntry("issuedRequests", 0L);
            assertThat(object(row.get("actual"))).containsEntry("records", 0L).containsEntry("bytes", 0L);
            assertThat(scope.snapshot()).containsEntry("state", "OPEN").containsEntry("pendingOperations", 0L);
            for (String flag : BenchmarkReturnProducerOrigins.FLAGS) {
                assertThat(pair.get(flag)).isEqualTo(false); assertThat(hello.get(flag)).isEqualTo(false);
            }
        }
    }

    @Test void the_actual_prior_roster_guard_refuses_before_traversal_or_material_copy() {
        var gets = new AtomicInteger();
        List<BenchmarkTargetClock.Reading> hostile = new AbstractList<>() {
            @Override public int size() { return 6001; }
            @Override public BenchmarkTargetClock.Reading get(int index) {
                gets.incrementAndGet(); throw new AssertionError("refused roster was traversed");
            }
        };
        try (var scope = BenchmarkReturnWorkBudget.activate(definition(), "ack-roster-refusal")) {
            assertThatThrownBy(() -> BenchmarkTargetClock.validateAckSeries(hostile))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("bounded interior samples");
            assertThat(gets).hasValue(0);
            assertThat(activity(scope.snapshot())).containsEntry("attempts", 1L).containsEntry("failedAttempts", 1L);
            assertThat(scope.snapshot()).containsEntry("state", "UNKNOWN").containsEntry("pendingOperations", 0L);
        }
    }

    @Test void an_actual_expanded_hello_row_fits_its_own_prior_shape_and_utf8_material() {
        // A maximal guarded scalar distinguishes UTF-16 character bounds from UTF-8 bytes;
        // only three readings are needed to exercise both full comparison maps.
        var hello = BenchmarkTargetClock.verifyAckHello(readings("\u0800".repeat(65536)), 2000000, 398000000);
        var row = object(((List<?>) hello.get("readings")).get(1));
        var part = parts("stateless").get("METHOD_SERIAL_HELLO_READINGS");
        var plan = new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(part.nodesPerRow(), part.localDepthUpper(),
                part.leafUtf8Upper(), part.stringsPerRow(), 128, 24);
        var frozen = BenchmarkReturnWorkBudgetDefinitions.freezeOutputTree(row, plan);
        assertThat(row).hasSize(11);
        assertThat(object(row.get("adjacentWallComparison"))).hasSize(9);
        assertThat(object(row.get("firstToEachWallComparison"))).hasSize(9);
        assertThat(frozen.work().nodes()).isLessThanOrEqualTo(part.nodesPerRow());
        assertThat(frozen.work().stringUtf8Bytes()).isGreaterThan(65536).isLessThanOrEqualTo(part.stringsPerRow());
        assertThat(part.nodesPerRow()).isEqualTo(61);
        assertThat(part.stringsPerRow()).isEqualTo(266592);
        assertThatThrownBy(() -> object(frozen.value()).put("state", "QUALIFIED"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void every_original_workload_keeps_raw_readings_and_each_new_projection_separate() {
        for (String workload : List.of("copy", "stateless", "stateful")) {
            long phases = workload.equals("stateful") ? 2 : 1;
            var parts = parts(workload);
            assertThat(parts.get("METHOD_SERIAL_MAPPING_CALIBRATION").rows()).isEqualTo(phases);
            assertThat(parts.get("METHOD_SERIAL_MAPPING_CALIBRATION").nodesPerRow()).isEqualTo(113);
            assertThat(parts.get("METHOD_SERIAL_MAPPING_CALIBRATION").stringsPerRow()).isEqualTo(33664);
            assertThat(parts.get("METHOD_SERIAL_HELLO_READINGS").rows()).isEqualTo(phases * 6000);
            assertThat(parts.get("METHOD_SERIAL_HELLO_HEADER").nodesPerRow()).isEqualTo(64);
            assertThat(parts.get("METHOD_SERIAL_HELLO_HEADER").stringsPerRow()).isEqualTo(7936);
            assertThat(parts.get("ACK_TARGET_HELLO_CONTEXT_READINGS").rows()).isEqualTo(2 * phases * 6000);
            assertThat(parts.get("ACK_TARGET_HELLO_CONTEXT_HEADERS").rows()).isEqualTo(2 * phases);
            assertThat(parts.get("ACK_TARGET_HELLO_OUTER_PAIR").rows()).isEqualTo(phases);
            assertThat(parts.get("ACK_TARGET_HELLO_OUTER_PAIR").nodesPerRow()).isEqualTo(106);
            assertThat(parts.get("RETURN_SERIAL_HELLO_RAW").nodesPerRow()).isEqualTo(19);
            assertThat(parts.get("LEGACY_TARGET_HELLO_TIMELINE").nodesPerRow()).isEqualTo(19);
        }
    }

    @Test void the_new_catalogue_and_existing_two_factory_closures_scale_from_source_rows() {
        var one = BenchmarkReturnWorkBudgetReportPlan.returnCatalogueUpper(1);
        assertThat(one.records()).isEqualTo(23);
        assertThat(one.requests()).isEqualTo(3216);
        assertThat(BenchmarkReturnWorkBudgetReportPlan.returnCatalogueUpper(2)).isEqualTo(one.times(2));
        var prepared = BenchmarkReturnWorkBudgetReportPlan.prepareWithReturnComponents(
                BenchmarkReturnWorkBudgetProfile.WorkloadInput.original("stateless"), false);
        assertThat(prepared.components()).hasSizeLessThanOrEqualTo(64);
        assertThat(prepared.metadata()).containsEntry("wholeMethodCostQualified", false)
                .containsEntry("performanceAcceptanceEligible", false).containsEntry("actualSizeUsedAsPriorBound", false);
        assertThatThrownBy(() -> BenchmarkReturnWorkBudgetReportPlan.returnCatalogueUpper(Long.MAX_VALUE))
                .isInstanceOf(ArithmeticException.class);
    }

    @Test void scalar_material_refusal_precedes_numeric_diagnostic_or_copy() {
        var reading = new BenchmarkTargetClock.Reading("x".repeat(65537), "0".repeat(24),
                Long.MIN_VALUE, 0, 1, Long.MAX_VALUE, Long.MIN_VALUE);
        assertThatThrownBy(() -> BenchmarkTargetClock.validateAckPair(reading, readings("owned").getLast()))
                .isInstanceOf(AssertionError.class).hasMessageContaining("bounded raw shape");
    }

    private static List<BenchmarkTargetClock.Reading> readings(String primary) {
        return List.of(new BenchmarkTargetClock.Reading(primary, "0".repeat(24), 1000, 0, 1000000, 1000, 1001),
                new BenchmarkTargetClock.Reading(primary, "0".repeat(24), 1400, 200000000, 201000000, 1400, 1401),
                new BenchmarkTargetClock.Reading(primary, "0".repeat(24), 1600, 400000000, 401000000, 1600, 1601));
    }
    private static Map<String, BenchmarkReturnWorkBudgetReportPlan.Component> parts(String workload) {
        var prepared = BenchmarkReturnWorkBudgetReportPlan.prepareWithReturnComponents(
                BenchmarkReturnWorkBudgetProfile.WorkloadInput.original(workload), false);
        var result = new LinkedHashMap<String, BenchmarkReturnWorkBudgetReportPlan.Component>();
        for (var part : prepared.components()) result.put(part.id(), part);
        return Map.copyOf(result);
    }
    private static BenchmarkReturnComponentSourceWork.Limits limits() {
        return new BenchmarkReturnComponentSourceWork.Limits(BenchmarkReturnWorkBudgetBindings.SourceRole.values().length,
                new BenchmarkReturnWorkBudgetDefinitions.OutputPlan(32768, 16, 8192, 65536, 512, 64));
    }
    private static BenchmarkReturnWorkBudget.Definition definition() {
        var upper = new EnumMap<BenchmarkReturnWorkBudget.Activity, BenchmarkReturnWorkBudget.Work>(BenchmarkReturnWorkBudget.Activity.class);
        upper.put(BenchmarkReturnWorkBudget.Activity.ASSOCIATION, BenchmarkReturnComponentSourceWork.ackCalibrationUpper());
        var excluded = EnumSet.allOf(BenchmarkReturnWorkBudget.Activity.class); excluded.removeAll(upper.keySet());
        return new BenchmarkReturnWorkBudget.Definition("ack-calibration-source-controls", "a".repeat(64), Map.of(), upper, excluded);
    }
    private static Map<String, Object> activity(Map<String, Object> snapshot) {
        return object(object(snapshot.get("activities")).get("ASSOCIATION"));
    }
    @SuppressWarnings("unchecked") private static Map<String, Object> object(Object value) {
        return (Map<String, Object>) value;
    }
}
