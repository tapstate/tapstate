package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkTargetClockTest {
    @Test void stableInteriorSamplesRetainTheirCadenceAndRejectAMissingCoverageInterval() {
        var samples = java.util.List.of(reading("one", 1_000, 0, 2), reading("one", 1_200, 199, 202),
                reading("one", 1_400, 399, 402));
        assertThat(BenchmarkTargetClock.validateSeries(samples)).containsEntry("state", "QUALIFIED_SAMPLED_INTERIOR")
                .containsEntry("sampleIntervalMillis", 200).containsEntry("samples", 3);
        assertThatThrownBy(() -> BenchmarkTargetClock.validateSeries(java.util.List.of(samples.getFirst(), samples.getLast())))
                .isInstanceOf(AssertionError.class).hasMessageContaining("bounded interior samples");
        assertThatThrownBy(() -> BenchmarkTargetClock.validateSeries(java.util.List.of(samples.getFirst(), samples.get(1),
                reading("one", 2_000, 999, 1002))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("sampling gap");
    }
    @Test void anInteriorMonotonicClockDistortionCannotBeHiddenByItsFinalForwardCorrection() {
        var samples = java.util.List.of(reading("one", 1_000, 0, 2), reading("one", 1_100, 99, 102),
                reading("one", 1_150, 199, 202), reading("one", 1_300, 299, 302));
        assertThat(BenchmarkTargetClock.validate(samples.getFirst(), samples.getLast())).containsEntry("state", "QUALIFIED");
        assertThatThrownBy(() -> BenchmarkTargetClock.validateSeries(samples))
                .isInstanceOf(AssertionError.class).hasMessageContaining("clock stepped");
    }
    @Test void sampleResolutionAllowsTheInclusiveBoundButNotAccumulatingRollbacks() {
        var samples = new BenchmarkTargetClock.WallSamples();
        assertThat(samples.accept(2_000L)).isTrue();
        assertThat(samples.accept(null)).isFalse();
        assertThat(samples.accept(1_999L)).isTrue();
        assertThat(samples.accept(1_998L)).isTrue();
        assertThat(samples.accept(1_997L)).isFalse();
        assertThat(samples.highWaterMillis()).isEqualTo(2_000L);
        assertThat(samples.accept(2_001L)).isTrue();
        assertThat(samples.accept(1_998L)).isFalse();
    }
    @Test void qualifiedSerialClockReadsDoNotResolveConcurrentOperationDateOrdering() {
        var clocks = java.util.List.of(reading("one", 1_000, 0, 2), reading("one", 1_200, 199, 202),
                reading("one", 1_400, 399, 402));
        assertThat(BenchmarkTargetClock.validateSeries(clocks))
                .containsEntry("state", "QUALIFIED_SAMPLED_INTERIOR");
        // An earlier date sampler may acquire a later logical slot after another operation.
        // This synthetic ordering does not identify the cause of an actual server refusal.
        var operationDatesInLogicalOrder = java.util.List.of(1_010L, 1_006L);
        var dates = new BenchmarkTargetClock.WallSamples();
        assertThat(dates.accept(operationDatesInLogicalOrder.getFirst())).isTrue();
        assertThat(dates.accept(operationDatesInLogicalOrder.getLast())).isFalse();
        assertThatThrownBy(() -> BenchmarkSteadyOutputWindow.validateLegacyWallSamples(operationDatesInLogicalOrder))
                .isInstanceOf(AssertionError.class).hasMessageContaining("beyond clock uncertainty")
                .hasMessageContaining("uncertaintyMillis=2")
                .hasMessageContaining("clockCause=UNKNOWN")
                .hasMessageContaining("evidenceScope=OPERATION_DATE_ORDER_IN_LOGICAL_STREAM");
    }
    @Test void ordinaryDateAdmissionRetainsLogicalIndependenceWhileLegacyEvidenceRetainsItsRefusal() {
        var ordinary = new BenchmarkTargetClock.WallSamples();
        assertThat(BenchmarkTargetClock.rejectOperationDate(1_010L, ordinary, false)).isFalse();
        assertThat(BenchmarkTargetClock.rejectOperationDate(1_006L, ordinary, false)).isFalse();
        assertThat(BenchmarkTargetClock.rejectOperationDate(null, ordinary, false)).isTrue();
        var legacy = new BenchmarkTargetClock.WallSamples();
        assertThat(BenchmarkTargetClock.rejectOperationDate(1_010L, legacy, true)).isFalse();
        assertThat(BenchmarkTargetClock.rejectOperationDate(1_006L, legacy, true)).isTrue();
    }
    @Test void missingOperationDateCannotIdentifyAClockFailure() {
        assertThatThrownBy(() -> BenchmarkSteadyOutputWindow.readServerOperations(java.util.Arrays.asList(1_000L, null)))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("clockCause=UNKNOWN")
                .hasMessageContaining("evidenceScope=OPERATION_DATE_ORDER_IN_LOGICAL_STREAM");
    }
    private static BenchmarkTargetClock.Reading reading(String process, long wall, long start, long end) {
        return new BenchmarkTargetClock.Reading("owned:27017", process, wall, start*1_000_000, end*1_000_000, wall-1, wall+1);
    }
    @Test void samePrimaryStableClockRemainsInsideRequestAndResolutionUncertainty() {
        assertThat(BenchmarkTargetClock.validate(reading("one",1000,0,2),reading("one",1100,99,102)))
                .containsEntry("state","QUALIFIED").containsEntry("endpointResolutionErrorMillis",2);
    }
    @Test void primaryChangeAndClockStepCannotBeCalledCalibrated() {
        assertThatThrownBy(()->BenchmarkTargetClock.validate(reading("one",1000,0,2),reading("two",1100,99,102)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("primary changed");
        assertThatThrownBy(()->BenchmarkTargetClock.validate(reading("one",1000,0,2),reading("one",1400,99,102)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("clock stepped")
                .hasMessageContaining("serverElapsedMillis=400")
                .hasMessageContaining("minimumElapsedMillis=97")
                .hasMessageContaining("maximumElapsedMillis=102")
                .hasMessageContaining("processId=one");
    }
    @Test void operationResourceWindowRetainsEndpointUncertaintyAndRejectsUnbracketedTimes() {
        var before=reading("one",1000,0,2); var after=reading("one",1100,99,102);
        var window=BenchmarkTargetClock.mapWindow(before,after,1020,1080);
        assertThat(window.latestStartNanos()).isGreaterThan(window.earliestStartNanos());
        assertThat(window.earliestEndNanos()).isGreaterThan(window.latestStartNanos());
        assertThatThrownBy(()->BenchmarkTargetClock.mapWindow(before,after,900,1080))
                .isInstanceOf(AssertionError.class).hasMessageContaining("outside");
    }
    @Test void targetCollectionsMustShareTheActualPrimaryProcessClock() {
        BenchmarkTargetClock.requireSharedClock(java.util.List.of(reading("one",1000,0,2),reading("one",1002,2,4)));
        assertThatThrownBy(()->BenchmarkTargetClock.requireSharedClock(java.util.List.of(reading("one",1000,0,2),reading("two",1002,2,4))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("same actual primary");
    }

    @Test void ack_outer_pair_retains_a_wall_refusal_without_using_it_as_nominal_mapping_calibration() {
        // These numeric observations exercise the historical refusal; this control does not rerate its run.
        var before = new BenchmarkTargetClock.Reading("owned:27017", "one", 1_791_754_866_975L,
                914_149_522_441_625L, 914_149_523_234_000L, 1_791_754_866_978L, 1_791_754_866_979L);
        var after = new BenchmarkTargetClock.Reading("owned:27017", "one", 1_791_754_874_531L,
                914_157_075_305_208L, 914_157_076_271_500L, 1_791_754_874_531L, 1_791_754_874_532L);
        assertThatThrownBy(() -> BenchmarkTargetClock.validate(before, after)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("maximumElapsedMillis=7553");
        var ack = BenchmarkTargetClock.validateAckPair(before, after);
        @SuppressWarnings("unchecked") var diagnostic = (java.util.Map<String, Object>) ack.get("wallComparison");
        assertThat(diagnostic).containsEntry("state", "LEGACY_WALL_COMPARISON_REFUSED")
                .containsEntry("rootElapsedUpperNominalNanos", 7_553_829_875L)
                .containsEntry("serverElapsedMillis", 7_556L).containsEntry("hostWallOuterUpperMillis", 7_554L);
        assertThat(ack).containsEntry("state", "ACK_HELLO_PAIR_IDENTITY_AND_ORDER_VERIFIED");
        for (String flag : BenchmarkReturnProducerOrigins.FLAGS) { assertThat(ack).containsEntry(flag, false); }
    }

    @Test void ack_outer_pairs_do_not_apply_the_interior_sampling_gap_limit() {
        var before = reading("one", 1_000, 0, 2);
        var after = reading("one", 2_000, 999, 1002);
        assertThat(BenchmarkTargetClock.validateAckPair(before, after))
                .containsEntry("state", "ACK_HELLO_PAIR_IDENTITY_AND_ORDER_VERIFIED");
        assertThatThrownBy(() -> BenchmarkTargetClock.validateAckSeries(java.util.List.of(before, after, reading("one", 3_000, 1999, 2002))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("sampling gap");
    }

    @Test void ack_series_keeps_first_to_each_wall_diagnostics_even_when_adjacent_pairs_pass() {
        var samples = java.util.List.of(reading("one", 1_000, 0, 2), reading("one", 1_204, 199, 202),
                reading("one", 1_408, 399, 402));
        assertThat(BenchmarkTargetClock.validateSeries(samples)).containsEntry("state", "QUALIFIED_SAMPLED_INTERIOR");
        assertThatThrownBy(() -> BenchmarkTargetClock.validate(samples.getFirst(), samples.getLast()))
                .isInstanceOf(AssertionError.class);
        var ack = BenchmarkTargetClock.verifyAckHello(samples, 10_000_000L, 398_000_000L);
        assertThat(ack).containsEntry("adjacentWallRefusals", 0L).containsEntry("firstToEachWallRefusals", 1L)
                .containsEntry("sampleIntervalMillis", 200).containsEntry("maximumObservedGapNominalNanos", 197_000_000L)
                .containsEntry("state", "ACK_HELLO_IDENTITY_ORDER_AND_COVERAGE_VERIFIED");
        for (String flag : BenchmarkReturnProducerOrigins.FLAGS) { assertThat(ack).containsEntry(flag, false); }
        assertThatThrownBy(() -> ack.put("performanceAcceptanceEligible", true)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void actual_utc_reversal_is_retained_for_ack_but_legacy_validation_still_refuses_it() {
        var first = new BenchmarkTargetClock.Reading("owned:27017", "one", 1_000, 0, 2_000_000, 1_000, 999);
        var samples = java.util.List.of(first, reading("one", 1_200, 199, 202), reading("one", 1_400, 399, 402));
        assertThatThrownBy(() -> BenchmarkTargetClock.validateSeries(samples)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("ordered actual identity");
        var ack = BenchmarkTargetClock.validateAckSeries(samples);
        assertThat(ack).containsEntry("adjacentWallRefusals", 1L).containsEntry("firstToEachWallRefusals", 2L);
        var rows = (java.util.List<?>) ack.get("readings");
        @SuppressWarnings("unchecked") var retained = (java.util.Map<String, Object>) rows.getFirst();
        assertThat(retained).containsEntry("localUtcBeforeMillis", 1_000L).containsEntry("localUtcAfterMillis", 999L);
    }

    @Test void ack_series_refuses_borrowed_identity_overlapping_requests_and_uncovered_phase_endpoints() {
        var first = reading("one", 1_000, 0, 2);
        var second = reading("one", 1_200, 199, 202);
        var last = reading("one", 1_400, 399, 402);
        assertThatThrownBy(() -> BenchmarkTargetClock.validateAckSeries(java.util.List.of(first, reading("two", 1_200, 199, 202), last)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("primary changed");
        assertThatThrownBy(() -> BenchmarkTargetClock.validateAckSeries(java.util.List.of(first, second, reading("one", 1_400, 201, 402))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("ordered serial requests");
        assertThatThrownBy(() -> BenchmarkTargetClock.validateAckSeries(java.util.List.of(first, last)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("bounded interior samples");
        for (long[] coverage : new long[][]{{1_999_999, 398_000_000}, {10_000_000, 399_000_001}, {10_000_000, 9_999_999}}) {
            assertThatThrownBy(() -> BenchmarkTargetClock.verifyAckHello(java.util.List.of(first, second, last), coverage[0], coverage[1]))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("coverage");
        }
    }

    @Test void ack_series_checks_the_exact_gap_and_prior_roster_limit_before_copying() {
        var first = reading("one", 1_000, 0, 2);
        var boundary = new BenchmarkTargetClock.Reading("owned:27017", "one", 1_502, 502_000_000, 503_000_000, 1_502, 1_503);
        var last = reading("one", 1_703, 703, 704);
        assertThat(BenchmarkTargetClock.validateAckSeries(java.util.List.of(first, boundary, last)))
                .containsEntry("maximumObservedGapNominalNanos", 500_000_000L);
        var beyond = new BenchmarkTargetClock.Reading("owned:27017", "one", 1_502, 502_000_001, 503_000_000, 1_502, 1_503);
        assertThatThrownBy(() -> BenchmarkTargetClock.validateAckSeries(java.util.List.of(first, beyond, last)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("sampling gap");
        var oversized = new java.util.AbstractList<BenchmarkTargetClock.Reading>() {
            @Override public int size() { return 6_001; }
            @Override public BenchmarkTargetClock.Reading get(int index) { throw new AssertionError("oversized roster was traversed"); }
        };
        assertThatThrownBy(() -> BenchmarkTargetClock.validateAckSeries(oversized)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("bounded interior samples");
        var wideIdentity = new BenchmarkTargetClock.Reading("x".repeat(65_537), "one", 1_000, 0, 2_000_000, 999, 1001);
        assertThatThrownBy(() -> BenchmarkTargetClock.validateAckPair(wideIdentity, boundary)).isInstanceOf(AssertionError.class)
                .hasMessageContaining("bounded raw shape");
        assertThatThrownBy(() -> BenchmarkTargetClock.validateAckSeries(java.util.List.of(wideIdentity, boundary, last)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("bounded raw shape");
    }

    @Test void wall_delta_overflow_remains_bounded_diagnostic_data_without_hiding_context_failure() {
        var before = new BenchmarkTargetClock.Reading("owned:27017", "one", Long.MIN_VALUE, 0, 2_000_000, Long.MIN_VALUE, Long.MIN_VALUE);
        var after = new BenchmarkTargetClock.Reading("owned:27017", "one", Long.MAX_VALUE, 199_000_000, 202_000_000, Long.MAX_VALUE, Long.MAX_VALUE);
        var ack = BenchmarkTargetClock.validateAckPair(before, after);
        @SuppressWarnings("unchecked") var comparison = (java.util.Map<String, Object>) ack.get("wallComparison");
        assertThat(comparison).containsEntry("state", "WALL_DELTA_ARITHMETIC_REFUSED")
                .containsEntry("failureType", "java.lang.ArithmeticException");
        assertThat(comparison).doesNotContainKeys("serverElapsedMillis", "failureMessage");
        assertThatThrownBy(() -> BenchmarkTargetClock.validate(before, after)).isInstanceOf(ArithmeticException.class);
        assertThatThrownBy(() -> BenchmarkTargetClock.validateAckPair(before,
                new BenchmarkTargetClock.Reading("other:27017", "one", 1_200, 199_000_000, 202_000_000, 1_199, 1_201)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("primary changed");
    }
}
