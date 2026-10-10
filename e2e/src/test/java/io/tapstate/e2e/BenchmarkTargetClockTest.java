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
        assertThatThrownBy(() -> BenchmarkSteadyOutputWindow.readServerOperations(operationDatesInLogicalOrder))
                .isInstanceOf(AssertionError.class).hasMessageContaining("beyond clock uncertainty")
                .hasMessageContaining("uncertaintyMillis=2")
                .hasMessageContaining("clockCause=UNKNOWN")
                .hasMessageContaining("evidenceScope=OPERATION_DATE_ORDER_IN_LOGICAL_STREAM");
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
}
