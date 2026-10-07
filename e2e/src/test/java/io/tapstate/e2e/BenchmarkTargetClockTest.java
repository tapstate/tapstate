package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkTargetClockTest {
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
                .isInstanceOf(AssertionError.class).hasMessageContaining("clock stepped");
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
