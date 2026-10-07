package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkSteadyOutputWindowTest {
    @Test void exactFivePercentTrendIsAcceptedButOneMoreEventIsRejected() {
        assertThat(BenchmarkSteadyOutputWindow.read(trendTimeline(10_500)).completedDeliveries())
                .isEqualTo(20_500);
        assertThatThrownBy(() -> BenchmarkSteadyOutputWindow.read(trendTimeline(10_501)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("trend");
    }

    @Test void missingOperationTimeCannotEraseAnAlreadyObservedClockValue() {
        var clock = new BenchmarkMongoDeliveryObserver.OperationClock();
        assertThat(clock.accept(2_000L)).isTrue();
        assertThat(clock.accept(null)).isTrue();
        assertThat(clock.accept(1_500L)).isFalse();
        assertThat(clock.accept(2_000L)).isTrue();
        assertThat(clock.accept(2_001L)).isTrue();
    }

    private static java.util.List<Long> trendTimeline(int late) {
        var times = new ArrayList<Long>();
        times.add(0L);
        for (int bin = 0; bin < 5; bin++) {
            for (int i = 0; i < 2_000; i++) { times.add(bin * 100L + 50); }
        }
        for (int bin = 5; bin < 10; bin++) {
            int count = late / 5 + (bin == 9 ? late % 5 : 0);
            for (int i = 0; i < count; i++) { times.add(bin == 9 && i == count - 1 ? 1_000L : bin * 100L + 50); }
        }
        return times;
    }

    @Test void fixedUniformCohortCountsItsOpenLowerBoundaryExactlyOnce() {
        var times = new ArrayList<Long>(); for (int i=0;i<12_000;i++) { times.add(i*1000L); }
        var reading = BenchmarkSteadyOutputWindow.read(times);
        assertThat(reading.completedDeliveries()).isEqualTo(11_999);
        assertThat(reading.recordsPerSecond()).isEqualTo(1_000_000);
        assertThat(reading.fixedBins().stream().mapToLong(Long::longValue).sum()).isEqualTo(11_999);
    }
    @Test void missingProgressOrAChangingRateCannotPassForSteadyOutput() {
        var empty = new ArrayList<Long>(); for(int i=0;i<12_000;i++){empty.add(i<6000?i*1000L:100_000_000L+i*1000L);}
        assertThatThrownBy(()->BenchmarkSteadyOutputWindow.read(empty)).isInstanceOf(AssertionError.class).hasMessageContaining("idle time bin");
        var changing = new ArrayList<Long>(); for(int i=0;i<12_000;i++){changing.add(i<6000?i*1000L:6_000_000L+(i-6000)*2000L);}
        assertThatThrownBy(()->BenchmarkSteadyOutputWindow.read(changing)).isInstanceOf(AssertionError.class).hasMessageContaining("trend");
    }
    @Test void delayedStreamArrivalCannotChangeTheSameTargetOperationRate() {
        var operations = new ArrayList<Long>();
        for (int i=0;i<12_000;i++) { operations.add(1_000_000L+i); }
        var observed = new ArrayList<Long>(); var delayed = new ArrayList<Long>();
        for (int i=0;i<12_000;i++) {
            observed.add(i*1_000_000L);
            delayed.add(i*1_000_000L+(i>=6000?400_000_000L:0));
        }
        var before = BenchmarkSteadyOutputWindow.readOperationCohort(operations, observed);
        var after = BenchmarkSteadyOutputWindow.readOperationCohort(operations, delayed);
        assertThat(after.recordsPerSecond()).isEqualTo(before.recordsPerSecond());
        var missing = new ArrayList<>(operations); missing.set(500, null);
        assertThatThrownBy(()->BenchmarkSteadyOutputWindow.readServerOperations(missing)).isInstanceOf(AssertionError.class).hasMessageContaining("unavailable");
        var backward = new ArrayList<>(operations); backward.set(500, operations.get(499)-1);
        assertThatThrownBy(()->BenchmarkSteadyOutputWindow.readServerOperations(backward)).isInstanceOf(AssertionError.class).hasMessageContaining("backward");
    }
    @Test void eachStreamMustBeOrderedBeforeAValidCrossStreamMerge() {
        assertThat(BenchmarkSteadyOutputWindow.mergeValidatedOperationStreams(java.util.List.of(java.util.List.of(1L,3L),java.util.List.of(2L,4L))))
                .containsExactly(1L,2L,3L,4L);
        assertThatThrownBy(()->BenchmarkSteadyOutputWindow.mergeValidatedOperationStreams(java.util.List.of(java.util.List.of(3L,1L),java.util.List.of(2L,4L))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("before stream merge");
    }
}
