package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkSteadyOutputWindowTest {
    @Test void ordinaryConcurrentOperationsKeepTheirExactTemporalCohortDespiteAdjacentWallTimeInversions() {
        var ordered = new ArrayList<Long>();
        for (int i = 0; i < 12_000; i++) { ordered.add(1_000_000L + i); }
        var nativeOrder = new ArrayList<>(ordered);
        java.util.Collections.swap(nativeOrder, 0, 1);
        java.util.Collections.swap(nativeOrder, 499, 500);
        java.util.Collections.swap(nativeOrder, 11_998, 11_999);
        assertThat(BenchmarkSteadyOutputWindow.readServerOperations(nativeOrder))
                .as("logical operation order may interleave independently sampled wall dates by one millisecond")
                .isEqualTo(BenchmarkSteadyOutputWindow.readServerOperations(ordered));
    }

    @Test void fixedCommonOperationBoundsUseEveryOriginalTimestampRatherThanTheLogicalStreamEndpoints() {
        var ordered = new ArrayList<Long>();
        for (int i = 0; i < 12_000; i++) { ordered.add(1_000_000L + i); }
        var nativeOrder = new ArrayList<>(ordered);
        java.util.Collections.swap(nativeOrder, 0, 1);
        java.util.Collections.swap(nativeOrder, 11_998, 11_999);
        var second = ordered.stream().map(value -> value + 7).toList();
        assertThat(BenchmarkSteadyOutputWindow.readCommonOperations(java.util.List.of(nativeOrder, second)))
                .isEqualTo(BenchmarkSteadyOutputWindow.readCommonOperations(java.util.List.of(ordered, second)));
    }

    @Test void rejectedTrendStillRetainsTheCompleteMeasuredReading() {
        var stream = trendTimeline(10_501);
        var reading = BenchmarkSteadyOutputWindow.readCommonOperations(java.util.List.of(stream), false);
        assertThat(reading.completedDeliveries()).isEqualTo(20_501);
        assertThat(reading.fixedBins()).hasSize(10);
        assertThat(reading.recordsPerSecond()).isEqualTo(20_501);
        assertThatThrownBy(() -> BenchmarkSteadyOutputWindow.requireSteady(reading))
                .isInstanceOf(AssertionError.class).hasMessageContaining("trend");
    }

    @Test void aRecordedIdleWindowStillFailsQualification() {
        var operations = new ArrayList<Long>();
        operations.add(0L);
        for (int i = 0; i < 12_000; i++) { operations.add(1_000_000L + i); }
        var reading = BenchmarkSteadyOutputWindow.readCommonOperations(java.util.List.of(operations), false);
        assertThat(reading.completedDeliveries()).isEqualTo(12_000);
        assertThat(reading.fixedBins()).contains(0L);
        assertThatThrownBy(() -> BenchmarkSteadyOutputWindow.requireSteady(reading))
                .isInstanceOf(AssertionError.class).hasMessageContaining("idle time bin");
    }

    @Test void independentlySteadyTargetsCannotCreateAnApparentRateTrend() {
        var faster = new ArrayList<Long>();
        var slower = new ArrayList<Long>();
        for (int i = 0; i < 48_000; i++) {
            faster.add(1_000_000L + i);
            slower.add(1_000_000L + i * 2L);
        }
        assertThat(BenchmarkSteadyOutputWindow.readServerOperations(faster).recordsPerSecond()).isEqualTo(1_000);
        assertThat(BenchmarkSteadyOutputWindow.readServerOperations(slower).recordsPerSecond()).isEqualTo(500);
        var common = BenchmarkSteadyOutputWindow.readCommonOperations(java.util.List.of(faster, slower));
        assertThat(common.completedDeliveries()).isEqualTo(71_998);
        assertThat(common.recordsPerSecond()).isEqualTo(71_998 * 1_000.0 / 47_999);
        assertThatThrownBy(() -> BenchmarkSteadyOutputWindow.readCommonOperations(
                java.util.List.of(faster, slower.stream().map(value -> value + 100_000).toList())))
                .isInstanceOf(AssertionError.class).hasMessageContaining("no common interval");
        var tied = BenchmarkSteadyOutputWindow.readCommonOperations(java.util.List.of(faster, faster));
        assertThat(tied.completedDeliveries()).as("both start ties are excluded and both end ties are retained")
                .isEqualTo(95_998);
        assertThat(tied.recordsPerSecond()).isEqualTo(2_000);
    }

    @Test void exactFivePercentTrendIsAcceptedButOneMoreEventIsRejected() {
        assertThat(BenchmarkSteadyOutputWindow.read(trendTimeline(10_500)).completedDeliveries())
                .isEqualTo(20_500);
        assertThatThrownBy(() -> BenchmarkSteadyOutputWindow.read(trendTimeline(10_501)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("trend");
    }

    @Test void missingLogicalTimeCannotEraseAnAlreadyObservedOperationOrder() {
        var order = new BenchmarkMongoDeliveryObserver.OperationOrder();
        assertThat(order.accept(new org.bson.BsonTimestamp(2_000, 1))).isTrue();
        assertThat(order.accept(null)).isFalse();
        assertThat(order.accept(new org.bson.BsonTimestamp(1_500, 1))).isFalse();
        assertThat(order.accept(new org.bson.BsonTimestamp(2_000, 0))).isFalse();
        assertThat(order.accept(new org.bson.BsonTimestamp(2_000, 1))).isTrue();
        assertThat(order.accept(new org.bson.BsonTimestamp(2_000, 2))).isTrue();
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
        assertThat(BenchmarkSteadyOutputWindow.readServerOperations(backward).completedDeliveries()).isEqualTo(11_999);
        var observedBackward = new ArrayList<>(observed); observedBackward.set(500, observed.get(499)-1);
        assertThatThrownBy(() -> BenchmarkSteadyOutputWindow.readOperationCohort(operations, observedBackward))
                .isInstanceOf(AssertionError.class).hasMessageContaining("local observed delivery cohort moved backward");
    }
    @Test void chronologicalMergePreservesEveryOperationAndRejectsMissingWallTime() {
        assertThat(BenchmarkSteadyOutputWindow.mergeValidatedOperationStreams(java.util.List.of(java.util.List.of(1L,3L),java.util.List.of(2L,4L))))
                .containsExactly(1L,2L,3L,4L);
        assertThat(BenchmarkSteadyOutputWindow.mergeValidatedOperationStreams(java.util.List.of(java.util.List.of(3L,1L),java.util.List.of(2L,4L))))
                .containsExactly(1L,2L,3L,4L);
        var missing = new ArrayList<Long>(); missing.add(3L); missing.add(null);
        assertThatThrownBy(() -> BenchmarkSteadyOutputWindow.mergeValidatedOperationStreams(java.util.List.of(missing)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("missing");
    }
}
