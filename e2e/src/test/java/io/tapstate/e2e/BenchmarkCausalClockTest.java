package io.tapstate.e2e;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BenchmarkCausalClockTest {
    private static final BenchmarkCausalClock.Identity OWNER = new BenchmarkCausalClock.Identity(17, 1_000);

    @Test void unrelatedOriginsAndMonotonicRateChangesRetainTheActualCausalBounds() {
        var samples = List.of(sample(0, 100, 110, -9_000), sample(1, 200, 210, -8_000),
                sample(2, 400, 410, -1_000));
        var mapped = new BenchmarkCausalClock(OWNER, samples).map(OWNER, -7_000);
        assertThat(mapped).isEqualTo(new BenchmarkCausalClock.Interval(200, 410));
        // A constant offset inferred from the first request would place the point at 2,100.
        // This synthetic point actually occurs at driver time 330 while the owned clock rate changes.
        assertThat(mapped.lowerNanos()).isLessThanOrEqualTo(330);
        assertThat(mapped.upperNanos()).isGreaterThanOrEqualTo(330);
        var shifted = samples.stream().map(value -> sample(value.sequence(), value.driverBeforeNanos(),
                value.driverAfterNanos(), value.ownedNanos() + 1_000_000)).toList();
        assertThat(new BenchmarkCausalClock(OWNER, shifted).map(OWNER, 993_000)).isEqualTo(mapped);
    }

    @Test void onlyStrictEnclosureCanUseAnEqualCounterReading() {
        var first = sample(0, 10, 15, 100);
        var middle = sample(1, 20, 25, 200);
        var last = sample(2, 30, 35, 300);
        assertThat(new BenchmarkCausalClock(OWNER, List.of(first, middle, last)).map(OWNER, 200))
                .isEqualTo(new BenchmarkCausalClock.Interval(10, 35));
        assertThatThrownBy(() -> new BenchmarkCausalClock(OWNER, List.of(first, middle)).map(OWNER, 200))
                .isInstanceOf(AssertionError.class).hasMessageContaining("strictly enclosing");
        assertThatThrownBy(() -> new BenchmarkCausalClock(OWNER, List.of(middle, last)).map(OWNER, 200))
                .isInstanceOf(AssertionError.class).hasMessageContaining("strictly enclosing");
    }

    @Test void finiteResolutionPlateausCannotInventANarrowerTimeBound() {
        var samples = List.of(sample(0, 10, 15, 100), sample(1, 20, 25, 200),
                sample(2, 30, 35, 200), sample(3, 40, 45, 300));
        assertThat(new BenchmarkCausalClock(OWNER, samples).map(OWNER, 200))
                .isEqualTo(new BenchmarkCausalClock.Interval(10, 45));
        assertThat(new BenchmarkCausalClock(OWNER, samples).map(OWNER, 250))
                .isEqualTo(new BenchmarkCausalClock.Interval(30, 45));
    }

    @Test void aPointOutsideEitherCoverageEndpointIsRejectedRatherThanExtrapolated() {
        var clock = new BenchmarkCausalClock(OWNER, List.of(sample(0, 10, 15, 100), sample(1, 20, 25, 200)));
        for (long point : List.of(99L, 100L, 200L, 201L)) {
            assertThatThrownBy(() -> clock.map(OWNER, point))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("strictly enclosing");
        }
    }

    @Test void everySampleAndPointMustBindTheExactRuntimeIncludingPidReuse() {
        var good = sample(0, 10, 15, 100);
        for (var other : List.of(new BenchmarkCausalClock.Identity(18, 1_000),
                new BenchmarkCausalClock.Identity(17, 1_001))) {
            assertThatThrownBy(() -> new BenchmarkCausalClock(OWNER,
                    List.of(good, new BenchmarkCausalClock.Sample(1, other, 20, 25, 200))))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("owned runtime identity");
            var clock = new BenchmarkCausalClock(OWNER, List.of(good, sample(1, 20, 25, 200)));
            assertThatThrownBy(() -> clock.map(other, 150))
                    .isInstanceOf(AssertionError.class).hasMessageContaining("owned runtime identity");
        }
        var clock = new BenchmarkCausalClock(OWNER, List.of(good, sample(1, 20, 25, 200)));
        assertThatThrownBy(() -> clock.map(null, 150)).isInstanceOf(AssertionError.class);
    }

    @Test void corruptionCannotBeHiddenBySortingCountersOrRequestReceipts() {
        var first = sample(0, 10, 15, 100);
        assertThatThrownBy(() -> new BenchmarkCausalClock(OWNER, List.of(first, sample(0, 20, 25, 200))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("sequence did not advance");
        assertThatThrownBy(() -> new BenchmarkCausalClock(OWNER, List.of(first, sample(1, 14, 25, 200))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("ordered serial requests");
        assertThatThrownBy(() -> new BenchmarkCausalClock(OWNER, List.of(first, sample(1, 20, 25, 99))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("counter moved backward");
        assertThatThrownBy(() -> sample(1, 25, 20, 200))
                .isInstanceOf(AssertionError.class).hasMessageContaining("bracket moved backward");
        assertThatThrownBy(() -> new BenchmarkCausalClock(OWNER, Arrays.asList(first, null)))
                .isInstanceOf(AssertionError.class).hasMessageContaining("missing owned runtime identity");
    }

    @Test void theRosterIsBoundedAndCannotBeChangedAfterAdmission() {
        var samples = new ArrayList<BenchmarkCausalClock.Sample>();
        for (int i = 0; i < BenchmarkCausalClock.MAX_SAMPLES; i++) {
            samples.add(sample(i, i * 10L, i * 10L + 5, i * 100L));
        }
        var clock = new BenchmarkCausalClock(OWNER, samples);
        samples.add(sample(BenchmarkCausalClock.MAX_SAMPLES, 10_000, 10_005, 100_000));
        assertThat(clock.samples()).hasSize(BenchmarkCausalClock.MAX_SAMPLES);
        assertThat(clock.map(OWNER, 150)).isEqualTo(new BenchmarkCausalClock.Interval(10, 25));
        assertThatThrownBy(() -> new BenchmarkCausalClock(OWNER, samples))
                .isInstanceOf(AssertionError.class).hasMessageContaining("bounded sample roster");
        assertThatThrownBy(() -> clock.samples().clear()).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> new BenchmarkCausalClock(OWNER, List.of(samples.getFirst())))
                .isInstanceOf(AssertionError.class).hasMessageContaining("bounded sample roster");
    }

    @Test void negativeOriginsArePermittedButArithmeticWraparoundIsNot() {
        var clock = new BenchmarkCausalClock(OWNER,
                List.of(sample(0, -30, -25, -1_000), sample(1, -20, -15, -500)));
        assertThat(clock.map(OWNER, -750)).isEqualTo(new BenchmarkCausalClock.Interval(-30, -15));
        assertThatThrownBy(() -> sample(0, Long.MIN_VALUE, Long.MAX_VALUE, 0))
                .isInstanceOf(AssertionError.class).hasMessageContaining("request bracket overflow");
        assertThatThrownBy(() -> new BenchmarkCausalClock(OWNER,
                List.of(sample(0, 10, 15, Long.MIN_VALUE), sample(1, 20, 25, Long.MAX_VALUE))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("owned counter order overflow");
        assertThatThrownBy(() -> clock.map(OWNER, Long.MAX_VALUE))
                .isInstanceOf(AssertionError.class).hasMessageContaining("owned point coverage span overflow");
    }

    @Test void cumulativeCoverageOverflowIsRejectedEvenWhenEveryIndividualStepFits() {
        assertThatThrownBy(() -> new BenchmarkCausalClock(OWNER, List.of(
                sample(0, Long.MIN_VALUE, Long.MIN_VALUE + 1, 10),
                sample(1, -1, 0, 20), sample(2, Long.MAX_VALUE - 1, Long.MAX_VALUE, 30))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("driver coverage span overflow");
        assertThatThrownBy(() -> new BenchmarkCausalClock(OWNER, List.of(
                sample(0, 10, 15, Long.MIN_VALUE), sample(1, 20, 25, -1),
                sample(2, 30, 35, Long.MAX_VALUE - 1))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("owned coverage span overflow");
    }

    @Test void latencyAndElapsedBoundsRetainTheWholeUncertaintyInterval() {
        var completion = new BenchmarkCausalClock.Interval(200, 410);
        assertThat(completion.relativeTo(180)).isEqualTo(new BenchmarkCausalClock.Interval(20, 230));
        assertThat(completion.relativeTo(250)).isEqualTo(new BenchmarkCausalClock.Interval(-50, 160));
        var start = new BenchmarkCausalClock.Interval(100, 120);
        var end = new BenchmarkCausalClock.Interval(110, 130);
        assertThat(BenchmarkCausalClock.elapsed(start, end)).isEqualTo(new BenchmarkCausalClock.Interval(-10, 30));
        assertThatThrownBy(() -> completion.relativeTo(Long.MIN_VALUE))
                .isInstanceOf(AssertionError.class).hasMessageContaining("relative lower bound overflow");
        assertThatThrownBy(() -> BenchmarkCausalClock.elapsed(start, null))
                .isInstanceOf(AssertionError.class).hasMessageContaining("both endpoints");
    }

    @Test void missingAndMalformedIdentityOrSampleMetadataCannotEnterTheMapper() {
        assertThatThrownBy(() -> new BenchmarkCausalClock.Identity(0, 1_000)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> new BenchmarkCausalClock.Identity(17, 0)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> new BenchmarkCausalClock.Sample(-1, OWNER, 10, 15, 100))
                .isInstanceOf(AssertionError.class).hasMessageContaining("invalid identity or sequence");
        assertThatThrownBy(() -> new BenchmarkCausalClock.Sample(0, null, 10, 15, 100))
                .isInstanceOf(AssertionError.class).hasMessageContaining("invalid identity or sequence");
        assertThatThrownBy(() -> new BenchmarkCausalClock(OWNER, null)).isInstanceOf(AssertionError.class);
        assertThatThrownBy(() -> new BenchmarkCausalClock(null,
                List.of(sample(0, 10, 15, 100), sample(1, 20, 25, 200))))
                .isInstanceOf(AssertionError.class).hasMessageContaining("bounded sample roster");
    }

    private static BenchmarkCausalClock.Sample sample(long sequence, long before, long after, long owned) {
        return new BenchmarkCausalClock.Sample(sequence, OWNER, before, after, owned);
    }
}
