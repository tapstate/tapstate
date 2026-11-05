package io.tapstate.core.lifecycle;

import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class NestColdLayerPressureTest {

    private static final NestColdLayerPressure PRESSURE = new NestColdLayerPressure(0.5, 100);

    @Test
    void largeBatchesStillRequireOneHundredLogicalAccesses() {
        assertThat(NestColdLayerPressure.DEFAULT).isEqualTo(PRESSURE);
        assertThat(PRESSURE.isOver(NestStateWindow.fromStart(
                new NestStateReading(8, 99, 400, 0).withKeyCounts(800, 800)))).isFalse();
        assertThat(PRESSURE.isOver(NestStateWindow.fromStart(
                new NestStateReading(8, 100, 400, 0).withKeyCounts(800, 800)))).isTrue();
    }

    @Test
    void pressureUsesTheFractionOfRequestedKeys() {
        assertThat(PRESSURE.isOver(NestStateWindow.fromStart(
                new NestStateReading(8, 100, 400, 0).withKeyCounts(800, 400)))).isTrue();
        assertThat(PRESSURE.isOver(NestStateWindow.fromStart(
                new NestStateReading(8, 100, 400, 0).withKeyCounts(800, 399)))).isFalse();
    }

    @Test
    void aNamespaceServingMostOfItsReachingFromDiskIsUnderPressure() {
        // The failure this exists for: nothing else says it is happening. The queues are short, the lag is
        // flat and the throughput is what it always was, because per-key state is a buffer backpressure
        // cannot see -- an event being consumed makes the resident state larger and fills no edge queue.
        NestStateWindow window = singleKeyWindow(1_000, 900);

        assertThat(PRESSURE.isOver(window)).isTrue();
    }

    @Test
    void aNamespaceServingItsReachingFromMemoryIsNotUnderPressureHoweverLittleOfItIsResident() {
        // Almost nothing being resident is the intended shape, not the alarm: the budget is one number and
        // everything past it lives on a layer with no limit. A namespace whose reaching lands on the hot
        // keys is doing exactly what the budget was set for, and alerting on the resident fraction alone
        // would fire on every correctly sized deployment.
        NestStateWindow window = NestStateWindow.between(
                new NestStateReading(100, 1_000, 20, 100, OptionalLong.of(10_000_000)).withKeyCounts(1_000, 20),
                new NestStateReading(100, 2_000, 40, 200, OptionalLong.of(10_000_000)).withKeyCounts(2_000, 40));

        assertThat(window.resident()).hasValueCloseTo(0.00001, within(1e-12));
        assertThat(PRESSURE.isOver(window)).isFalse();
    }

    @Test
    void aWindowTooSmallToMeanAnythingIsNotReportedHoweverBadItsRatioLooks() {
        // Three reaches of which two missed is not a namespace under pressure, it is three reaches. Without
        // a floor the quiet intervals -- which is most of them for a namespace nobody is touching -- would
        // each produce a ratio drawn from single digits and the alert would be noise.
        NestStateWindow window = singleKeyWindow(3, 3);

        assertThat(window.servedFromCold()).hasValue(1.0);
        assertThat(PRESSURE.isOver(window)).isFalse();
    }

    @Test
    void aWindowThatReachedForNothingIsNotUnderPressure() {
        // An idle namespace has no ratio at all, and absent must not fall through to the alarming side.
        NestStateReading reading = new NestStateReading(50, 1_000, 900, 100, OptionalLong.of(500));

        assertThat(PRESSURE.isOver(NestStateWindow.between(reading, reading))).isFalse();
    }

    @Test
    void aNamespaceExactlyAtTheThresholdIsUnderPressure() {
        // Read the other way the threshold is a value nothing ever reaches from below, since the ratio is
        // continuous; taking it as met is what makes "half of the reaching goes to disk" the line it says
        // it is.
        NestStateWindow window = singleKeyWindow(1_000, 500);

        assertThat(window.servedFromCold()).hasValue(0.5);
        assertThat(PRESSURE.isOver(window)).isTrue();
    }

    @Test
    void aNamespaceWithNoLayerBehindItsMemoryIsNeverUnderPressure() {
        // Nothing to go to disk for. A run holding its state in memory alone reports no backfills, so the
        // ratio is zero on its own -- this is not a special case in the rule, and the test is here to hold
        // it that way rather than to describe a branch.
        NestStateWindow window = NestStateWindow.between(
                new NestStateReading(100, 1_000, 0, 0).withKeyCounts(1_000, 0),
                new NestStateReading(100, 5_000, 0, 0).withKeyCounts(5_000, 0));

        assertThat(PRESSURE.isOver(window)).isFalse();
    }

    /** A single-key-per-operation fixture; batch key counts are specified independently above. */
    private static NestStateWindow singleKeyWindow(long accesses, long backfills) {
        return NestStateWindow.between(
                new NestStateReading(50, 0, 0, 0, OptionalLong.of(500)).withKeyCounts(0, 0),
                new NestStateReading(50, accesses, backfills, backfills, OptionalLong.of(500)).withKeyCounts(accesses, backfills));
    }
}
