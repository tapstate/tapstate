package io.tapstate.core.lifecycle;

import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class NestStateWindowTest {

    @Test
    void callbackCountsStaySeparateFromTheKeyShare() {
        NestStateWindow window = NestStateWindow.fromStart(
                new NestStateReading(8, 1, 4, 20).withKeyCounts(8, 4));
        assertThat(window.accesses()).isOne();
        assertThat(window.backfills()).isEqualTo(4);
        assertThat(window.servedFromCold()).hasValue(0.5);
        assertThat(window.millisPerBackfill()).hasValue(5.0);
    }

    @Test
    void legacyAndPartiallyMeasuredWindowsHaveNoKeyShare() {
        NestStateReading legacy = new NestStateReading(8, 100, 400, 20);
        assertThat(NestStateWindow.fromStart(legacy).servedFromCold()).isEmpty();
        assertThat(NestStateWindow.between(legacy, legacy.withKeyCounts(800, 400)).servedFromCold())
                .isEmpty();
        assertThat(new NestStateWindow(100, 400, 20, 8, OptionalLong.empty(),
                OptionalLong.of(800), OptionalLong.empty()).servedFromCold()).isEmpty();
    }

    @Test
    void aKeyCounterResetStartsANewWindowEvenWhenLegacyCountersIncreased() {
        NestStateReading before = new NestStateReading(8, 100, 4, 20).withKeyCounts(800, 400);
        NestStateReading after = new NestStateReading(8, 200, 8, 40).withKeyCounts(80, 40);
        NestStateWindow window = NestStateWindow.between(before, after);
        assertThat(window.accesses()).isEqualTo(200);
        assertThat(window.keyAccesses()).hasValue(80);
        assertThat(window.coldKeys()).hasValue(40);
        assertThat(window.servedFromCold()).hasValue(0.5);
    }

    @Test
    void whatWasServedFromTheColdLayerIsCountedOverTheWindowAndNotOverTheRun() {
        // The counts a reading carries run from when the member came up, so a ratio taken from one of them
        // is an average over the whole run -- the one window in which a namespace that fell off its cliff a
        // minute ago still reads as healthy. Here the run has served 1000 accesses with 100 backfills (10%)
        // and the latest interval served 100 with 90 (90%); the window is what the second pair says.
        NestStateReading before = new NestStateReading(50, 1_000, 100, 10, OptionalLong.of(500)).withKeyCounts(1_000, 100);
        NestStateReading after = new NestStateReading(50, 1_100, 190, 100, OptionalLong.of(500)).withKeyCounts(1_100, 190);

        NestStateWindow window = NestStateWindow.between(before, after);

        assertThat(window.accesses()).isEqualTo(100);
        assertThat(window.backfills()).isEqualTo(90);
        assertThat(window.servedFromCold()).hasValue(0.9);
    }

    @Test
    void countersThatWentBackwardsAreReadAsARunThatStartedOverRatherThanAsNegativeTraffic() {
        // The counts live on the member, not in the state, and are dropped when a pipeline lets go of its
        // namespace. A pipeline stopped and started again therefore reports smaller numbers than last time.
        // Subtracting would make the window negative, and a negative over a negative is a ratio that reads
        // like anything at all; what the second reading actually describes is the whole of the new run.
        NestStateReading before = new NestStateReading(50, 1_000, 100, 10, OptionalLong.of(500)).withKeyCounts(1_000, 100);
        NestStateReading after = new NestStateReading(20, 40, 36, 8, OptionalLong.of(200)).withKeyCounts(40, 36);

        NestStateWindow window = NestStateWindow.between(before, after);

        assertThat(window.accesses()).isEqualTo(40);
        assertThat(window.backfills()).isEqualTo(36);
        assertThat(window.servedFromCold()).hasValue(0.9);
    }

    @Test
    void aWindowThatReachedForNothingHasNoRatioRatherThanARatioOfZero() {
        // Nothing was asked of the namespace in this interval, which is not the same as everything having
        // been served from memory. Zero here would be the healthy end of the scale, so an idle namespace
        // would read as the one thing this is watching for the absence of.
        NestStateReading reading = new NestStateReading(50, 1_000, 100, 10, OptionalLong.of(500)).withKeyCounts(1_000, 100);

        NestStateWindow window = NestStateWindow.between(reading, reading);

        assertThat(window.accesses()).isZero();
        assertThat(window.servedFromCold()).isEmpty();
    }

    @Test
    void howMuchOfTheNamespaceIsResidentIsReadFromTheLatestReadingAndNotDifferenced() {
        // entries and stored are both how much there is right now, not how much there has been, so the
        // window takes them as they stand. Differencing them would describe how much the namespace grew,
        // which says nothing about how much of it can be served without going to disk.
        NestStateReading before = new NestStateReading(100, 1_000, 100, 10, OptionalLong.of(1_000)).withKeyCounts(1_000, 100);
        NestStateReading after = new NestStateReading(250, 1_100, 190, 100, OptionalLong.of(1_000)).withKeyCounts(1_100, 190);

        NestStateWindow window = NestStateWindow.between(before, after);

        assertThat(window.entries()).isEqualTo(250);
        assertThat(window.stored()).hasValue(1_000);
        assertThat(window.resident()).hasValue(0.25);
    }

    @Test
    void aRunWithNoLayerBehindItsMemoryHasNoResidentFractionRatherThanAFullOne() {
        // Absent, not 1.0: a namespace held in memory alone has no second number to divide by, and
        // answering "all of it is resident" would be the first number wearing the name of the ratio.
        NestStateReading before = new NestStateReading(100, 1_000, 0, 0);
        NestStateReading after = new NestStateReading(250, 1_100, 0, 0);

        NestStateWindow window = NestStateWindow.between(before, after);

        assertThat(window.stored()).isEmpty();
        assertThat(window.resident()).isEmpty();
    }

    @Test
    void anEmptyNamespaceHasNoResidentFractionRatherThanANotANumber() {
        // A namespace that has reported but holds nothing yet -- which every namespace is at startup, and
        // for a while after -- would divide zero by zero. That is not an error and not zero either; it
        // renders as NaN, which would travel all the way into what an operator reads.
        NestStateReading before = new NestStateReading(0, 0, 0, 0, OptionalLong.of(0)).withKeyCounts(0, 0);
        NestStateReading after = new NestStateReading(0, 10, 0, 0, OptionalLong.of(0)).withKeyCounts(10, 0);

        NestStateWindow window = NestStateWindow.between(before, after);

        assertThat(window.stored()).hasValue(0);
        assertThat(window.resident()).isEmpty();
    }

    @Test
    void whatEachTripToTheColdLayerCostIsWhatTurnsTheRatioIntoATime() {
        // A ratio alone cannot be compared against anything: a tenth of the reaching going to a layer that
        // answers in microseconds and a tenth going to one that answers in tens of milliseconds are not the
        // same finding. 90 backfills costing 450ms is 5ms apiece.
        NestStateReading before = new NestStateReading(50, 1_000, 100, 10, OptionalLong.of(500)).withKeyCounts(1_000, 100);
        NestStateReading after = new NestStateReading(50, 1_100, 190, 460, OptionalLong.of(500)).withKeyCounts(1_100, 190);

        NestStateWindow window = NestStateWindow.between(before, after);

        assertThat(window.millisPerBackfill()).hasValueCloseTo(5.0, within(0.001));
    }

    @Test
    void aWindowThatWentToTheColdLayerForNothingHasNoCostPerTripToReport() {
        // Dividing by no trips at all would be a cost invented for a namespace that paid none.
        NestStateReading reading = new NestStateReading(50, 1_000, 100, 10, OptionalLong.of(500)).withKeyCounts(1_000, 100);
        NestStateReading after = new NestStateReading(50, 1_100, 100, 10, OptionalLong.of(500)).withKeyCounts(1_100, 100);

        NestStateWindow window = NestStateWindow.between(reading, after);

        assertThat(window.backfills()).isZero();
        assertThat(window.millisPerBackfill()).isEmpty();
    }

    @Test
    void theFirstReadingOfARunIsAWindowFromNothingSoItIsNotWaitedOutBeforeAnythingCanBeSaid() {
        // There is no earlier reading to difference against when a run has just been seen for the first
        // time, and the counts are from the start of that run anyway. Treating it as unreportable would
        // blind the first interval, which is where a badly sized namespace announces itself soonest.
        NestStateReading first = new NestStateReading(10, 200, 180, 900, OptionalLong.of(10_000)).withKeyCounts(200, 180);

        NestStateWindow window = NestStateWindow.fromStart(first);

        assertThat(window.accesses()).isEqualTo(200);
        assertThat(window.backfills()).isEqualTo(180);
        assertThat(window.servedFromCold()).hasValue(0.9);
    }
}
