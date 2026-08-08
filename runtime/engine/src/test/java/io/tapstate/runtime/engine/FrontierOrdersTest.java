package io.tapstate.runtime.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.SourceOrder;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Locks the single long an engine bound travels as. A bound rides a broadcast marker that carries one
 * timestamp, while the order it stands for is a pair — a generation and a position within it — so the
 * pair is split across the bits of that one long. Everything downstream compares bounds by that long
 * alone, which makes three properties load-bearing: the packing is injective, its natural order is
 * exactly the order on the pair, and its whole range avoids the two values the engine keeps for its own
 * signalling.
 *
 * <p>The bit widths themselves are free to move — a bound is in flight only, never written down and
 * never outliving a run — but every property below has to survive the move, which is why they are pinned
 * as values rather than as arithmetic that would move with the constants.
 */
class FrontierOrdersTest {

    /** How many values the sequence field holds: one more than the highest field value. */
    private static final long SEQ_SPAN = 1L << 47;

    /** The highest sequence a ring can assign and still be encodable — the field's top is reserved. */
    private static final long HIGHEST_SEQ = SEQ_SPAN - 2;

    private static final long HIGHEST_EPOCH = 65_534;

    @Test
    void aSnapshotRowSitsAtTheBottomOfItsGenerationRatherThanJustAboveTheReservedSequence() {
        // Snapshot rows share one reserved sequence, and it is the lowest long there is: adding one to it
        // without branching lands just above the bottom of the entire range, which is a negative value
        // that reverses the order against every change of the same generation. Nothing downstream would
        // report that — the axis would simply be strictly increasing from a large negative first value.
        for (long epoch : List.of(0L, 1L, 7L, HIGHEST_EPOCH)) {
            assertThat(FrontierOrders.pack("orders", SourceOrder.snapshotRow(epoch)))
                    .describedAs("generation %s's snapshot rows", epoch)
                    .isEqualTo(epoch * SEQ_SPAN)
                    .isNotNegative();
        }
    }

    @Test
    void pinsTheExactLongsThreeKnownOrdersEncodeTo() {
        assertThat(FrontierOrders.pack("orders", SourceOrder.snapshotRow(7)))
                .isEqualTo(985_162_418_487_296L);
        assertThat(FrontierOrders.pack("orders", new SourceOrder(7, 19_088_743)))
                .isEqualTo(985_162_437_576_040L);
        assertThat(FrontierOrders.pack("orders", new SourceOrder(8, 0)))
                .isEqualTo(1_125_899_906_842_625L);
    }

    @Test
    void everyOrderInRangeComesBackOutAsTheOrderThatWentIn() {
        for (long epoch : List.of(0L, 1L, 7L, HIGHEST_EPOCH)) {
            for (long seq : List.of(SourceOrder.SNAPSHOT_SEQ, 0L, 1L, 2L, HIGHEST_SEQ)) {
                SourceOrder order = new SourceOrder(epoch, seq);
                assertThat(FrontierOrders.unpack(FrontierOrders.pack("orders", order)))
                        .describedAs("epoch %s sequence %s", epoch, seq)
                        .isEqualTo(order);
            }
        }
    }

    @Test
    void packingKeepsExactlyTheOrderComparingThePairsGives() {
        // Two distinct orders packing to one long would let a bound of "at or below X" select a change
        // that is really above X, which is the frontier claiming a change is durable when it is not.
        List<SourceOrder> ascending = List.of(
                SourceOrder.snapshotRow(0),
                new SourceOrder(0, 0),
                new SourceOrder(0, 1),
                new SourceOrder(0, HIGHEST_SEQ),
                SourceOrder.snapshotRow(1),
                new SourceOrder(1, 0),
                SourceOrder.snapshotRow(7),
                new SourceOrder(7, 19_088_743),
                SourceOrder.snapshotRow(8),
                new SourceOrder(HIGHEST_EPOCH, HIGHEST_SEQ));

        List<Long> packed = new ArrayList<>();
        for (SourceOrder order : ascending) {
            packed.add(FrontierOrders.pack("orders", order));
        }

        assertThat(ascending)
                .describedAs("the list below is only a witness if it really is in ascending pair order")
                .isSorted();
        assertThat(packed)
                .describedAs("a bound is compared as this long alone, so its order has to be the pair's")
                .isSorted()
                .doesNotHaveDuplicates();
    }

    @Test
    void theWholeRangeStaysClearOfTheTwoValuesTheEngineKeepsForItsOwnSignalling() {
        // The engine reads the highest long as "this queue has gone idle" — and it acts on that without
        // looking at which axis carried it, so one axis reaching that value stops the whole queue from
        // constraining anything. The lowest long is its "no new bound" answer.
        assertThat(FrontierOrders.pack("orders", new SourceOrder(HIGHEST_EPOCH, HIGHEST_SEQ)))
                .describedAs("the top of the range must stay below the idle signal")
                .isLessThan(Long.MAX_VALUE);
        assertThat(FrontierOrders.pack("orders", SourceOrder.snapshotRow(0)))
                .describedAs("the bottom of the range must stay above the no-new-bound signal")
                .isGreaterThan(Long.MIN_VALUE);
        assertThat((65_535L << 47) | (SEQ_SPAN - 1))
                .describedAs("one generation more than the cap collides with the idle signal exactly, "
                        + "which is why the cap is not the round number it looks like it should be")
                .isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void oneBelowAGenerationsFirstOrderIsTheLastOrderOfTheGenerationBeforeIt() {
        // A level that is holding a change promises up to one below it. That subtraction is exact rather
        // than approximate only because the packing leaves no gaps, and it has to stay exact across a
        // generation boundary too, where the neighbouring orders are not neighbouring pairs.
        assertThat(FrontierOrders.pack("orders", SourceOrder.snapshotRow(8)) - 1)
                .isEqualTo(FrontierOrders.pack("orders", new SourceOrder(7, HIGHEST_SEQ)));
    }

    @Test
    void refusesAGenerationOrSequencePastWhatTheFieldsHold() {
        assertThatThrownBy(() -> FrontierOrders.pack("orders", new SourceOrder(65_535, 0)))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("engine.frontier-order-not-encodable")
                .hasMessageContaining("chain=orders")
                .hasMessageContaining("epoch=65535");
        assertThatThrownBy(() -> FrontierOrders.pack("orders", new SourceOrder(-1, 0)))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("engine.frontier-order-not-encodable");
        assertThatThrownBy(() -> FrontierOrders.pack("orders", new SourceOrder(0, HIGHEST_SEQ + 1)))
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("engine.frontier-order-not-encodable")
                .hasMessageContaining("seq=" + (HIGHEST_SEQ + 1));
        assertThatThrownBy(() -> FrontierOrders.pack("orders", new SourceOrder(0, -5)))
                .describedAs("only the one reserved sequence is below zero; any other negative is a "
                        + "sequence the ring never assigned")
                .isInstanceOf(TapstateException.class)
                .hasMessageContaining("engine.frontier-order-not-encodable");
    }

    @Test
    void unpackingAValueTheEncodingCouldNotHaveProducedTearsDownRatherThanAnsweringAnything() {
        // The two ways of reading the generation back out fail in opposite directions: a signed shift
        // answers with a negative generation, which stalls the frontier forever, and an unsigned one
        // answers with an enormous generation, which runs the frontier up to the newest settled change
        // and drops everything beneath it. Neither is an answer worth having, so this is a teardown.
        assertThatThrownBy(() -> FrontierOrders.unpack(-1L))
                .isInstanceOf(IllegalArgumentException.class)
                .isNotInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> FrontierOrders.unpack(65_535L << 47))
                .describedAs("the one generation above the cap is reachable by arithmetic but never by "
                        + "packing")
                .isInstanceOf(IllegalArgumentException.class);
    }
}
