package io.tapstate.runtime.engine;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.SourceOrder;
import java.util.Map;

/**
 * The one long a cross-level lower bound travels as, and the way back out of it. A bound rides a
 * broadcast marker that carries a single timestamp, while the order it stands for is a pair — the
 * generation of a chain's change ring and the position within it — so the pair is split across the bits
 * of that long: the generation high, the position low.
 *
 * <p>The position field is shifted by one so that the reserved sequence every snapshot row carries lands
 * at the bottom of its generation. That branch is not an optimisation to skip: the reserved sequence is
 * the lowest long there is, and adding one to it unbranched produces a negative value whose order against
 * the changes of the same generation is reversed. Nothing would report it — the axis stays strictly
 * increasing, which is all the engine checks — and the frontier would simply run past changes that are
 * still in flight.
 *
 * <p>Reading it back is equally fixed. An unsigned shift answers with an enormous generation where a
 * signed one answers with a negative generation, and the two fail in opposite directions: a negative
 * generation stalls the frontier forever, an enormous one runs it up to the newest settled change and
 * drops everything beneath. A value the packing could not have produced is a defect in the engine's own
 * wiring, so it tears down rather than answering either way.
 *
 * <p>Both fields are capped so the whole range stays clear of the two values the engine keeps for its own
 * signalling: the highest long means a queue has gone idle — acted on without regard for which axis
 * carried it, so one axis reaching it stops the whole queue from constraining anything — and the lowest
 * means no new bound. The widths themselves are free to move: a bound is in flight only, never written
 * down and never outliving a run, so a wider generation or a wider position costs nothing but a
 * recompile.
 */
public final class FrontierOrders {

    /** How many of the low bits carry the position within a generation. */
    private static final int SEQ_BITS = 47;

    /** Every value the position field can hold. */
    private static final long SEQ_FIELD_MASK = (1L << SEQ_BITS) - 1;

    /**
     * The highest generation that keeps the top of the range below the engine's idle signal. One more
     * than this makes the largest packed value equal that signal exactly, which is why the cap is not the
     * round number the field width suggests.
     */
    public static final long HIGHEST_EPOCH = 65_534;

    /** The highest position a ring can assign and still be encodable: the field spends one on the shift. */
    public static final long HIGHEST_SEQ = SEQ_FIELD_MASK - 1;

    private FrontierOrders() {
    }

    /**
     * The long that carries {@code order} on {@code chain}'s axis. Refuses an order outside the range the
     * fields hold: that is a capacity line rather than a defect, so it is diagnosable and stops the job.
     */
    public static long pack(String chain, SourceOrder order) {
        long epoch = order.epoch();
        long seq = order.seq();
        if (epoch < 0 || epoch > HIGHEST_EPOCH) {
            throw notEncodable(chain, epoch, seq);
        }
        long field;
        if (seq == SourceOrder.SNAPSHOT_SEQ) {
            field = 0;
        } else if (seq < 0 || seq > HIGHEST_SEQ) {
            throw notEncodable(chain, epoch, seq);
        } else {
            field = seq + 1;
        }
        return (epoch << SEQ_BITS) | field;
    }

    /** The order {@code packed} carries. Tears down on a value the packing could not have produced. */
    public static SourceOrder unpack(long packed) {
        long epoch = packed >>> SEQ_BITS;
        if (epoch > HIGHEST_EPOCH) {
            throw new IllegalArgumentException("not a packed frontier order: " + packed
                    + " reads as generation " + epoch);
        }
        long field = packed & SEQ_FIELD_MASK;
        return field == 0 ? SourceOrder.snapshotRow(epoch) : new SourceOrder(epoch, field - 1);
    }

    private static TapstateException notEncodable(String chain, long epoch, long seq) {
        return new TapstateException(EngineError.FRONTIER_ORDER_NOT_ENCODABLE,
                Map.of("chain", chain, "epoch", epoch, "seq", seq), null);
    }
}
