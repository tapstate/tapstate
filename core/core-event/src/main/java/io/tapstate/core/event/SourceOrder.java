package io.tapstate.core.event;

import java.io.Serializable;

/**
 * Where one event sits in the order the engine itself assigns: a generation of the chain's change ring
 * ({@code epoch}) and the position the ring gave the event within that generation ({@code seq}).
 *
 * <p>This — not the source position token — is what stateful nodes compare. The token is a connector
 * value the engine never parses: only equality is defined on it, no order, and no connector API offers
 * a comparison, so it can carry a read back to where it left off but can never decide which of two
 * events is later. The two travel together and answer different questions: this one orders, the token
 * resumes.
 *
 * <p>Ordering is lexicographic — epoch first, then sequence — so a higher epoch always wins. That is
 * sound because a new generation begins only where a replay does, above the durable frontier, and each
 * chain is delivered in order: nothing a later generation carries precedes state an earlier one wrote.
 * A bare ring sequence without its epoch is not comparable across generations and must never be used
 * as an order on its own. Both components are plain longs and a sequence may be negative: a value
 * below every sequence the ring assigns is how rows that arrive with no position of their own — the
 * rows of a snapshot — are ordered ahead of every change of their epoch.
 *
 * <p>{@link Serializable} because it is held inside operator state that outlives a single run.
 */
public record SourceOrder(long epoch, long seq) implements Comparable<SourceOrder>, Serializable {

    /**
     * The sequence every snapshot row carries, reserved below every sequence a ring can assign. A ring
     * numbers from zero upwards, so reserving the bottom of the range makes the guarantee total: within one
     * generation, no change can tie with a snapshot row, let alone precede it.
     *
     * <p>Rows of one snapshot are not ordered among themselves — they all carry this one value — which
     * holds because a snapshot reads each key once, so two of its rows never compete for the same key. A
     * source that emits a key twice in one snapshot breaks that, and the two rows will simply not update
     * each other; it is a defect of the source rather than something this order resolves.
     */
    public static final long SNAPSHOT_SEQ = Long.MIN_VALUE;

    /**
     * The order carried by every row of the snapshot that belongs to generation {@code epoch} — the
     * generation the snapshot <em>began</em> in, which is not necessarily the one running now.
     */
    public static SourceOrder snapshotRow(long epoch) {
        return new SourceOrder(epoch, SNAPSHOT_SEQ);
    }

    @Override
    public int compareTo(SourceOrder other) {
        int byEpoch = Long.compare(epoch, other.epoch);
        return byEpoch != 0 ? byEpoch : Long.compare(seq, other.seq);
    }
}
