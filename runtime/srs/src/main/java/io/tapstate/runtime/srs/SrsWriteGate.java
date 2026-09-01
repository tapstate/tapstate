package io.tapstate.runtime.srs;

import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * The headroom guard on writes into one per-table change ring. A cdc write is admitted only when it would
 * not overwrite a change the slowest consumer has not read yet; otherwise it is backpressured — refused,
 * so the source read pauses until a consumer advances and frees a slot, never silently dropping an unread
 * change. This is the application-layer overflow control the ring itself does not do: the ring only writes
 * and reports its bounds.
 *
 * <p>The precheck compares the sequence the next write would take, {@code tailSeq + 1}, against the
 * slowest consumer's read cursor — a write is refused when {@code (tailSeq + 1) - minConsumerReadSeq >
 * capacity}, i.e. it would evict a sequence no consumer has read. {@code minConsumerReadSeq} is the
 * minimum last-read sequence across the ring's consumers ({@code -1} when a consumer has read nothing);
 * pass {@link Long#MAX_VALUE} when no consumer constrains the ring.
 */
public final class SrsWriteGate {

    private final SrsRingbuffer ring;

    public SrsWriteGate(SrsRingbuffer ring) {
        this.ring = Objects.requireNonNull(ring, "ring");
    }

    /**
     * Appends the change and returns the sequence the ring assigned it, or empty when the write is
     * backpressured — it would overwrite a change the slowest consumer has not read, so nothing is
     * written and the caller pauses the source read until a consumer advances.
     */
    public OptionalLong append(SrsItem item, long minConsumerReadSeq) {
        Objects.requireNonNull(item, "item");
        if (!hasHeadroom(ring.tailSequence(), ring.capacity(), minConsumerReadSeq)) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(ring.append(item));
    }

    /**
     * Admits a whole run of changes, returning the sequence the ring assigned the last of them, or empty
     * when the run does not fit. <strong>All of it or none of it</strong>: admitting a prefix would leave
     * the caller holding the rest with no way to say so, and the run is written down in one act, so a
     * partial admission is not a state the store behind the ring can be left in either.
     */
    public OptionalLong appendAll(List<SrsItem> items, long minConsumerReadSeq) {
        Objects.requireNonNull(items, "items");
        if (items.isEmpty()) {
            return OptionalLong.empty();
        }
        if (!hasHeadroom(ring.tailSequence(), ring.capacity(), minConsumerReadSeq, items.size())) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(ring.appendAll(items));
    }

    /** The ring's capacity -- the largest run that can ever be admitted at once. */
    public long capacity() {
        return ring.capacity();
    }

    /**
     * Whether a write is admitted: the next write takes {@code tailSeq + 1}, and it must not evict a
     * sequence the slowest consumer has not read — refused when {@code (tailSeq + 1) - minConsumerReadSeq
     * > capacity}. The subtraction form stays correct for the {@link Long#MAX_VALUE} unconstrained
     * sentinel, driving the difference far negative rather than overflowing into a false refusal.
     */
    static boolean hasHeadroom(long tailSeq, long capacity, long minConsumerReadSeq) {
        return hasHeadroom(tailSeq, capacity, minConsumerReadSeq, 1);
    }

    /**
     * As above for a run of {@code items} changes: the run takes the sequences up to {@code tailSeq +
     * items}, and none of them may evict a sequence the slowest consumer has not read.
     */
    static boolean hasHeadroom(long tailSeq, long capacity, long minConsumerReadSeq, long items) {
        return (tailSeq + items) - minConsumerReadSeq <= capacity;
    }
}
