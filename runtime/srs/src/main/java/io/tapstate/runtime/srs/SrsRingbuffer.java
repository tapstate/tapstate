package io.tapstate.runtime.srs;

import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionException;

/**
 * The per-table change ring: the in-memory hot buffer holding one mining chain's cdc changes for one
 * table, backed by a Hazelcast Ringbuffer. An append carries a monotonic sequence the ring assigns;
 * consumers track their own read cursor against that sequence.
 *
 * <p>Append is a raw write. Overflow safety — the headroom precheck that refuses a write which would
 * overwrite a change no consumer has read yet, and the backpressure that follows — is layered by the
 * caller, not here: this class only writes and reports the ring's bounds.
 *
 * <p>One thing is translated rather than passed on: the cluster's own refusal of an operation. Every ring
 * is guarded by the cluster's split brain protection, and this is the class that holds the library, so
 * this is where its exception type ends — the write path raises {@link RingWriteRefusedException} instead,
 * which says the one thing a caller can act on: nothing was written, and the refusal clears on its own.
 * Only the write path: a read refused the same way has no caller waiting on it, so renaming its failure
 * would disguise it rather than handle it.
 */
public final class SrsRingbuffer {

    private static final String NAME_PREFIX = "srs.";

    private final Ringbuffer<SrsItem> ringbuffer;

    public SrsRingbuffer(Ringbuffer<SrsItem> ringbuffer) {
        this.ringbuffer = Objects.requireNonNull(ringbuffer, "ringbuffer");
    }

    /**
     * The ring name for one table of a mining chain — the per-chain, per-table namespace under which
     * the ring is created and looked up.
     */
    public static String ringName(String miningChainId, String table) {
        if (miningChainId == null || miningChainId.isBlank()) {
            throw new IllegalArgumentException("ring name miningChainId must be non-blank");
        }
        if (table == null || table.isBlank()) {
            throw new IllegalArgumentException("ring name table must be non-blank");
        }
        return NAME_PREFIX + miningChainId + "." + table;
    }

    /**
     * Appends one change and returns the sequence the ring assigned it. A raw write: the caller guards
     * against overwriting an unread change (headroom precheck + backpressure) before calling this.
     */
    public long append(SrsItem item) {
        Objects.requireNonNull(item, "item");
        try {
            return ringbuffer.add(item);
        } catch (RuntimeException raised) {
            throw translate(raised);
        }
    }

    /**
     * Appends a run of changes in one act and returns the sequence the ring assigned the last of them; the
     * run occupies the sequences ending there, one per change. A raw write, like {@link #append}: the
     * caller guards against overwriting an unread change before calling.
     *
     * <p>One act rather than one call per change, because that is what a store behind the ring is offered
     * as one write. The overflow policy is the same one a single append carries -- the headroom precheck
     * is what stops an overwrite, not a policy that refuses to write.
     */
    public long appendAll(List<SrsItem> items) {
        Objects.requireNonNull(items, "items");
        if (items.isEmpty()) {
            throw new IllegalArgumentException("an append of no changes has no sequence to report");
        }
        try {
            return ringbuffer.addAllAsync(items, OverflowPolicy.OVERWRITE).toCompletableFuture().join();
        } catch (CompletionException e) {
            // The ring reports a store failure through the future. Unwrap it so the caller sees what the
            // store said rather than the plumbing that carried it -- a change the store refused is not in
            // the ring, which is the outcome that has to reach the caller intact.
            if (e.getCause() instanceof RuntimeException cause) {
                throw translate(cause);
            }
            throw e;
        } catch (RuntimeException raised) {
            // A refusal decided before the operation is even sent arrives here instead, raised where it is
            // called rather than carried by the future.
            throw translate(raised);
        }
    }

    /**
     * Reads the change at {@code seq}. A reader advances a cursor only up to {@link #tailSequence()}, and
     * every sequence at or before the tail is already present, so this returns immediately rather than
     * blocking for a not-yet-written change. An interrupt while reading an already-present change is not
     * expected; it is restored and surfaced bare rather than swallowed.
     */
    public SrsItem readOne(long seq) {
        try {
            return ringbuffer.readOne(seq);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted reading srs ring at seq " + seq, e);
        } catch (RuntimeException raised) {
            throw translate(raised);
        }
    }

    /** The sequence of the oldest change still in the ring — where a fresh reader starts its replay. */
    public long headSequence() {
        try {
            return ringbuffer.headSequence();
        } catch (RuntimeException raised) {
            throw translate(raised);
        }
    }

    /** The sequence of the most recent item, or {@code -1} when the ring is empty. */
    public long tailSequence() {
        try {
            return ringbuffer.tailSequence();
        } catch (RuntimeException raised) {
            throw translate(raised);
        }
    }

    /** The fixed capacity of the ring — the bound the headroom precheck measures against. */
    public long capacity() {
        try {
            return ringbuffer.capacity();
        } catch (RuntimeException raised) {
            throw translate(raised);
        }
    }

    /**
     * The cluster's refusal restated in this module's terms, or the original failure when it is not one.
     *
     * <p>Every reading either path takes is a guarded operation, not just the append: the tail the headroom
     * precheck compares against is a partition operation, the capacity read is checked locally against the
     * same protection, and so are the head and the item read a fresh reader is positioned by. A refusal of
     * any of them means the operation did not happen, which is the one thing a caller needs in order to
     * wait and try again.
     *
     * <p>The read side is here for the same reason the write side is, and it was added later at the cost of
     * a defect: a reader positioned during a forming cluster met the refusal as a bare library exception,
     * which nothing on that path recognised, so the run ended instead of waiting.
     */
    private RuntimeException translate(RuntimeException raised) {
        return raised instanceof SplitBrainProtectionException refused
                ? new RingWriteRefusedException(ringbuffer.getName(), refused)
                : raised;
    }
}
