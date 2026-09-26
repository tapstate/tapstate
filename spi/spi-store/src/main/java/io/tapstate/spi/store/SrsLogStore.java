package io.tapstate.spi.store;

import java.util.List;
import java.util.Optional;

/**
 * The durable change log: every change that has entered a mining chain's per-table ring, kept so the
 * changes outlive the process that read them and so a ring can be rebuilt at the sequence it had
 * reached. A pure interface over the store's own value model (rule R2); positions travel as opaque
 * tokens, never as a connector type.
 *
 * <p>A change's identity is the pair (ring, sequence): the ring is the per-chain, per-table namespace it
 * was written under, and the sequence is the one that ring assigned.
 *
 * <p><strong>Writes are synchronous and happen before the change enters the ring.</strong> A write that
 * fails keeps the change out of the ring, which is what lets "in the ring" mean "already written down"
 * with no second reconciliation pass. Batching is what makes that affordable rather than a second round
 * trip per change: the cost of a durable write is per call, not per byte, so a run of changes written in
 * one act costs what one change costs.
 */
public interface SrsLogStore {

    /** Writes one change at {@code seq} of {@code ring}, replacing any record already at that key. */
    void store(String ring, long seq, SrsLogRecord record);

    /**
     * Writes a run of changes occupying consecutive sequences from {@code firstSeq} upward, in one act.
     * An empty run writes nothing. Partial success is not a state this can leave behind: either every
     * change of the run is written down or the call fails, so a caller that saw it return knows the whole
     * run is safe to admit to the ring.
     */
    void storeAll(String ring, long firstSeq, List<SrsLogRecord> records);

    /** The change at {@code seq} of {@code ring}, or empty when the log holds none there. */
    Optional<SrsLogRecord> load(String ring, long seq);

    /**
     * The largest sequence {@code ring} has ever been written at, or {@code -1} for a ring this log has
     * never seen. A rebuilt ring resumes numbering above this rather than from zero, so a sequence keeps
     * naming the same change across a restart.
     */
    long largestSequence(String ring);

    /**
     * Drops confirmed changes of one ring generation at or below {@code throughSeq}. Untagged legacy
     * records are retained. The store also retains the ring's highest sequence as a high-water marker,
     * so a rebuilt ring cannot reuse a trimmed sequence in the same generation.
     */
    void trim(String ring, long throughSeq, long ringEpoch);
}
