package io.tapstate.spi.store;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
     * The durable highest-written and trimmed-through sequences. A store without retention metadata can
     * report its existing highest sequence but cannot claim that any prefix was retired. A recovery read
     * still has to prove every expected sequence rather than interpreting absence as permission to skip.
     */
    default SrsLogBounds bounds(String ring) {
        return new SrsLogBounds(largestSequence(ring), -1L);
    }

    /**
     * Reads at most {@code maxSize} consecutive sequence positions beginning at {@code firstSeq}, returning
     * each record under its actual sequence key. Missing positions stay missing so a caller can diagnose a
     * gap. The compatibility implementation uses exact reads; durable adapters can serve the same bounded
     * range in one query.
     */
    default SrsLogBatch readBatch(String ring, long firstSeq, int maxSize) {
        Objects.requireNonNull(ring, "ring");
        if (firstSeq < 0 || maxSize < 1) {
            throw new IllegalArgumentException("an SRS log read requires a non-negative sequence and positive size");
        }
        SrsLogBounds observed = bounds(ring);
        Map<Long, SrsLogRecord> records = new LinkedHashMap<>();
        long sequence = firstSeq;
        for (int i = 0; i < maxSize && sequence <= observed.largestSequence(); i++) {
            long at = sequence;
            load(ring, at).ifPresent(record -> records.put(at, record));
            if (sequence == Long.MAX_VALUE) {
                break;
            }
            sequence++;
        }
        return new SrsLogBatch(observed, records);
    }

    /**
     * Drops every change of {@code ring} at or below {@code throughSeq}. Without it the log grows without
     * bound; a change that every consumer of the chain has durably landed has no replay value left, and
     * that is the cut this performs. Which sequence is safe to cut at is the caller's to resolve -- this
     * store applies the resolved one.
     */
    void trim(String ring, long throughSeq);
}
