package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;

import java.time.Instant;
import java.util.List;

/**
 * The durable coordination record for one mining chain — the offset and schema truth that outlives the
 * in-memory change ring. A mining chain is the shared cdc capture keyed by physical source coordinates,
 * so one record serves every table and every consumer pipeline on that chain.
 *
 * <p>Fields — {@code miningChainId} (the chain this record is keyed by), {@code sourceRead} (how far the
 * chain has read: the source's own position paired with the order the engine assigned it; absent until
 * the first cdc read; its durable advance is bounded by the slowest consumer's acked position, and it
 * only ever moves forward), {@code consumerOffsets} (one record per consumer pipeline — see
 * {@link ConsumerOffset} — carrying that pipeline's cursor, its acked position and which tables it has
 * finished loading), {@code cdcStartPosition} (the opaque position the cdc tail starts from,
 * recorded at the snapshot-to-cdc seam; absent until a snapshot seam or start point resolves it),
 * {@code schemaHistory} (the append-only versioned schema), {@code retention} (the retention
 * configuration passed through from the source; a config value only — the change ring is bounded by
 * its capacity and backpressure, not trimmed by this), {@code epoch} (the change ring's current
 * generation, zero until one is opened), {@code snapshotEpoch} (the generation the recorded snapshot
 * began in, zero until a snapshot records its seam) and {@code sourceReadAt} (when {@code sourceRead} was
 * last written, absent on a record whose offset predates the stamp).
 *
 * <p>{@code sourceReadAt} is here for the reader, not for the run: nothing branches on it. What it answers
 * is how old the recorded position is, which is the one thing that decides whether resuming from it is
 * still possible at all -- a source retains its change log for a window, and a position older than that
 * window is one no read can start from. An opaque token cannot be looked at and dated; this can.
 *
 * <p><strong>What is here is the chain's, and only the chain's.</strong> The line is which of the two
 * things a quantity answers for: the chain is one read of one source's change log, shared by everyone on
 * it, so how far that read has got, where its tail joins, and what the source's schema has been are the
 * chain's. Anything that answers for one pipeline's target belongs to that pipeline and lives in its
 * {@link ConsumerOffset} — the acked position, the read cursor, and which tables it has finished loading.
 * The last of those used to be recorded here, as one list for the whole chain, and a second pipeline on
 * the chain then read the first one's answer and skipped a load it had never done, leaving its target
 * short of every row of those tables with the run healthy and nothing logged.
 *
 * <p>The two generations are separate because a snapshot outlives the ring it started under. Every
 * restart or re-mine opens a new generation, and orders compare generation first — so a snapshot that
 * had not drained keeps the one it began in, and its rerun rows can never overwrite changes the earlier
 * generation already applied. Reading the current generation for a rerun's rows is precisely the
 * reversal these two fields exist to prevent, which is why one field cannot serve both. {@code
 * snapshotEpoch} is written in the same update as {@code cdcStartPosition}: the seam position is the
 * only record that a snapshot began, so a snapshot that resumes would otherwise have no way to know
 * what to pin its rows to.
 *
 * <p>The field set is append-only: a field may be added but never removed or repurposed, so an older
 * reader stays forward-compatible. That rule was broken once, deliberately and on the record, to move
 * snapshot completion out to {@link ConsumerOffset}: the guarantee protects readers of already-written
 * data, this product has not shipped, and so the set it protected was empty. Keeping the field as well
 * would have left two places recording one fact, which is the shape the defect above had. The rule holds
 * for every field named here, and the next removal needs its own argument.
 *
 * <p>The lists are unmodifiable defensive copies. A pure value over {@code java..} only (rule R2):
 * positions travel as opaque tokens, never as a connector type.
 */
public record SrsMeta(
        String miningChainId,
        ChainPosition sourceRead,
        List<ConsumerOffset> consumerOffsets,
        String cdcStartPosition,
        List<SchemaVersion> schemaHistory,
        String retention,
        long epoch,
        long snapshotEpoch,
        Instant sourceReadAt) {

    public SrsMeta {
        if (miningChainId == null || miningChainId.isBlank()) {
            throw new IllegalArgumentException("srs meta miningChainId must be non-blank");
        }
        if (epoch < 0 || snapshotEpoch < 0) {
            throw new IllegalArgumentException(
                    "srs meta generations must not be negative, got epoch " + epoch
                            + " and snapshotEpoch " + snapshotEpoch);
        }
        if (consumerOffsets == null) {
            throw new IllegalArgumentException("srs meta consumerOffsets must be set");
        }
        if (schemaHistory == null) {
            throw new IllegalArgumentException("srs meta schemaHistory must be set");
        }
        consumerOffsets = List.copyOf(consumerOffsets);
        schemaHistory = List.copyOf(schemaHistory);
    }

    /**
     * The token the chain has read up to, or null before its first cdc read — what a resuming read is
     * started from, and the only half a connector understands.
     */
    public String sourceReadOffset() {
        return sourceRead == null ? null : sourceRead.token();
    }

    /**
     * The tables {@code pipelineId} has finished loading, empty when it has finished none or is not on
     * this chain — the one reading of snapshot completion, taken from that pipeline's own record.
     *
     * <p>An absent consumer answers "none" rather than refusing, because that is the same answer for the
     * same reason: a pipeline with no record here has confirmed nothing, and a pipeline new to the chain
     * owes every table it selected.
     */
    public List<String> snapshotCompletedTables(String pipelineId) {
        return consumerOffsets.stream()
                .filter(consumer -> consumer.pipelineId().equals(pipelineId))
                .findFirst()
                .map(ConsumerOffset::snapshotCompletedTables)
                .orElse(List.of());
    }

    /** A record with no generation opened and no snapshot pinned — the shape a freshly seeded chain has. */
    public SrsMeta(String miningChainId, ChainPosition sourceRead, List<ConsumerOffset> consumerOffsets,
            String cdcStartPosition, List<SchemaVersion> schemaHistory, String retention) {
        this(miningChainId, sourceRead, consumerOffsets, cdcStartPosition, schemaHistory, retention,
                0L, 0L);
    }

    /**
     * The same record with no time recorded against its offset — the shape callers used before the stamp
     * was added, and the one a record written by an earlier version reads back as. Backward compatible:
     * {@code sourceReadAt} reads null, which is absence rather than a moment at the epoch.
     */
    public SrsMeta(String miningChainId, ChainPosition sourceRead, List<ConsumerOffset> consumerOffsets,
            String cdcStartPosition, List<SchemaVersion> schemaHistory, String retention,
            long epoch, long snapshotEpoch) {
        this(miningChainId, sourceRead, consumerOffsets, cdcStartPosition, schemaHistory, retention,
                epoch, snapshotEpoch, null);
    }
}
