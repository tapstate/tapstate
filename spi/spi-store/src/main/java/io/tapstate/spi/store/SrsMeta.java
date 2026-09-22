package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * The durable coordination record for one mining chain — the offset and schema truth that outlives the
 * in-memory change ring. A mining chain is the shared cdc capture keyed by physical source coordinates,
 * so one record serves every table and every consumer pipeline on that chain.
 *
 * <p>Fields — {@code miningChainId} (the chain this record is keyed by), {@code sourceRead} (how far the
 * chain has read: the source's own position paired with the order the engine assigned it; absent until
 * the first cdc read; its durable advance is bounded by the slowest consumer's acked position, and it
 * only ever moves forward), {@code consumerOffsets} (one record per consumer pipeline — see
 * {@link ConsumerOffset} — carrying that pipeline's cursor, acked position and snapshot state),
 * {@code schemaHistory} (the versioned schema, appended to on a schema change and holding as much of that
 * history as the store retains), {@code retention} (the retention configuration passed through from the source; a
 * config value only — the change ring is bounded by its capacity and backpressure, not trimmed by this),
 * {@code epoch} (the change ring's current generation, zero until one is opened) and
 * {@code sourceReadAt} (when {@code sourceRead} was last written, absent on a record whose offset
 * predates the stamp).
 *
 * <p>{@code sourceReadAt} is here for the reader, not for the run: nothing branches on it. What it answers
 * is how old the recorded position is, which is the one thing that decides whether resuming from it is
 * still possible at all -- a source retains its change log for a window, and a position older than that
 * window is one no read can start from. An opaque token cannot be looked at and dated; this can.
 *
 * <p><strong>What is here is the chain's, and only the chain's.</strong> The line is which of the two
 * things a quantity answers for: the chain is one read of one source's change log, shared by everyone on
 * it, so how far that read has got and what the source's schema has been are the
 * chain's. Anything that answers for one pipeline's target belongs to that pipeline and lives in its
 * {@link ConsumerOffset} — the acked position, the read cursor, which tables it has finished loading, and
 * the seam and generation its load began at. Snapshot completion and the seam are the same kind of fact:
 * recording either once for the chain lets a second pipeline read the first pipeline's answer.
 *
 * <p>The chain and snapshot generations are separate because a snapshot outlives the ring it started under. Every
 * restart or re-mine opens a new generation, and orders compare generation first — so a snapshot that
 * had not drained keeps the one it began in, and its rerun rows can never overwrite changes the earlier
 * generation already applied. The current chain generation therefore lives here while each pipeline's
 * pinned snapshot generation lives beside that pipeline's seam in {@link ConsumerOffset}.
 *
 * <p>The field set is append-only: a field may be added but never removed or repurposed, so an older
 * reader stays forward-compatible. That rule was broken once, deliberately and on the record, to move
 * pipeline-owned snapshot state out to {@link ConsumerOffset}: the guarantee protects readers of
 * already-written data, this product has not shipped, and so the set it protected was empty. Keeping the
 * fields as well would leave two places recording one fact. The rule holds for every field named here,
 * and the next removal needs its own argument.
 *
 * <p>The lists are unmodifiable defensive copies. A pure value over {@code java..} only (rule R2):
 * positions travel as opaque tokens, never as a connector type.
 */
public record SrsMeta(
        String miningChainId,
        ChainPosition sourceRead,
        List<ConsumerOffset> consumerOffsets,
        List<SchemaVersion> schemaHistory,
        String retention,
        long epoch,
        Instant sourceReadAt) {

    public SrsMeta {
        if (miningChainId == null || miningChainId.isBlank()) {
            throw new IllegalArgumentException("srs meta miningChainId must be non-blank");
        }
        if (epoch < 0) {
            throw new IllegalArgumentException("srs meta epoch must not be negative, got " + epoch);
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
        return consumerOffset(pipelineId)
                .map(ConsumerOffset::snapshotCompletedTables)
                .orElse(List.of());
    }

    /** The state recorded for {@code pipelineId}, or empty before that pipeline first writes this chain. */
    public Optional<ConsumerOffset> consumerOffset(String pipelineId) {
        return consumerOffsets.stream()
                .filter(consumer -> consumer.pipelineId().equals(pipelineId))
                .findFirst();
    }

    /** A record with no generation opened and no snapshot pinned — the shape a freshly seeded chain has. */
    public SrsMeta(String miningChainId, ChainPosition sourceRead, List<ConsumerOffset> consumerOffsets,
            List<SchemaVersion> schemaHistory, String retention) {
        this(miningChainId, sourceRead, consumerOffsets, schemaHistory, retention, 0L, null);
    }

    /**
     * The same record with no time recorded against its offset — the shape callers used before the stamp
     * was added, and the one a record written by an earlier version reads back as. Backward compatible:
     * {@code sourceReadAt} reads null, which is absence rather than a moment at the epoch.
     */
    public SrsMeta(String miningChainId, ChainPosition sourceRead, List<ConsumerOffset> consumerOffsets,
            List<SchemaVersion> schemaHistory, String retention, long epoch) {
        this(miningChainId, sourceRead, consumerOffsets, schemaHistory, retention, epoch, null);
    }
}
