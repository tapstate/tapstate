package io.tapstate.spi.store;

import java.util.List;

/**
 * The durable coordination record for one mining chain — the offset and schema truth that outlives the
 * in-memory change ring. A mining chain is the shared cdc capture keyed by physical source coordinates,
 * so one record serves every table and every consumer pipeline on that chain.
 *
 * <p>Fields — {@code miningChainId} (the chain this record is keyed by), {@code sourceReadOffset} (the
 * opaque source capture watermark the chain has read up to; absent until the first cdc read; its
 * durable advance is bounded by the slowest consumer's acked position), {@code consumerOffsets} (one
 * cursor per consumer pipeline), {@code cdcStartPosition} (the opaque position the cdc tail starts from,
 * recorded at the snapshot-to-cdc seam; absent until a snapshot seam or start point resolves it),
 * {@code schemaHistory} (the append-only versioned schema), {@code retention} (the retention
 * configuration passed through from the source; a config value only — the change ring is bounded by
 * its capacity and backpressure, not trimmed by this), {@code snapshotCompletedTables} (the tables
 * whose bounded snapshot read has drained to completion), {@code epoch} (the change ring's current
 * generation, zero until one is opened) and {@code snapshotEpoch} (the generation the recorded snapshot
 * began in, zero until a snapshot records its seam).
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
 * <p>Snapshot completion is tracked per table while the read offset is a chain-level scalar, and the
 * asymmetry is deliberate. A chain is keyed by the physical source coordinate and deliberately excludes
 * the table subset, so sources reading different tables of one database share this record: the cdc tail
 * is one log read with one offset, but each table is snapshotted by its own capture run and finishes at
 * its own time. A chain-level completion flag could not say which table it meant. A table is listed only
 * once its own snapshot has drained — presence means drained, never merely started, which is what
 * {@code cdcStartPosition} means. Membership is a set: marking a table that is already listed changes
 * nothing.
 *
 * <p>The field set is append-only: a field may be added but never removed or repurposed, so an older
 * reader stays forward-compatible. The lists are unmodifiable defensive copies. A pure value over
 * {@code java..} only (rule R2): positions travel as opaque tokens, never as a connector type.
 */
public record SrsMeta(
        String miningChainId,
        String sourceReadOffset,
        List<ConsumerOffset> consumerOffsets,
        String cdcStartPosition,
        List<SchemaVersion> schemaHistory,
        String retention,
        List<String> snapshotCompletedTables,
        long epoch,
        long snapshotEpoch) {

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
        if (snapshotCompletedTables == null) {
            throw new IllegalArgumentException("srs meta snapshotCompletedTables must be set");
        }
        consumerOffsets = List.copyOf(consumerOffsets);
        schemaHistory = List.copyOf(schemaHistory);
        snapshotCompletedTables = List.copyOf(snapshotCompletedTables);
    }

    /** A record with no table marked snapshot-complete — the shape every producer predating the mark builds. */
    public SrsMeta(String miningChainId, String sourceReadOffset, List<ConsumerOffset> consumerOffsets,
            String cdcStartPosition, List<SchemaVersion> schemaHistory, String retention) {
        this(miningChainId, sourceReadOffset, consumerOffsets, cdcStartPosition, schemaHistory, retention,
                List.of());
    }

    /** A record with no generation opened and no snapshot pinned — the shape a freshly seeded chain has. */
    public SrsMeta(String miningChainId, String sourceReadOffset, List<ConsumerOffset> consumerOffsets,
            String cdcStartPosition, List<SchemaVersion> schemaHistory, String retention,
            List<String> snapshotCompletedTables) {
        this(miningChainId, sourceReadOffset, consumerOffsets, cdcStartPosition, schemaHistory, retention,
                snapshotCompletedTables, 0L, 0L);
    }
}
