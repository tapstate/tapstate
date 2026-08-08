package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;
import java.util.Optional;

/**
 * The durable SRS coordination store: one {@link SrsMeta} record per mining chain — the offset, consumer
 * cursor and schema truth that outlives the in-memory change ring. A pure interface over the store's own
 * value model (rule R2); a store backend persists it, and positions travel as opaque tokens, never as a
 * connector type.
 *
 * <p>{@link #create} seeds a chain's first record, carrying only the pass-through retention config. It is
 * insert-only: it must not overwrite an existing record, because doing so would discard the offset,
 * consumer-cursor and schema history the chain has accumulated. Seeding a chain that already has a record
 * is a caller ordering error; a caller that needs to know can {@link #read} first.
 *
 * <p>The mutators each update one facet of an already-seeded record — the source read offset, one
 * consumer's cursor, the cdc start position, or the schema history. A mutate on a chain that has not been
 * seeded is a caller ordering error, surfaced bare (an {@code IllegalStateException}), not laundered into
 * a coded diagnostic that would hide the defect. The durable-frontier bound on a source-read-offset
 * advance (an advance must not pass the slowest consumer's acked position) is the caller's concern; this
 * store persists the value the caller resolved.
 */
public interface SrsMetaStore {

    /** Returns the meta record for a mining chain, or empty if the chain has not been seeded. */
    Optional<SrsMeta> read(String miningChainId);

    /**
     * Seeds a mining chain's first record — no offsets, no consumers, no schema history, carrying only
     * the pass-through {@code retention} config (which may be absent). Insert-only: it must not overwrite
     * an existing record (which would discard the chain's accumulated offset / cursor / schema truth).
     */
    void create(String miningChainId, String retention);

    /**
     * Sets the chain's source read offset to {@code sourceReadOffset}, an opaque source capture
     * watermark. The durable-frontier bound is the caller's concern; this persists the resolved value.
     * A mutate on an unseeded chain is a caller ordering error.
     */
    void advanceSourceReadOffset(String miningChainId, String sourceReadOffset);

    /**
     * Inserts or replaces one consumer pipeline's cursor on the chain, keyed by its pipeline id. A
     * mutate on an unseeded chain is a caller ordering error.
     */
    void upsertConsumerOffset(String miningChainId, ConsumerOffset offset);

    /**
     * Advances one consumer pipeline's read cursor into one table's change ring — a scoped set of that
     * consumer's {@code perTableSeq} entry for the table alone. It touches only the read cursor, so a
     * reader advancing here never clobbers the {@code sinkAckedSrcpos} the pipeline's sink writes to the
     * same consumer record: the read cursor and the sink-ack are independent writers of one consumer, of
     * different lifetime. It creates the consumer entry when the pipeline has none yet, so a reader may
     * advance before the sink first acks. A mutate on an unseeded chain is a caller ordering error.
     */
    void advanceConsumerReadSeq(String miningChainId, String pipelineId, String table, long lastReadSeq);

    /**
     * Advances one consumer pipeline's durable sink-acked source position on the chain — a scoped set of
     * that consumer's {@code sinkAckedSrcpos} alone. It touches only the sink-ack, so a sink advancing here
     * never clobbers the {@code perTableSeq} read cursor the pipeline's reader writes to the same consumer
     * record: the sink-ack and the read cursor are independent writers of one consumer, of different
     * lifetime. It creates the consumer entry when the pipeline has none yet, so a sink may ack before the
     * reader first publishes a cursor. The caller only ever advances, never lowers; this store persists the
     * position the caller resolved. A mutate on an unseeded chain is a caller ordering error.
     *
     * <p>Both halves of the position are persisted. The token is what a read resumes from; the order is
     * what the next comparison runs on, and a stored token without it can no longer be ranked against the
     * reader's own position — which is the comparison that keeps a source read from passing the slowest
     * sink.
     */
    void advanceSinkAcked(String miningChainId, String pipelineId, ChainPosition position);

    /**
     * Records the chain's snapshot-to-cdc seam: the opaque position the cdc tail starts from, together
     * with the ring generation the snapshot that reached this seam began in. A mutate on an unseeded chain
     * is a caller ordering error.
     *
     * <p>The two are one call because they are only ever read together. The seam position is the sole
     * record that a snapshot began at all, so a snapshot resuming after a restart looks here to learn
     * both where the tail picks up and which generation to pin its rows to. A store that could write the
     * position without its generation would leave a resumed snapshot with nothing to pin to, and a rerun
     * that then took the current generation would overwrite changes the earlier one had already applied.
     */
    void setCdcStart(String miningChainId, String cdcStartPosition, long snapshotEpoch);

    /**
     * Opens the chain's next ring generation and returns it — the monotonic counter every order on this
     * chain compares first. Generations begin at one, so a chain whose stored generation is still zero has
     * never had a ring opened.
     *
     * <p>Called once per ring establishment: a restart or a re-mine rebuilds the ring and takes a new
     * generation, while a second source force-merging onto an already-open chain joins the generation
     * already running rather than opening one. Generations are per chain, because an order is only ever
     * compared against another order of the same chain. It leaves the recorded snapshot generation alone —
     * that is the whole point of keeping the two apart. A mutate on an unseeded chain is a caller ordering
     * error.
     */
    long openEpoch(String miningChainId);

    /**
     * Appends a version to the chain's append-only schema history. A mutate on an unseeded chain is a
     * caller ordering error.
     */
    void appendSchemaVersion(String miningChainId, SchemaVersion version);

    /**
     * Marks one table's bounded snapshot read as drained to completion on the chain. The caller marks a
     * table only once that table's snapshot has finished, so a reader may take the mark as "every row of
     * this table has been through" — a distinct question from the one {@link #setCdcStart}
     * answers, which is where the tail resumes and is written before the snapshot begins. Completion is
     * per table because one chain carries many, each snapshotted by its own capture run. Marking is
     * idempotent (set membership): a table already marked stays marked once, so a re-run or replay of a
     * table's snapshot is safe. A mutate on an unseeded chain is a caller ordering error.
     */
    void markSnapshotComplete(String miningChainId, String table);
}
