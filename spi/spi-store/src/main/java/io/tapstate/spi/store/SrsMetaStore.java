package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;
import java.util.List;
import java.util.Map;
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
 * consumer's cursor or snapshot start, or the schema history. A mutate on a chain that has not been
 * seeded is a caller ordering error, surfaced bare (an {@code IllegalStateException}), not laundered into
 * a coded diagnostic that would hide the defect. The durable-frontier bound on a source-read-offset
 * advance (an advance must not pass the slowest consumer's acked position) is the caller's concern; this
 * store persists the value the caller resolved.
 */
public interface SrsMetaStore {

    default CaptureResumeWitness resumeWitness(String sourceId, String connectorId, String chain,
            String consumerId, io.tapstate.core.model.ReadMode mode, boolean shared, List<String> tables) {
        return CaptureResumeWitness.from(sourceId, connectorId, chain, consumerId, mode, shared, tables, read(chain));
    }

    /** Records the exact pre-open facts under the pipeline's real successor claim, never a read-time UUID. */
    default boolean prepareCaptureResume(WorkloadClaimFence pipelineClaim, CaptureResumeWitness witness,
            ClusterRecoveryPosition requestedPosition) {
        return false;
    }

    default Optional<CaptureReadAttempt> beginCaptureReadAttempt(String chain, long epoch, List<String> tables,
            CaptureReadAttempt.Kind kind, String requestedToken, java.time.Instant requestedInstant,
            WorkloadClaimFence captureClaim) {
        return Optional.empty();
    }

    default boolean recordCaptureAnchor(CaptureReadAttempt attempt, String anchor) { return false; }
    default boolean recordCaptureFirstDelivery(CaptureReadAttempt attempt) { return false; }
    default boolean recordCaptureReadFailure(CaptureReadAttempt attempt, String code) { return false; }
    default Optional<CaptureReadState> captureReadState(String chain) { return Optional.empty(); }

    /** A snapshot-only read proves its own different mode, without inventing a CDC position. */
    default boolean recordSnapshotStartup(WorkloadClaimFence pipelineClaim, CaptureResumeWitness witness) { return false; }

    /** Records a requested table union for one physical capture without broadening any consumer. */
    default void requestCaptureTables(String miningChainId, List<String> tables) {
    }

    /** The physical capture's durable requested table set, independent of source-node consumption. */
    default List<String> captureTables(String miningChainId) {
        return List.of();
    }

    /** Tables actually served by the current physical capture, not merely requested by a consumer. */
    default List<String> captureServingTables(String miningChainId) {
        return List.of();
    }

    /** Publishes a same-generation subscription only when it covers every current request. */
    default boolean publishCaptureTables(String miningChainId, long epoch, List<String> tables) {
        return true;
    }

    /**
     * The chain's source read offset as it stands durably, with whether it is a write-through checkpoint;
     * empty for a chain with no offset or no record.
     *
     * <p>Read so that only a write a majority of the store's members has taken is seen. This is the value a
     * source may be told to release its change log up to, and a write the store could still roll back in a
     * failover must never be told to one: the source would have let go of changes that the rolled-back record
     * then asks it for again. Only the offset is read, never the whole record, which grows with every schema
     * version. The default reads the record as {@link #read} does, which is durable for a store with no
     * replicas to fail over to.
     */
    default Optional<DurableSourceRead> durableSourceRead(String miningChainId) {
        return read(miningChainId).filter(record -> record.sourceRead() != null)
                .map(record -> new DurableSourceRead(record.sourceRead(), record.sourceReadDurable()));
    }

    /**
     * Checkpoints the physical capture after every change in its source batch was written to recoverable
     * SRS. Consumer confirmations are independent; they never certify this source-database position.
     */
    default void advanceCaptureCheckpoint(String miningChainId, ChainPosition position) {
        throw new UnsupportedOperationException("a durable capture checkpoint requires its served table selection");
    }

    /**
     * Checkpoints only while this callback's immutable served selection covers every requested table.
     * An older narrow callback cannot certify a position after a wider subscription is published.
     * Backends must make the selection check atomic with the checkpoint write.
     */
    default void advanceCaptureCheckpoint(
            String miningChainId, ChainPosition position, List<String> servedTables) {
        throw new UnsupportedOperationException("durable capture checkpoints are not implemented by this store");
    }

    /** Starts one isolated direct stream without carrying pending batches into a new source generation. */
    default void beginDirectCapture(String miningChainId, String consumerId, long epoch, String anchor) {
    }

    /**
     * Records the source-stream batch boundary and the last event each selected table must confirm.
     * Orders here belong to one direct channel; quiet tables with no event do not pin its checkpoint.
     */
    default void recordDirectBatch(String miningChainId, String consumerId, ChainPosition position,
            Map<String, Long> targets) {
    }

    /** Returns the meta record for a mining chain, or empty if the chain has not been seeded. */
    Optional<SrsMeta> read(String miningChainId);

    /**
     * The chain's consumer cursors alone, empty when the chain has not been seeded — what the cdc write
     * path needs, and all of it. Both bounds that path applies are functions of these cursors: the
     * headroom the ring has left, and how far the durable read offset may advance.
     *
     * <p>Separate from {@link #read} because the record also carries a schema history — one entry per DDL,
     * bounded by what the store retains of it — and this path never looks at it. Fetching the whole record
     * on every run of changes therefore carries that history back across the wire each time, and the cost
     * of doing so grows with what the record holds for the life of the chain. A store that can answer this
     * without the history stops paying for it; one that cannot is still correct, which is why this has a
     * default at all.
     */
    default List<ConsumerOffset> consumerOffsets(String miningChainId) {
        return read(miningChainId).map(SrsMeta::consumerOffsets).orElse(List.of());
    }

    /**
     * Seeds a mining chain's first record — no offsets, no consumers, no schema history, carrying only
     * the pass-through {@code retention} config (which may be absent). Insert-only: it must not overwrite
     * an existing record (which would discard the chain's accumulated offset / cursor / schema truth).
     */
    void create(String miningChainId, String retention);

    /**
     * Advances the chain's source read offset to {@code position} — the source's own token paired with
     * the order the engine assigned it. The durable-frontier bound (an advance must not pass the slowest
     * consumer's acked position) is the caller's concern; this persists the resolved value. A mutate on
     * an unseeded chain is a caller ordering error.
     *
     * <p><strong>It only ever moves forward.</strong> A position that does not rank after the one already
     * recorded is ignored, silently and successfully — not an error, because two writers racing to advance
     * the same chain is ordinary, and the loser has nothing to report. This is a store guarantee rather
     * than a caller convention because the failure it prevents is invisible in every other way: a restart
     * raises the generation while the consumers' acked positions still sit in the generation before it, so
     * the clamp resolves to a position the chain has already passed. Written down, the next restart resumes
     * from there and re-mines everything after it — or, once the source has aged past it, cannot.
     *
     * <p>Both halves are persisted. The token is what a read resumes from; the order is what the next
     * comparison — this one included — runs on, and a stored token without it can no longer be ranked
     * against anything.
     */
    void advanceSourceReadOffset(String miningChainId, ChainPosition position);

    /**
     * Puts the chain's source read offset at exactly {@code token}, forward or back, and drops the order
     * recorded beside it.
     *
     * <p>The write-back path, and the only one that may move the offset backwards.
     * {@link #advanceSourceReadOffset} carries its ordering condition inside its own filter, so handed an
     * earlier position it matches nothing — silently, because for a reader that is the ordinary case of a
     * clamp to a consumer that is behind. A rewind routed through it would report success and change
     * nothing.
     *
     * <p>The order goes because there is no truthful value to put there. An order is where the engine
     * observed that token in the ring; a written-back position names a spot in the source's own log,
     * which was never observed here and may predate every ring this chain has had. Writing one anyway
     * would put a made-up coordinate into the comparison that decides which changes are safe to forget.
     * With none recorded the next advance is admitted whatever generation it carries, which is the state
     * a fresh run comes back in regardless.
     *
     * <p>A mutate on an unseeded chain is a caller ordering error, as with every other mutator here.
     */
    void rewindSourceReadOffset(String miningChainId, String token);

    /**
     * Inserts or replaces one consumer pipeline's cursor on the chain, keyed by its pipeline id. A
     * mutate on an unseeded chain is a caller ordering error.
     */
    void upsertConsumerOffset(String miningChainId, ConsumerOffset offset);

    /**
     * Advances one consumer pipeline's read cursor into one table's change ring — a scoped raise of that
     * consumer's {@code perTableSeq} entry for the table alone. A call with {@code -1} registers a table
     * before its reader publishes progress without lowering a cursor that already exists. It touches only
     * the read cursor, so a reader advancing here never clobbers the {@code sinkAckedSrcpos} the sink writes to the
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
     * Advances the sink-acked position as {@link #advanceSinkAcked(String, String, ChainPosition)} does, and
     * records with it where in {@code table}'s own change ring that change sat: the ring sequence the order
     * carries. A run that replaces this pipeline's run carries on from just past it, rather than from the
     * head of the ring.
     *
     * <p>Kept per table because the chain's acked position is one pair for every table a source has, while
     * each table has a ring -- and a sequence space -- of its own. The chain position says how far the source
     * has been confirmed; it cannot say where in any one ring that was, and positioning a table's ring by
     * another table's sequence would skip changes nobody confirmed. Only ever raised, never lowered.
     *
     * <p>A position no later than the last one recorded for {@code table} leaves the chain position where it
     * is, and still raises the table's place in its ring: every writer of a sink reports on its own, so a
     * report worked out before a later one can land after it, and written it would move the position back.
     * Only the table's own last position is compared: each table's ring numbers its changes on its own, so
     * one table's position says nothing about another's, and the last position recorded that moved its own
     * table on stands. A snapshot row sits beneath every change of its generation and raises no ring place.
     *
     * <p>The default records the chain position alone, compared with nothing, which leaves a replacing run
     * starting at the head of each ring as runs always have: more replayed than needed, nothing missed.
     */
    default void advanceSinkAcked(
            String miningChainId, String pipelineId, String table, ChainPosition position) {
        advanceSinkAcked(miningChainId, pipelineId, position);
    }

    /**
     * The store-fenced form of {@link #advanceSinkAcked(String, String, String, ChainPosition)}. The consumer
     * must already be bound to {@code fence}'s run by {@link #beginWriterRun(String, String, String, Map,
     * WorkloadClaimFence)}, and {@code fence} must still be the live claim, proved in the same store operation
     * as the write: a stale or differently bound advance is ignored, and answers false. Stores that cannot
     * enforce that condition refuse the fenced operation rather than silently falling back to the unfenced
     * contract.
     */
    default boolean advanceSinkAcked(
            String miningChainId,
            String pipelineId,
            String table,
            ChainPosition position,
            WorkloadClaimFence fence) {
        throw new UnsupportedOperationException("this SRS meta store does not support fenced sink acknowledgements");
    }

    /**
     * Starts {@code runId}'s writer accounting for {@code pipelineId} on the chain, replacing whatever run's
     * accounting was there: each table maps to the writers its changes are expected to reach in this run.
     *
     * <p>Replacing rather than merging is what keeps a writer of the run being replaced from standing for a
     * writer of this one: its progress is dropped with its run, and nothing it writes afterwards lands. What
     * the replaced run proved has already been carried into the pipeline's own record as it was proved, so
     * dropping the per-writer detail loses nothing a resume reads. A mutate on an unseeded chain is a caller
     * ordering error.
     */
    default void beginWriterRun(String miningChainId, String pipelineId, String runId,
            Map<String, List<String>> expectedWritersByTable) {
        throw new UnsupportedOperationException("this store keeps no per-writer accounting");
    }

    /**
     * Starts {@code runId}'s writer accounting as {@link #beginWriterRun(String, String, String, Map)} does, and
     * records with it what the consumer's progress is measured against: {@code kind}, which says which of the
     * positions its record holds a run replacing this one may resume from.
     *
     * <p>A consumer named for its source node, whose pipeline still holds progress recorded under the
     * pipeline's own name on the chain, is refused: that progress cannot say which source node it was made
     * for, so neither carrying it over nor dropping it is safe, and the pipeline's state has to be cleared.
     * Read cursors and a snapshot seam are not progress any sink made, and do not count.
     */
    default void beginWriterRun(String miningChainId, String consumerId, String runId,
            Map<String, List<String>> expectedWritersByTable, ConsumerProgressKind kind) {
        throw new UnsupportedOperationException("this store keeps no per-writer accounting");
    }

    /** The fenced form of {@link #beginWriterRun(String, String, String, Map, ConsumerProgressKind)}. */
    default boolean beginWriterRun(String miningChainId, String consumerId, String runId,
            Map<String, List<String>> expectedWritersByTable, ConsumerProgressKind kind, WorkloadClaimFence fence) {
        throw new UnsupportedOperationException("this SRS meta store does not support fenced sink acknowledgements");
    }

    /**
     * Records how far {@code consumerId} has durably landed {@code table}: the position every change of the
     * table at or below which has landed - its order, and the token of the change there where it carried one -
     * with the table's place in its own ring raised to the order's sequence. Only ever raised: a position no
     * later than the one the table holds leaves it, as every writer reports on its own and an older answer can
     * land after a newer one. The consumer's acked position is left as it is, since one table's progress says
     * nothing about how far the source has been confirmed for the others.
     */
    default void advanceTableConfirmed(String miningChainId, String consumerId, String table,
            ChainPosition confirmed) {
        throw new UnsupportedOperationException("this store keeps no per-table confirmations");
    }

    /** The fenced form of {@link #advanceTableConfirmed}, under the same condition as the fenced advance. */
    default boolean advanceTableConfirmed(String miningChainId, String consumerId, String table,
            ChainPosition confirmed, WorkloadClaimFence fence) {
        throw new UnsupportedOperationException("this SRS meta store does not support fenced sink acknowledgements");
    }

    /**
     * Moves a direct channel's consumer on as far as its tables' confirmations now reach. Its tables share one
     * source order, so the acked position - and the channel's checkpoint with it - moves as far as the source
     * batches recorded for it ({@link #recordDirectBatch}) are complete: every table a batch carried a change of
     * confirmed through that change, a table it carried none of holding nothing back. Where it has recorded no
     * batch, the acked position moves to the lowest of its tables' confirmations, once every table of its run has
     * one. Only ever raised. What it decides comes from what is stored alone, so settling twice, or late, settles
     * the same.
     */
    default void settleDirectBatches(String miningChainId, String consumerId) {
    }

    /**
     * Raises {@code consumerId}'s acked position to {@code position} where the one it holds is earlier, or where
     * it holds none, and leaves it otherwise: every writer works the position out on its own, so an older
     * answer can land after a newer one, and written as it came it would move the position back.
     */
    default void raiseSinkAcked(String miningChainId, String consumerId, ChainPosition position) {
        throw new UnsupportedOperationException("this store keeps no per-writer accounting");
    }

    /** The fenced form of {@link #raiseSinkAcked}, under the same condition as the fenced advance. */
    default boolean raiseSinkAcked(String miningChainId, String consumerId, ChainPosition position,
            WorkloadClaimFence fence) {
        throw new UnsupportedOperationException("this SRS meta store does not support fenced sink acknowledgements");
    }

    /**
     * Starts {@code runId}'s writer accounting as {@link #beginWriterRun(String, String, String, Map)} does, and
     * binds every later durable sink effect of the pipeline on the chain to {@code fence}'s run. The binding and
     * the proof that {@code fence} is still the live claim are one store operation, so a superseded run can
     * neither start its accounting again nor take the binding back. False, with nothing written, where
     * {@code fence} is not the live claim.
     */
    default boolean beginWriterRun(String miningChainId, String pipelineId, String runId,
            Map<String, List<String>> expectedWritersByTable, WorkloadClaimFence fence) {
        throw new UnsupportedOperationException("this SRS meta store does not support fenced sink acknowledgements");
    }

    /**
     * Records how far {@code writerId} of {@code runId} has durably landed {@code table}'s changes, and answers
     * the run's accounting as it stands after the write - or empty where {@code runId} is no longer the
     * chain's current run for the pipeline, in which case nothing was written.
     *
     * <p>Each writer reports only its own progress and only ever forward, so the write is a plain replacement
     * of that writer's entry; the store's part is refusing a run that has been replaced.
     */
    default Optional<WriterRun> advanceWriter(String miningChainId, String pipelineId, String runId,
            String writerId, String table, WriterProgress progress) {
        throw new UnsupportedOperationException("this store keeps no per-writer accounting");
    }

    /** The pipeline's current writer accounting on the chain, or empty where no run has begun one. */
    default Optional<WriterRun> writerRun(String miningChainId, String pipelineId) {
        return Optional.empty();
    }

    /**
     * The store-fenced form of {@link #advanceWriter}: also empty, with nothing written, where {@code fence} is
     * no longer the live claim or the pipeline is bound to another run, proved in the same store operation as
     * the write.
     */
    default Optional<WriterRun> advanceWriter(String miningChainId, String pipelineId, String runId,
            String writerId, String table, WriterProgress progress, WorkloadClaimFence fence) {
        throw new UnsupportedOperationException("this SRS meta store does not support fenced sink acknowledgements");
    }

    /**
     * Per table, the ring sequence up to which this pipeline has nothing left to receive from that table's
     * change ring: the last change its sink confirmed there, or where the ring stood when the pipeline
     * arrived on it, whichever {@link #advanceSinkAcked(String, String, String, ChainPosition)} and
     * {@link #startRingAfter(String, String, String, long)} left higher. A run of this pipeline carries on
     * just past it. A table with neither is absent, and so is everything once the pipeline's record is
     * rewritten or dropped. Empty where nothing is recorded, which is also what the default answers.
     */
    default Map<String, Long> ringDoneThrough(String miningChainId, String pipelineId) {
        return Map.of();
    }

    /**
     * Records that a pipeline arriving on {@code table}'s change ring starts just past {@code seq} -- where
     * the ring stood as it arrived -- unless the pipeline already has a place in that ring, which it keeps:
     * a run coming back carries on from where it was, not from wherever the ring has got to since.
     * A newly recorded arrival also registers the table in that consumer's {@code perTableSeq}, at the
     * same sequence and in the same atomic write. The arrival says the consumer owes nothing through that
     * sequence, so those changes must not pin write headroom; publishing the arrival without the cursor,
     * however, leaves a gap in which the write path mistakes the consumer for one that does not subscribe
     * to the table and may overwrite changes written after it arrived.
     *
     * <p>The mark is taken before the pipeline's own load reads anything and before any tail this run opens
     * has mined anything, so every change the pipeline is owed lands above it, and everything below it is
     * either history from before the pipeline existed or a change its own load already read.
     *
     * <p>The default records nothing, which leaves an arriving run starting where its read mode puts it.
     */
    default void startRingAfter(String miningChainId, String pipelineId, String table, long seq) {
    }

    /**
     * Records one pipeline's snapshot-to-cdc seam: the opaque position its cdc tail starts from, together
     * with the ring generation that pipeline's snapshot began in. A mutate on an unseeded chain is a
     * caller ordering error. The consumer entry is created when the pipeline has none yet, and only these
     * two fields are touched.
     *
     * <p>The two are one call because they are only ever read together. The seam position is the sole
     * record that a snapshot began at all, so a snapshot resuming after a restart looks here to learn
     * both where the tail picks up and which generation to pin its rows to. A store that could write the
     * position without its generation would leave a resumed snapshot with nothing to pin to, and a rerun
     * that then took the current generation would overwrite changes the earlier one had already applied.
     */
    void setCdcStart(String miningChainId, String pipelineId, String cdcStartPosition, long snapshotEpoch);

    /**
     * Opens the chain's next ring generation and returns it — the monotonic counter every order on this
     * chain compares first. Generations begin at one, so a chain whose stored generation is still zero has
     * never had a ring opened.
     *
     * <p>Called once per ring establishment: a restart or a re-mine rebuilds the ring and takes a new
     * generation, while a second source force-merging onto an already-open chain joins the generation
     * already running rather than opening one. Generations are per chain, because an order is only ever
     * compared against another order of the same chain. It leaves every pipeline's recorded snapshot
     * generation alone — that is the whole point of keeping them apart. A mutate on an unseeded chain is
     * a caller ordering error.
     */
    long openEpoch(String miningChainId);

    /**
     * Appends a version to the chain's schema history — the version just appended is always recorded. A
     * mutate on an unseeded chain is a caller ordering error.
     *
     * <p>A store may bound how much of the history it retains, dropping the oldest versions once the bound
     * is reached, so a caller must not assume every version ever appended is still there. What it may
     * assume is that the version it just appended is, because recording a schema change is the whole of
     * what this call is for. The bound belongs to the store rather than to this contract because the room
     * it is measured against is the store's own — a record that is one document under a fixed ceiling, of
     * which this history is the only facet that grows for the life of a chain. A store that kept every
     * version arrives at a state where this call, the one write that can record a schema change, is the
     * write the store refuses.
     */
    void appendSchemaVersion(String miningChainId, SchemaVersion version);

    /**
     * Marks one table's bounded snapshot read as drained to completion <em>for one consumer pipeline</em>.
     * The caller marks a table only once that pipeline's sink has confirmed that table's rows, so a reader
     * may take the mark as "every row of this table is in this pipeline's target" — a distinct question
     * from the one {@link #setCdcStart} answers, which is where that pipeline's tail resumes and is written before the
     * snapshot begins.
     *
     * <p>Per table because one chain carries many, each snapshotted by its own capture run; per pipeline
     * because each pipeline writes to a target of its own. A chain excludes the table subset from its
     * identity, so two pipelines reading one database share a chain by construction — and a mark recorded
     * against the chain alone would answer the second pipeline's question with the first one's answer. It
     * would then skip a load it had never done and leave its target short of every row of that table, with
     * the run healthy and nothing logged.
     *
     * <p>It creates the consumer entry when the pipeline has none yet, and it touches only the completion
     * set, so it never clobbers the {@code perTableSeq} the pipeline's reader writes or the
     * {@code sinkAckedSrcpos} its sink writes to the same record. Marking is idempotent (set membership):
     * a table already marked stays marked once, so a re-run or replay of a table's snapshot is safe. A
     * mutate on an unseeded chain is a caller ordering error.
     */
    void markSnapshotComplete(String miningChainId, String pipelineId, String table);

    /** The store-fenced form of a snapshot-completion mark, under the same condition as the fenced advance. */
    default boolean markSnapshotComplete(
            String miningChainId, String pipelineId, String table, WorkloadClaimFence fence) {
        throw new UnsupportedOperationException("this SRS meta store does not support fenced sink acknowledgements");
    }

    /**
     * Lists the id of every mining chain that carries a cursor for {@code pipelineId} — exactly the
     * chains a departing consumer still has to be detached from. It answers from durable cursor membership,
     * so a caller that cannot derive which chains a pipeline reads (chain identity is resolved where captures
     * are built, not where artifacts are removed) can still detach from all of them, and a chain the pipeline
     * never joined is never touched.
     *
     * <p>It returns ids only, never reconstructed records, so enumerating never fails on a single corrupt
     * document.
     */
    List<String> miningChainIdsWithConsumer(String pipelineId);

    /**
     * Removes one consumer pipeline's cursor from a chain, leaving the chain itself and every other
     * consumer untouched. A departing pipeline <em>must</em> be detached: its cursor is folded into two
     * independent minimums — the durable frontier over every consumer's acked position, and the cdc
     * write headroom over every consumer's read cursor — and a consumer that will never advance again
     * pins both, permanently and silently, for every other pipeline on the shared chain.
     *
     * <p>It removes the whole of that consumer's record, the tables it had finished loading included. That
     * is what makes a pipeline asked to re-read everything actually re-read it while others stay on the
     * chain: its completion marks are its own, so clearing them decides nothing on anybody else's behalf.
     *
     * <p>Unlike the other mutators, this one is idempotent rather than an ordering error on an unseeded
     * chain: a detach states the end condition "this consumer holds nothing here", which an absent chain
     * and an absent cursor already satisfy. Refusing them would let one benign race abort a removal
     * partway and leave the consumer attached to the chains not yet reached — the very residue this
     * exists to prevent.
     */
    void detachConsumer(String miningChainId, String pipelineId);

    /**
     * Removes a mining chain's whole record — the read offset, every pipeline's seam, the schema
     * history, and every consumer's cursor with it. Only the last pipeline to leave a chain may call this:
     * what it removes is shared, and taking it away while another pipeline reads the chain would have that
     * pipeline read its whole source again with nothing anywhere saying why. A pipeline leaving a chain
     * others are still on gives back its own record instead — {@link #detachConsumer}.
     *
     * <p>Idempotent rather than an ordering error on an absent chain, for the reason
     * {@link #detachConsumer} is: this states the end condition "there is no record for this chain",
     * which an absent one already satisfies, and refusing it would let one benign race abandon a
     * clearing partway.
     */
    void dropChain(String miningChainId);
}
