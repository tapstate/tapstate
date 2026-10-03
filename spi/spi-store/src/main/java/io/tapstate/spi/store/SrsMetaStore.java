package io.tapstate.spi.store;

import io.tapstate.core.event.ChainPosition;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

    /**
     * The tables the chain's one ring-backed reader subscribed to, in the ring generation it opened, and
     * which revision of that subscription this is within the generation.
     *
     * <p>A chain is read from its source once, for every pipeline on it, so what that one read subscribes
     * to is the union of what they select. A pipeline arriving with a table outside it cannot be served by
     * the running subscription: the source is not reading that table at all.
     */
    record PhysicalSelection(long epoch, long revision, List<String> tables) {

        public PhysicalSelection {
            if (epoch < 1 || revision < 1) {
                throw new IllegalArgumentException(
                        "a physical selection belongs to an opened generation and revision, got "
                                + epoch + "/" + revision);
            }
            if (tables == null || tables.isEmpty()
                    || tables.stream().anyMatch(table -> table == null || table.isBlank())) {
                throw new IllegalArgumentException("a physical selection names at least one table");
            }
            tables = tables.stream().distinct().sorted().toList();
        }

        /** The first subscription of a generation. */
        public PhysicalSelection(long epoch, List<String> tables) {
            this(epoch, 1L, tables);
        }
    }

    /** Returns the meta record for a mining chain, or empty if the chain has not been seeded. */
    Optional<SrsMeta> read(String miningChainId);

    /**
     * What the chain's ring-backed reader subscribed to, or empty when no reader has published one. A
     * selection from an earlier generation is still answered: whether it is current is the caller's to
     * judge against the generation it reads under. The default answers empty.
     */
    default Optional<PhysicalSelection> physicalSelection(String miningChainId) {
        return Optional.empty();
    }

    /**
     * Publishes the first subscription of a generation, while the chain is in that generation, nothing else
     * has been published for it -- or the same tables already have -- and it includes every table
     * {@linkplain #requestPhysicalTables requested}. Answers whether the chain now holds exactly this
     * selection; false means a request landed since the caller read them, another reader got there first, or
     * the chain moved on. A mutate on an unseeded chain is a caller ordering error.
     */
    default boolean publishPhysicalSelection(String miningChainId, PhysicalSelection selection) {
        throw new UnsupportedOperationException("this store does not record physical selections");
    }

    /**
     * Replaces the subscription published in a generation with a wider one of the same generation: only while
     * {@code current} is still exactly what is published and {@code wider} includes every table requested.
     * Answers whether it did. The reader replaces the stream {@code current} described once this lands,
     * beginning the new one from where the chain had been released to before it called. A mutate on an
     * unseeded chain is a caller ordering error.
     */
    default boolean replacePhysicalSelection(
            String miningChainId, PhysicalSelection current, PhysicalSelection wider) {
        throw new UnsupportedOperationException("this store does not record physical selections");
    }

    /** The tables pipelines arriving on the chain asked its reader to subscribe to; empty by default. */
    default List<String> requestedPhysicalTables(String miningChainId) {
        return List.of();
    }

    /**
     * Records that a pipeline arriving on the chain needs its reader to subscribe to {@code tables}, while the
     * chain is in ring generation {@code epoch}; answers whether it is.
     *
     * <p>A reader publishes a subscription only if it includes every table asked for, so a request recorded
     * before the reader publishes is served by that very subscription, and one recorded after it is found the
     * next time the reader looks. The pipeline goes on only once a published subscription includes its
     * tables: a pipeline that loaded a table the reader was not subscribed to would miss every change made
     * between its load and the moment the reader took the table on. A mutate on an unseeded chain is a
     * caller ordering error.
     */
    default boolean requestPhysicalTables(String miningChainId, long epoch, List<String> tables) {
        throw new UnsupportedOperationException("this store does not record physical selections");
    }

    /** Drops the requests for {@code tables}, which a published subscription now serves; others are kept. */
    default void clearPhysicalRequests(String miningChainId, List<String> tables) {
        throw new UnsupportedOperationException("this store does not record physical selections");
    }

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
     *
     * <p>The offset it leaves is {@linkplain #physicalPrefixTrusted trusted}: somebody chose it knowing
     * where every pipeline on the chain stands, which is the one proof an offset written before per-table
     * acknowledgements existed cannot otherwise get. It is also where the next restart resumes.
     */
    void rewindSourceReadOffset(String miningChainId, String token);

    /**
     * Advances the source read offset to a position the chain's reader has released -- every change up to
     * it landed for every consumer that selects a table it touched -- while the chain is still in ring
     * generation {@code epoch}, and only forward. Answers whether the chain is still in that generation.
     *
     * <p>Fenced by generation because only the reader holding the chain's current generation may say how
     * far its source was read: one that lost it to a newer reader is still running out its last callbacks,
     * and a release from it could carry the offset past what the newer one has not landed. It then changes
     * nothing and answers false. A position that does not rank after the one recorded is ignored, as
     * {@link #advanceSourceReadOffset} ignores one, and the answer is still true.
     *
     * <p>The position carries that generation in its order, and a token: it names where a read resumes. A
     * mutate on an unseeded chain is a caller ordering error.
     */
    default boolean advancePhysicalSourceReadOffset(String miningChainId, long epoch, ChainPosition position) {
        throw new UnsupportedOperationException("this store does not fence source read releases by generation");
    }

    /**
     * Advances the source read offset as {@link #advancePhysicalSourceReadOffset(String, long, ChainPosition)}
     * does, and makes the position the one a restart resumes from only when {@code resumable}: when the run
     * that named it carried a change.
     *
     * <p>A run carrying no change still says how far the source has been read, which is how far the source
     * may be told to release its log. It is not a point every source resumes after correctly: a source may
     * name it in the form it keeps for the last change handed over -- PostgreSQL names the end of the last
     * commit, which is where the next transaction's first change begins -- and a stream resumed there passes
     * over that change as one already read. Nothing lies between the last released change and a quiet run
     * released after it, so resuming from the change reads nothing that was not read before. The default
     * keeps the two together, which is what a store without the distinction did.
     */
    default boolean advancePhysicalSourceReadOffset(
            String miningChainId, long epoch, ChainPosition position, boolean resumable) {
        return advancePhysicalSourceReadOffset(miningChainId, epoch, position);
    }

    /**
     * The same split for {@link #advanceSourceReadOffset(String, ChainPosition)}, which a tail reading its
     * source directly writes through.
     */
    default void advanceSourceReadOffset(String miningChainId, ChainPosition position, boolean resumable) {
        advanceSourceReadOffset(miningChainId, position);
    }

    /**
     * Advances the source read offset as {@link #advanceSourceReadOffset(String, ChainPosition, boolean)} does,
     * for a tail reading its source directly while its pipeline is the only one on the chain, and marks the
     * offset {@linkplain #physicalPrefixTrusted trusted} where it moves it: that tail released it once its
     * pipeline had landed every change up to it on every table it reads, and that pipeline is everyone on the
     * chain. A pipeline turning the shared ring on later picks up there, over however many tables it reads. The
     * default leaves the mark off, as a store without the notion does.
     */
    default void advanceDirectSourceReadOffset(String miningChainId, ChainPosition position, boolean resumable) {
        advanceSourceReadOffset(miningChainId, position, resumable);
    }

    /**
     * Where a restart of the chain's reader resumes, and when that was written down: the position of the last
     * released run that carried a change, where the stream began, or a position put there by hand --
     * whichever was recorded last. Empty for a chain with no offset or no record. The default answers the
     * source read offset itself, which is what a store that does not keep the two apart, and every record
     * written before they were, means.
     */
    default Optional<ResumePoint> resumePoint(String miningChainId) {
        return read(miningChainId)
                .filter(meta -> meta.sourceReadOffset() != null)
                .map(meta -> new ResumePoint(meta.sourceRead(), meta.sourceReadAt()));
    }

    /** The token of {@link #resumePoint}: what a restart hands its source to resume from. */
    default Optional<String> resumeOffset(String miningChainId) {
        return resumePoint(miningChainId).map(point -> point.position().token());
    }

    /**
     * The chain's source read offset as it stands durably, or empty for a chain with no offset or no record.
     *
     * <p>Read so that only a write a majority of the store's members has taken is seen. This is the value a
     * source may be told to release its change log up to, and a write the store could still roll back in a
     * failover must never be told to one: the source would have let go of changes that the rolled-back record
     * then asks it for again. The default reads the record as {@link #read} does, which is durable for a store
     * with no replicas to fail over to.
     */
    default Optional<ChainPosition> durableSourceRead(String miningChainId) {
        return read(miningChainId).map(SrsMeta::sourceRead);
    }

    /**
     * One consumer's acknowledged position as it stands durably, or empty when there is none: read, like
     * {@link #durableSourceRead}, so that only a write a majority of the store's members has taken is seen.
     * It is what a tail reading its source directly for that one pipeline may tell its source to release up
     * to. The default reads the record as {@link #read} does.
     */
    default Optional<ChainPosition> durableSinkAcked(String miningChainId, String pipelineId) {
        return read(miningChainId).flatMap(meta -> meta.consumerOffset(pipelineId)).map(ConsumerOffset::sinkAcked);
    }

    /**
     * Whether the chain's source read offset is one every table it carries can resume from.
     *
     * <p>An offset is trusted once it was laid down as where a stream began, released by the chain's reader
     * across every table it reads, released by a direct tail while its pipeline was the only one on the chain,
     * adopted from a chain that provably carried a single table, or put there by hand. One written before any of those existed is not: while a chain carried several tables, one
     * table's acknowledgement could move it past another table's change that had not landed, and nothing
     * recorded says whether that happened. False for a chain with no record, and what a store without the
     * notion answers.
     */
    default boolean physicalPrefixTrusted(String miningChainId) {
        return false;
    }

    /**
     * Lays down where the chain's reader actually began -- the connector's own start position, ordered in
     * {@code position}'s generation beneath the first change of it -- as the chain's source read offset, and
     * marks the offset trusted. Answers whether the chain now holds a trusted offset in that generation.
     *
     * <p>Laid down when the chain holds no offset, or holds a trusted one from an earlier generation or with
     * no order at all: the new stream began where it began, and until one of its changes lands that is the
     * only point it can be resumed from. A trusted offset already in this generation is kept -- it is a
     * release the same reader made, or a start it already laid down. An offset that is not trusted is left
     * alone and the answer is false: laying a start over it would make it look proven. So is a chain that
     * has moved to another generation. Where it is laid down it is also where a restart resumes. A mutate on
     * an unseeded chain is a caller ordering error.
     */
    default boolean establishPhysicalAnchor(String miningChainId, ChainPosition position) {
        throw new UnsupportedOperationException("this store does not record physical anchors");
    }

    /**
     * Marks the chain's source read offset trusted without moving it, while the chain is in ring generation
     * {@code epoch}; answers whether it is.
     *
     * <p>For a caller that has established that every consumer of the chain reads one and the same table. On
     * such a chain an acknowledgement could only ever speak for that table, so the offset it bounded is
     * exact, and a later start over several tables may build on it. A mutate on an unseeded chain is a
     * caller ordering error.
     */
    default boolean trustSourceReadOffset(String miningChainId, long epoch) {
        throw new UnsupportedOperationException("this store does not record physical anchors");
    }

    /**
     * Inserts or replaces one consumer pipeline's cursor on the chain, keyed by its pipeline id. A
     * mutate on an unseeded chain is a caller ordering error.
     */
    void upsertConsumerOffset(String miningChainId, ConsumerOffset offset);

    /**
     * Lets go of everything one consumer is recorded as having landed -- its chain-level acknowledgement and its
     * per-table ones -- and leaves the rest of its record as it is: its read cursors, its selection, its finished
     * loads, where its own run begins, and its sink writers' plan and progress. Does nothing for a consumer the
     * chain does not record.
     *
     * <p>For a position moved by hand: what a pipeline landed must not outrank the move, while the work its
     * writers did still happened. Rewriting the record whole drops the writer plan instead, and a pipeline with
     * several writers that finished a load cannot prepare its writers again. The default rewrites the record from
     * a read of it, which is all a store keeping nothing beside these fields has to do.
     */
    default void releaseSinkAcknowledgements(String miningChainId, String pipelineId) {
        read(miningChainId).flatMap(meta -> meta.consumerOffset(pipelineId)).ifPresent(offset ->
                upsertConsumerOffset(miningChainId, new ConsumerOffset(offset.pipelineId(), offset.perTableSeq(),
                        null, offset.snapshotCompletedTables(), offset.cdcStartPosition(), offset.snapshotEpoch(),
                        offset.selectedTables(), offset.selectedTablesEpoch(), Map.of())));
    }

    /**
     * Makes {@code cdcStartPosition} where one consumer's own run begins and lets go of what it is recorded as
     * having landed, as {@link #releaseSinkAcknowledgements} does; everything else on its record stays. For a
     * position a pipeline reading directly is moved to -- by hand, or as it turns from the shared ring to a direct
     * tail of its own. The default rewrites the record from a read of it.
     */
    default void moveConsumerStart(String miningChainId, String pipelineId, String cdcStartPosition) {
        Objects.requireNonNull(cdcStartPosition, "cdcStartPosition");
        read(miningChainId).flatMap(meta -> meta.consumerOffset(pipelineId)).ifPresent(offset ->
                upsertConsumerOffset(miningChainId, new ConsumerOffset(offset.pipelineId(), offset.perTableSeq(),
                        null, offset.snapshotCompletedTables(), cdcStartPosition, offset.snapshotEpoch(),
                        offset.selectedTables(), offset.selectedTablesEpoch(), Map.of())));
    }

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
     * Advances one consumer's sink-acked position to a point its reader has released for it, while the chain
     * is still in ring generation {@code epoch}; answers whether it is.
     *
     * <p>The release is the reader's word that every change up to that point landed for this consumer on
     * every table it selects, which is what the consumer's chain-level position has to mean once the chain
     * carries several tables. Fenced by generation for the reason {@link #advancePhysicalSourceReadOffset}
     * is. Only forward, and never for a consumer the chain no longer records: a pipeline that left is not
     * brought back by a release it no longer needs. A mutate on an unseeded chain is a caller ordering error.
     */
    default boolean advancePhysicalSinkAcked(
            String miningChainId, String pipelineId, long epoch, ChainPosition position) {
        throw new UnsupportedOperationException("this store does not fence sink releases by generation");
    }

    /**
     * Writes down one run the chain's reader has released, as one act: each consumer's acknowledgement in
     * {@code acknowledged}, as {@link #advancePhysicalSinkAcked} advances one; then the source read offset to
     * {@code position}, as {@link #advancePhysicalSourceReadOffset(String, long, ChainPosition, boolean)}
     * advances it -- a restart resumes from {@code resumeAt} when that is the position, and from
     * {@code resumeAt} laid down first when the run ended in runs that carried no change after it; with no
     * {@code resumeAt}, no run released carried a change and the resume point is left where it is. Fenced by
     * generation as those are, and answers whether the chain is still in it; nothing is written when it is not.
     *
     * <p>A release is written on the thread a source hands its runs over on, so its cost is paid by every run;
     * a store that can write it in one go should. The default makes the separate calls.
     */
    default boolean advancePhysicalRelease(String miningChainId, long epoch,
            Map<String, ChainPosition> acknowledged, ChainPosition resumeAt, ChainPosition position) {
        for (Map.Entry<String, ChainPosition> landed : acknowledged.entrySet()) {
            if (!advancePhysicalSinkAcked(miningChainId, landed.getKey(), epoch, landed.getValue())) {
                return false;
            }
        }
        if (resumeAt != null && !resumeAt.equals(position)
                && !advancePhysicalSourceReadOffset(miningChainId, epoch, resumeAt, true)) {
            return false;
        }
        return advancePhysicalSourceReadOffset(miningChainId, epoch, position, position.equals(resumeAt));
    }

    /**
     * The store-fenced form of {@link #advanceSinkAcked(String, String, ChainPosition)}. The consumer must
     * already be bound to {@code fence} by fenced writer-plan configuration; a stale or differently bound
     * advance is ignored. Stores that cannot enforce that condition refuse the fenced operation rather than
     * silently falling back to the unfenced contract.
     */
    default void advanceSinkAcked(
            String miningChainId,
            String pipelineId,
            ChainPosition position,
            WorkloadClaimFence fence) {
        throw new UnsupportedOperationException("this SRS meta store does not support fenced sink acknowledgements");
    }

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
     * <p>The default records the chain position alone, which leaves a replacing run starting at the head of
     * each ring as runs always have: more replayed than needed, nothing missed.
     */
    default void advanceSinkAcked(
            String miningChainId, String pipelineId, String table, ChainPosition position) {
        advanceSinkAcked(miningChainId, pipelineId, position);
    }

    /** The store-fenced form of the table-aware sink acknowledgement. */
    default void advanceSinkAcked(
            String miningChainId,
            String pipelineId,
            String table,
            ChainPosition position,
            WorkloadClaimFence fence) {
        throw new UnsupportedOperationException("this SRS meta store does not support fenced sink acknowledgements");
    }

    /**
     * Records the complete set of sink writers expected to receive each table on this mining chain.
     * Writer-aware stores use this before acknowledgements begin so one writer cannot advance a
     * pipeline-level position on behalf of another. A writer-aware store must refuse to expand retained
     * aggregate progress into a plan naming multiple writers: an older record cannot prove which writer
     * reached that position, so the pipeline has to clear its retained state and either run a full resync
     * or explicitly accept a new incremental baseline. The default keeps older stores compatible with the
     * single-writer contract.
     */
    default void configureSinkWriters(
            String miningChainId, String pipelineId, Map<String, List<String>> writerIdsByTable) {
    }

    /**
     * Records the writer plan and binds its later durable sink effects to {@code fence}. The binding and
     * proof that the workload claim is still live are one store operation.
     */
    default void configureSinkWriters(
            String miningChainId,
            String pipelineId,
            Map<String, List<String>> writerIdsByTable,
            WorkloadClaimFence fence) {
        throw new UnsupportedOperationException("this SRS meta store does not support fenced sink acknowledgements");
    }

    /**
     * Records one writer's acknowledgement and takes what {@code table} has landed for the pipeline to be the
     * lowest every writer configured for the table has reached, as {@link #advanceTableSinkAcked} records it:
     * without moving the pipeline's chain-level acknowledgement, which the chain's reader releases. The
     * default is the single-writer case.
     */
    default void advanceSinkWriterAcked(
            String miningChainId,
            String pipelineId,
            String writerId,
            String table,
            ChainPosition position) {
        advanceTableSinkAcked(miningChainId, pipelineId, table, position);
    }

    /** The store-fenced form of one writer's acknowledgement. */
    default void advanceSinkWriterAcked(
            String miningChainId,
            String pipelineId,
            String writerId,
            String table,
            ChainPosition position,
            WorkloadClaimFence fence) {
        throw new UnsupportedOperationException("this SRS meta store does not support fenced sink acknowledgements");
    }

    /**
     * Replaces one pipeline's table selection on the chain with {@code tables}, made in ring generation
     * {@code epoch}: the whole set of tables this pipeline reads from the chain's shared ring, across every
     * source of the pipeline that reads it, not one source's share of them. It creates the consumer entry
     * when the pipeline has none yet. A pipeline that reads the chain only through a direct tail of its own
     * selects nothing from the ring, and records an empty selection: the chain's reader owes it nothing.
     *
     * <p>Replaced whole rather than added to, because what a chain may release is decided by who still reads
     * which table: a table a pipeline stopped reading and was still recorded as reading would hold the chain
     * back for an acknowledgement that is never coming. A table acknowledgement survives only for a table that
     * stays selected in the same generation; any other one belongs to a reader that is gone, and its sequence
     * says nothing about the ring this generation writes. The read cursor and the ring completion are left as
     * they are -- except for an empty selection: a pipeline that selects nothing has no place in any ring, and
     * a cursor nothing advances any more would hold the chain's reader back, so both go with it. A mutate on an
     * unseeded chain is a caller ordering error.
     */
    default void selectConsumerTables(String miningChainId, String pipelineId, List<String> tables, long epoch) {
        throw new UnsupportedOperationException("this store does not record consumer table selections");
    }

    /**
     * Records what {@code table}'s sink confirmed, in the order it was read under, without moving the
     * pipeline's chain-level acknowledgement. Only ever raised: a position that does not rank after the one
     * recorded for the table is ignored.
     *
     * <p>On a chain carrying several tables the chain-level acknowledgement is released by whoever reads the
     * chain, and only once every change up to a point of the source log has landed for every table that
     * reads it. This is the input to that release, and it is recorded whatever generation it came from: a
     * reader that took the chain over writes under a generation of its own while the pipelines already on
     * the chain carry on reading, and their confirmations are what lets it release anything. One from a
     * replaced generation can never stand for a change of the current one, because the release compares
     * generations first.
     *
     * <p>The table's {@code ringDoneThrough} is raised to the same sequence in the same write only when the
     * sequence is one of the ring this pipeline is positioned in: the selection records the table, in the
     * generation the position carries, or no selection has been recorded at all. A sequence from another
     * generation, or from a direct tail that numbers its own changes, says nothing about where in that ring
     * a run of this pipeline should carry on. A mutate on an unseeded chain is a caller ordering error.
     */
    default void advanceTableSinkAcked(
            String miningChainId, String pipelineId, String table, ChainPosition position) {
        throw new UnsupportedOperationException("this store does not record table acknowledgements");
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
     * <p>The two are one call because they are only ever read together. With a generation above zero, the
     * seam position is the sole record that a snapshot began at all, so a snapshot resuming after a restart
     * looks here to learn both where the tail picks up and which generation to pin its rows to. A store that
     * could write the position without its generation would leave a resumed snapshot with nothing to pin to,
     * and a rerun that then took the current generation would overwrite changes the earlier one had already
     * applied.
     *
     * <p>Generation zero says no snapshot began: the position is where a tail reading the source directly for
     * this pipeline began, or where a write-back put it, so that its own restart picks up there until a change
     * of its has landed.
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
     * Takes a generation for one stream of a tail that reads the chain's source directly, for one pipeline,
     * and returns it -- above every generation opened on the chain before, shared or direct, and never made
     * the chain's own. The chain's own generation is the one its shared reader writes, and is fenced, under;
     * a direct tail numbers only its own changes, and needs a generation no earlier stream of it numbered
     * under, so that a confirmation of an earlier stream can never stand for one of the new.
     *
     * <p>The default opens the chain's next generation, which is what a store that keeps one counter can do.
     */
    default long openDirectEpoch(String miningChainId) {
        return openEpoch(miningChainId);
    }

    /**
     * The highest generation ever opened on the chain, by its shared reader or by a stream reading its source
     * directly; zero for a chain with no record. A direct stream's generation is never the chain's own
     * ({@link SrsMeta#epoch}), so this is the number anything that has to rank above every change a stream of
     * the chain stamped -- a later run's rows -- must start past. The default answers the chain's own
     * generation, which is the highest a store whose direct streams open the chain's generation has.
     */
    default long highestGenerationOpened(String miningChainId) {
        return read(miningChainId).map(SrsMeta::epoch).orElse(0L);
    }

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

    /** The store-fenced form of a snapshot-completion mark. */
    default void markSnapshotComplete(
            String miningChainId, String pipelineId, String table, WorkloadClaimFence fence) {
        throw new UnsupportedOperationException("this SRS meta store does not support fenced sink acknowledgements");
    }

    /**
     * Marks one writer's copy of a table snapshot complete. Writer-aware stores expose the table as
     * complete only after every configured writer has marked it; the default is the single-writer case.
     */
    default void markSinkWriterSnapshotComplete(
            String miningChainId, String pipelineId, String writerId, String table) {
        markSnapshotComplete(miningChainId, pipelineId, table);
    }

    /** The store-fenced form of one writer's snapshot-completion mark. */
    default void markSinkWriterSnapshotComplete(
            String miningChainId,
            String pipelineId,
            String writerId,
            String table,
            WorkloadClaimFence fence) {
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
