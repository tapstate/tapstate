package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.SinkAck;
import io.tapstate.runtime.engine.SinkAckFactory;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.runtime.srs.SrsDurableFrontier;
import io.tapstate.runtime.srs.SrsWriterFrontier;
import io.tapstate.runtime.srs.SrsWriterFrontier.Landed;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.WriterProgress;
import io.tapstate.spi.store.WriterRun;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The production sink-ack factory carried onto the DAG: it advances one consumer pipeline's durable
 * sink-acked source position as the pipeline's sinks confirm writes, so the source-read durable frontier has
 * a real input. It holds only serializable coordinates — a {@code table -> mining chain id} map for every
 * source the pipeline reads, the consumer pipeline id, and the name of the run its writers report under — and
 * resolves the durable store on the member that runs the sink, mirroring how the source's read-cursor
 * publisher binds its store member-side. The store itself is not serializable and never crosses the wire.
 *
 * <p><b>Every sink processor is a writer, and the pipeline has landed a change only once every writer the
 * change was routed to has.</b> Each writer records its own progress per table under the run, and what the
 * pipeline's record says is worked out from all of them: the lowest progress among the writers the run
 * expects for the table, and the highest position a read can resume from at or below it. A pipeline with a
 * view and a serve, or with several serve elements, has several writers on every table they share, and a
 * record any one of them could move by itself would say the pipeline had landed what only the fastest had -
 * a resume from there skips every change the slower ones still held, and nothing ever writes them again. So
 * the ack resolved here lands nothing of its own; only one bound to a writer does.
 *
 * <p>The sink knows a chain only by the {@code src} stream name its events carry — a table at L1 — so this
 * maps that stream to the mining chain that keys its durable record. A member with no store bound resolves to
 * a no-op ack, so a sink still runs before the assembly layer makes the member SRS-capable. A stream the map
 * does not carry is a builder-side wiring defect (the sink saw a chain the pipeline never sourced) and crashes
 * bare.
 *
 * <p>A snapshot row is ordered but carries no token, and where a read resumes from for it is the pipeline's
 * cdc start position: the read has confirmed rows of a snapshot but no change at all, so a resume belongs
 * where changes begin. Resolving it here rather than at the sink is what keeps the durable store out of the
 * engine — the sink says which position it reached, this says what that spells on disk.
 *
 * <p>A change can carry no token too, and it is not the same case. A source names a position for a run of
 * changes when it has one and names none when it has not, and that absence is load-bearing: the recipient
 * carries on rather than inventing a position, because an invented one claims changes were read that were
 * not, and a later run would resume past them. So a change with no position of its own moves how far the
 * writer has landed and nothing a read resumes from. Reading the two cases as one reached for a cdc start
 * that only a snapshot phase ever writes, so a cdc_only pipeline, which runs no snapshot, died on its first
 * acknowledged change with the target still empty.
 */
final class StoreBackedSinkAckFactory implements SinkAckFactory {

    private static final long serialVersionUID = 1L;

    private final Map<String, String> chainIdByTable;
    private final String pipelineId;
    private final String runId;

    StoreBackedSinkAckFactory(Map<String, String> chainIdByTable, String pipelineId, String runId) {
        this.chainIdByTable = Map.copyOf(chainIdByTable);
        this.pipelineId = Objects.requireNonNull(pipelineId, "pipelineId");
        this.runId = Objects.requireNonNull(runId, "runId");
    }

    /**
     * Starts the run's accounting on every mining chain the pipeline's sinks receive changes of: per table,
     * the writers the run routes it to. Starting it replaces whatever run was there, so a writer of a replaced
     * run lands nothing from then on.
     */
    @Override
    public void beginRun(HazelcastInstance coordinator, Map<String, List<String>> writersByChain) {
        SrsMetaStore meta = storeOn(coordinator);
        if (meta == null) {
            return;
        }
        Map<String, Map<String, List<String>>> byMiningChain = new LinkedHashMap<>();
        writersByChain.forEach((table, writers) -> byMiningChain
                .computeIfAbsent(miningChainOf(chainIdByTable, table), chain -> new LinkedHashMap<>())
                .put(table, writers));
        byMiningChain.forEach((miningChainId, byTable) ->
                meta.beginWriterRun(miningChainId, pipelineId, runId, byTable));
    }

    @Override
    public SinkAck resolve(HazelcastInstance member) {
        SrsMetaStore meta = storeOn(member);
        if (meta == null) {
            return (chain, position) -> { };
        }
        return new Unbound(meta, chainIdByTable, pipelineId, runId);
    }

    private static SrsMetaStore storeOn(HazelcastInstance member) {
        return member.getUserContext().get(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY) instanceof SrsMetaStore meta
                ? meta : null;
    }

    /** The mining chain {@code table} is read from, or a bare crash for a table the pipeline never sourced. */
    private static String miningChainOf(Map<String, String> chainIdByTable, String table) {
        String miningChainId = chainIdByTable.get(table);
        if (miningChainId == null) {
            throw new IllegalStateException("sink acked a chain the pipeline never sourced: '" + table + "'");
        }
        return miningChainId;
    }

    /**
     * The member's ack before it knows which writer is speaking. It lands nothing: progress with no writer
     * behind it has no place among the writers the run waits for, and a record it moved would stand for all
     * of them.
     */
    private static final class Unbound implements SinkAck {

        private static final long serialVersionUID = 1L;

        private final transient SrsMetaStore meta;
        private final Map<String, String> chainIdByTable;
        private final String pipelineId;
        private final String runId;

        Unbound(SrsMetaStore meta, Map<String, String> chainIdByTable, String pipelineId, String runId) {
            this.meta = meta;
            this.chainIdByTable = chainIdByTable;
            this.pipelineId = pipelineId;
            this.runId = runId;
        }

        @Override
        public void advance(String chain, ChainPosition position) {
            throw new IllegalStateException("pipeline '" + pipelineId + "' acked chain '" + chain
                    + "' without naming the writer that landed it; its progress is only ever kept per writer");
        }

        @Override
        public SinkAck forWriter(String writerId) {
            return new WriterAck(meta, chainIdByTable, pipelineId, runId, writerId);
        }
    }

    /**
     * One writer's ack: what it has landed of each table, recorded under the run as its own, and the
     * pipeline's record moved only as far as the slowest writer the run expects for that table has got.
     *
     * <p>One per processor, and a processor is only ever driven by one thread at a time, so nothing here is
     * shared.
     */
    private static final class WriterAck implements SinkAck {

        private static final long serialVersionUID = 1L;

        private final transient SrsMetaStore meta;
        private final Map<String, String> chainIdByTable;
        private final String pipelineId;
        private final String runId;
        private final String writerId;
        // Per table: how far this writer has landed it.
        private final transient Map<String, WriterProgress> progress = new HashMap<>();
        // Per table: what this writer last wrote into the pipeline's record, so an answer that has not moved
        // since is not written again.
        private final transient Map<String, Landed> landed = new HashMap<>();
        // Per mining chain: the source position last recorded as read, for the same reason.
        private final Map<String, ChainPosition> recordedRead = new HashMap<>();
        // Per mining chain: where the pipeline's changes begin, read once rather than on every snapshot row.
        private final Map<String, String> cdcStarts = new HashMap<>();
        // The tables already recorded as loaded. The mark is idempotent; this only saves the round trip.
        private final Set<String> loaded = new HashSet<>();
        // Whether a later run has taken over the accounting, after which nothing this writer says lands.
        private boolean replaced;

        WriterAck(SrsMetaStore meta, Map<String, String> chainIdByTable, String pipelineId, String runId,
                String writerId) {
            this.meta = meta;
            this.chainIdByTable = chainIdByTable;
            this.pipelineId = pipelineId;
            this.runId = runId;
            this.writerId = Objects.requireNonNull(writerId, "writerId");
        }

        @Override
        public void advance(String chain, ChainPosition position) {
            String miningChainId = miningChainOf(chainIdByTable, chain);
            WriterProgress was = progress.get(chain);
            SourceOrder durable = was == null || position.order().compareTo(was.durableThrough()) > 0
                    ? position.order() : was.durableThrough();
            ChainPosition tokened = was == null ? null : was.lastTokened();
            ChainPosition resumable = resumableAt(position, miningChainId);
            if (resumable != null && (tokened == null || resumable.order().compareTo(tokened.order()) > 0)) {
                tokened = resumable;
            }
            WriterProgress next = new WriterProgress(durable, tokened);
            if (!next.equals(was)) {
                report(chain, miningChainId, next);
            }
        }

        @Override
        public void bounded(String chain, SourceOrder through) {
            WriterProgress was = progress.get(chain);
            if (was != null && through.compareTo(was.durableThrough()) <= 0) {
                return;
            }
            report(chain, miningChainOf(chainIdByTable, chain),
                    new WriterProgress(through, was == null ? null : was.lastTokened()));
        }

        /**
         * Where a read can resume from once {@code position} is landed: the position itself where it carries
         * a token, the pipeline's cdc start for a snapshot row, and nowhere for a change the source stated no
         * position at.
         */
        private ChainPosition resumableAt(ChainPosition position, String miningChainId) {
            if (position.token() != null) {
                return position;
            }
            if (!isSnapshotOf(position)) {
                return null;
            }
            String cdcStart = cdcStarts.computeIfAbsent(miningChainId,
                    chain -> StoreBackedSinkAckFactory.cdcStart(meta, chain, pipelineId));
            return new ChainPosition(position.order(), cdcStart);
        }

        private void report(String chain, String miningChainId, WriterProgress next) {
            progress.put(chain, next);
            if (replaced) {
                return;
            }
            Optional<WriterRun> run = meta.advanceWriter(miningChainId, pipelineId, runId, writerId, chain, next);
            if (run.isEmpty()) {
                refused(miningChainId);
                return;
            }
            land(chain, miningChainId, run.get());
        }

        /**
         * Works out why the store turned this writer's progress away. A later run having taken the accounting
         * over is the ordinary case: this writer belongs to a run that is being stopped, and nothing it says
         * may land any more. No run at all is not: an execution starts its run before any of its writers
         * exists, so a writer reporting into none was wired to a run nothing started, and nothing would ever
         * wait on what it landed.
         */
        private void refused(String miningChainId) {
            if (meta.writerRun(miningChainId, pipelineId).isEmpty()) {
                throw new IllegalStateException("writer '" + writerId + "' of pipeline '" + pipelineId
                        + "' reported into run '" + runId + "', which nothing started on mining chain '"
                        + miningChainId + "'");
            }
            replaced = true;
        }

        /**
         * Moves the pipeline's record of {@code chain} as far as every writer the run expects has landed it.
         *
         * <p>A position a read can resume from moves the acked position, with the table's ring beneath it; a
         * snapshot row's resumes where changes begin and is no place in any ring. Landed progress beyond the
         * last such position only raises the ring, because the acked position is a token and the order it sat
         * at, and there is no token to pair that order with. And the table is recorded as loaded once every
         * writer has passed the one position every row of the load sits at.
         *
         * <p>A writer reporting a table the run does not route to it crashes bare: its progress would be left
         * out of the lowest one, so a resume could pass changes it still holds.
         */
        private void land(String chain, String miningChainId, WriterRun run) {
            if (!run.expectedFor(chain).contains(writerId)) {
                throw new IllegalStateException("writer '" + writerId + "' landed changes of '" + chain
                        + "' that run '" + runId + "' of pipeline '" + pipelineId + "' does not route to it");
            }
            Optional<Landed> answer = SrsWriterFrontier.landed(run, chain);
            if (answer.isEmpty()) {
                return;
            }
            Landed now = answer.get();
            Landed before = landed.get(chain);
            ChainPosition resumable = now.resumableAt();
            if (resumable != null && (before == null || before.resumableAt() == null
                    || resumable.order().compareTo(before.resumableAt().order()) > 0)) {
                if (isSnapshotOf(resumable)) {
                    meta.advanceSinkAcked(miningChainId, pipelineId, resumable);
                } else {
                    // Recorded against the table's own ring too, at the sequence the change sat at there, so a
                    // run replacing this one carries on from it instead of from the head of the ring.
                    meta.advanceSinkAcked(miningChainId, pipelineId, chain, resumable);
                    recordHowFarTheSourceHasBeenRead(meta, miningChainId, resumable, recordedRead);
                }
            }
            SourceOrder durable = now.durableThrough();
            if (durable.seq() >= 0 && (before == null || durable.compareTo(before.durableThrough()) > 0)
                    && (resumable == null || durable.compareTo(resumable.order()) > 0)) {
                meta.advanceRingDone(miningChainId, pipelineId, chain, durable.seq());
            }
            if (!loaded.contains(chain) && SrsWriterFrontier.passedLoad(now, run.snapshotEpoch())) {
                meta.markSnapshotComplete(miningChainId, pipelineId, chain);
                loaded.add(chain);
            }
            landed.put(chain, now);
        }
    }

    /**
     * Works out again, now that an acknowledgement has landed, how far the chain may say its source has
     * been read.
     *
     * <p>That value is the lowest of what the source read and what every consumer has durably landed, and
     * it used to be resolved only while a run of changes was being forwarded — against the acknowledgements
     * that existed at that instant, which on a first forward is none at all. The acknowledgement arrives
     * here, afterwards, and nothing carried it back: the record kept whatever an earlier forward had
     * resolved, one delivery behind while changes kept arriving and nothing whatsoever once the source went
     * quiet. A cdc-only read has no recorded start for changes to fall back on either, so a run restarted
     * from that state re-attached at the present moment and everything written while it was down was gone,
     * with nothing thrown and nothing logged. Resolving it here is the carry-back: the input that was
     * missing is the one that has just landed.
     *
     * <p>The position just acknowledged is the candidate because it is itself one of those
     * acknowledgements, so the lowest of it and the rest is the lowest of the rest — the same clamp, from
     * the side the late input arrives on. It can never pass what was read: a sink acknowledges only a
     * change that reached it, and a change reaches it only by being read.
     *
     * <p>A snapshot row does not come here. What this value means while a load runs is the reader's own
     * business and nothing states it from this side; the change acknowledgements that follow the load carry
     * it from there.
     *
     * <p>The consumers are asked for on their own rather than read off the whole record, because the record
     * also carries a schema history that grows for the life of the chain and this path would then pay for
     * it on every acknowledged batch. Unchanged from last time means the slowest consumer has landed
     * nothing since, so the write would say what the record already holds — the same round trip for nothing
     * that the forwarding side skips. Two members acking at once can still both write; the store's own
     * guarantee that this value only ever moves forward is what makes that harmless, and it is the same
     * guarantee that lets a reader and a sink write it at all.
     */
    private static void recordHowFarTheSourceHasBeenRead(
            SrsMetaStore meta,
            String miningChainId,
            ChainPosition acked,
            Map<String, ChainPosition> recorded) {
        SrsDurableFrontier.safeAdvance(acked, meta.consumerOffsets(miningChainId)).ifPresent(safe -> {
            if (!safe.equals(recorded.get(miningChainId))) {
                meta.advanceSourceReadOffset(miningChainId, safe);
                recorded.put(miningChainId, safe);
            }
        });
    }

    /**
     * Whether {@code position} is where a table's snapshot sits: the one reserved position every row of a
     * snapshot carries, beneath every change of its generation. A frontier that has reached it has confirmed
     * the whole of that table's snapshot, because there is nothing of the snapshot above it left to wait on.
     *
     * <p>Reaching it is the only moment anyone learns that a table's rows are in <em>this pipeline's</em>
     * target, which is why the mark is recorded against the pipeline and not the chain -- every pipeline on a
     * chain writes somewhere of its own. The read side knows when it finished reading, which is a different
     * question again: a table read and never written looks finished to it, and a run that trusted that would
     * skip the table on its way back. The tail only replays what changed after the snapshot began, so a row
     * that never changed again would be absent from the target for good -- nothing thrown, nothing logged.
     */
    private static boolean isSnapshotOf(ChainPosition position) {
        return position.order() != null && position.order().seq() == SourceOrder.SNAPSHOT_SEQ;
    }

    /**
     * Where changes begin for {@code pipelineId} on {@code miningChainId}, for a frontier that has only
     * reached snapshot rows. The capture writes it before it drains that pipeline's snapshot, so a snapshot
     * row reaching a sink without one means the pipeline was never seeded — the same caller-ordering defect
     * as acking an unseeded chain, and it crashes bare rather than borrowing another pipeline's position.
     */
    private static String cdcStart(SrsMetaStore meta, String miningChainId, String pipelineId) {
        return meta.read(miningChainId)
                .flatMap(record -> record.consumerOffset(pipelineId))
                .map(offset -> offset.cdcStartPosition())
                .orElseThrow(() -> new IllegalStateException("sink acked snapshot rows of mining chain '"
                        + miningChainId + "' for pipeline '" + pipelineId
                        + "', which has no recorded position for changes to begin at"));
    }
}
