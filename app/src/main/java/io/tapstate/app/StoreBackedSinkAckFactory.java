package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.AwaitedLoad;
import io.tapstate.core.lifecycle.LoadLandings;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.engine.SinkAck;
import io.tapstate.runtime.engine.SinkAckFactory;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.runtime.srs.SrsDurableFrontier;
import io.tapstate.runtime.srs.SrsWriterFrontier;
import io.tapstate.runtime.srs.SrsWriterFrontier.Landed;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.ConsumerProgressKind;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WriterProgress;
import io.tapstate.spi.store.WriterRun;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The production sink-ack factory carried onto the DAG: it confirms each source node's own per-table progress as
 * the pipeline's sinks confirm writes. It holds only serializable coordinates - per table, the mining chain it is
 * read from, the consumer that names the pipeline's source node on that chain and what that consumer's progress
 * is measured against; the pipeline id; and the name of the run its writers report under - and resolves the
 * durable store on the member that runs the sink, mirroring how the source's read-cursor publisher binds its
 * store member-side. The store itself is not serializable and never crosses the wire.
 *
 * <p><b>Every sink processor is a writer, and the pipeline has landed a change only once every writer the
 * change was routed to has.</b> Each writer records its own progress per table under the run, and what the
 * consumer's record says is worked out from all of them: the lowest progress among the writers the run expects
 * for the table, and the highest position a read can resume from at or below it. A pipeline with a view and a
 * serve, or with several serve elements, has several writers on every table they share, and a record any one of
 * them could move by itself would say the pipeline had landed what only the fastest had - a resume from there
 * skips every change the slower ones still held, and nothing ever writes them again. So the ack resolved here
 * lands nothing of its own; only one bound to a writer does.
 *
 * <p>What a table's landing moves depends on what the consumer's progress is measured against. Each table's
 * own confirmation always moves: it is what a run replacing this one carries on from in that table. The
 * consumer's one acked position is a single place in a source's order, and tables counted on their own say
 * nothing about each other: for a consumer reading several tables of a shared capture it moves only to the load's
 * seam, below every change; for a direct channel, whose tables share one order, the store moves it as the
 * channel's recorded batches complete. And a shared capture's source is told how far it may release by the
 * capture itself, once a batch is in the recoverable log, never by a sink - only a direct channel, or a consumer
 * that reads one table on its own chain, carries its sinks' progress back to its source.
 *
 * <p>Where the run is fenced, each of those writes also carries the exact claim the member admitted it under,
 * and the store proves that claim in the same operation as the write: a report that left the member before a
 * takeover and arrives after it lands nothing. The run's start binds the consumer's record to the run the same
 * way.
 *
 * <p>The sink knows a chain only by the {@code src} stream name its events carry — a table at L1 — so this
 * maps that stream to the source node that read it. A member with no store bound resolves to a no-op ack, so a
 * sink still runs before the assembly layer makes the member SRS-capable. A stream the map does not carry is a
 * builder-side wiring defect (the sink saw a chain the pipeline never sourced) and crashes bare.
 *
 * <p>A snapshot row is ordered but carries no token, and where a read resumes from for it is the source node's
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

    private final Map<String, SourceProgress> progressByTable;
    private final String pipelineId;
    private final String runId;

    StoreBackedSinkAckFactory(Map<String, SourceProgress> progressByTable, String pipelineId, String runId) {
        this.progressByTable = Map.copyOf(progressByTable);
        this.pipelineId = Objects.requireNonNull(pipelineId, "pipelineId");
        this.runId = Objects.requireNonNull(runId, "runId");
    }

    /**
     * Starts the run's accounting for every source node the pipeline's sinks receive changes of: per table, the
     * writers the run routes it to. Starting it replaces whatever run was there, so a writer of a replaced run
     * lands nothing from then on.
     */
    @Override
    public void beginRun(HazelcastInstance coordinator, Map<String, List<String>> writersByChain) {
        beginRun(coordinator, writersByChain, null);
    }

    /**
     * As above, and where the run is fenced, binding every later durable effect of it to {@code fence} at the
     * store in the same operation that proves the claim is still live. A claim that has moved on by then is a
     * run that may not start: its writers would report into accounting it could never hold.
     */
    @Override
    public void beginRun(HazelcastInstance coordinator, Map<String, List<String>> writersByChain,
            WorkloadClaimFence fence) {
        SrsMetaStore meta = storeOn(coordinator);
        if (meta == null) {
            return;
        }
        Map<SourceProgress, Map<String, List<String>>> bySource = new LinkedHashMap<>();
        writersByChain.forEach((table, writers) -> bySource
                .computeIfAbsent(sourceOf(progressByTable, table), source -> new LinkedHashMap<>())
                .put(table, writers));
        bySource.forEach((source, byTable) -> {
            if (fence == null) {
                meta.beginWriterRun(source.miningChainId(), source.consumerId(), runId, byTable, source.kind());
            } else if (!meta.beginWriterRun(
                    source.miningChainId(), source.consumerId(), runId, byTable, source.kind(), fence)) {
                throw new TapstateException(
                        EngineError.EXECUTION_NOT_AUTHORIZED, Map.of("pipeline", pipelineId), null);
            }
        });
    }

    @Override
    public SinkAck resolve(HazelcastInstance member) {
        SrsMetaStore meta = storeOn(member);
        if (meta == null) {
            return (chain, position) -> { };
        }
        return new Unbound(meta, progressByTable, pipelineId, runId);
    }

    /**
     * Where the loads a source's changes wait for stand, as the record of the source node behind each table
     * says: a load has landed once every writer named with it has recorded progress, in this run, past the
     * reserved position every row of the load sits at.
     *
     * <p>A load the consumer recorded as landed has landed, whichever run landed it, and a table the pipeline
     * reads no load of on its chain has none in flight. A run that is not this one - not started yet, or
     * already replaced by a later one - says nothing about this run's writers, so nothing of it counts: a
     * source of a replaced run keeps holding until it is stopped. A member with no store bound has no record to
     * read, and holds nothing back, as its acks record nothing.
     */
    @Override
    public LoadLandings loadLandings(HazelcastInstance member) {
        SrsMetaStore meta = storeOn(member);
        if (meta == null) {
            return awaited -> List.of();
        }
        return awaited -> {
            // One read of each source node's consumers and run per look, however many of its tables are awaited
            // - and the consumers on their own, for the reason the ack path asks for them that way.
            Map<String, List<ConsumerOffset>> consumers = new HashMap<>();
            Map<SourceProgress, Optional<WriterRun>> runs = new HashMap<>();
            List<AwaitedLoad> landing = new ArrayList<>();
            for (AwaitedLoad load : awaited) {
                SourceProgress source = sourceOf(progressByTable, load.table());
                List<ConsumerOffset> offsets =
                        consumers.computeIfAbsent(source.miningChainId(), meta::consumerOffsets);
                Optional<WriterRun> run = runs.computeIfAbsent(source,
                        node -> meta.writerRun(node.miningChainId(), node.consumerId()));
                if (!landed(load, source, offsets, run)) {
                    landing.add(load);
                }
            }
            return landing;
        };
    }

    private boolean landed(AwaitedLoad load, SourceProgress source, List<ConsumerOffset> consumers,
            Optional<WriterRun> run) {
        boolean recorded = consumers.stream().anyMatch(consumer -> consumer.pipelineId().equals(source.consumerId())
                && consumer.snapshotCompletedTables().contains(load.table()));
        if (recorded) {
            return true;
        }
        if (run.isEmpty() || !run.get().runId().equals(runId)) {
            return false;
        }
        Long snapshotEpoch = run.get().snapshotEpoch();
        if (snapshotEpoch == null) {
            return true;
        }
        return SrsWriterFrontier.landed(run.get(), load.table(), load.writers())
                .map(landed -> SrsWriterFrontier.passedLoad(landed, snapshotEpoch))
                .orElse(false);
    }

    private static SrsMetaStore storeOn(HazelcastInstance member) {
        return member.getUserContext().get(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY) instanceof SrsMetaStore meta
                ? meta : null;
    }

    /** The source node {@code table} is read by, or a bare crash for a table the pipeline never sourced. */
    private static SourceProgress sourceOf(Map<String, SourceProgress> progressByTable, String table) {
        SourceProgress source = progressByTable.get(table);
        if (source == null) {
            throw new IllegalStateException("sink acked a chain the pipeline never sourced: '" + table + "'");
        }
        return source;
    }

    /** Every table read under the pipeline's own name on its chain, its progress measured as it always was. */
    static Map<String, SourceProgress> legacyProgress(Map<String, String> chainIdByTable, String pipelineId) {
        Map<String, SourceProgress> progress = new LinkedHashMap<>();
        chainIdByTable.forEach((table, chainId) ->
                progress.put(table, new SourceProgress(chainId, pipelineId, ConsumerProgressKind.LEGACY)));
        return Map.copyOf(progress);
    }

    /**
     * Where one source node's progress is kept: the mining chain it reads, the consumer that names it there, and
     * what its progress is measured against.
     */
    record SourceProgress(String miningChainId, String consumerId, ConsumerProgressKind kind)
            implements Serializable {
        SourceProgress {
            Objects.requireNonNull(miningChainId, "miningChainId");
            Objects.requireNonNull(consumerId, "consumerId");
            Objects.requireNonNull(kind, "kind");
        }
    }

    /**
     * The member's ack before it knows which writer is speaking. It lands nothing: progress with no writer
     * behind it has no place among the writers the run waits for, and a record it moved would stand for all
     * of them.
     */
    private static final class Unbound implements SinkAck {

        private static final long serialVersionUID = 1L;

        private final transient SrsMetaStore meta;
        private final Map<String, SourceProgress> progressByTable;
        private final String pipelineId;
        private final String runId;

        Unbound(SrsMetaStore meta, Map<String, SourceProgress> progressByTable, String pipelineId, String runId) {
            this.meta = meta;
            this.progressByTable = progressByTable;
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
            return new WriterAck(meta, progressByTable, pipelineId, runId, writerId);
        }
    }

    /**
     * One writer's ack: what it has landed of each table, recorded under the run as its own, and the source
     * node's record moved only as far as the slowest writer the run expects for that table has got.
     *
     * <p>One per processor, and a processor is only ever driven by one thread at a time, so nothing here is
     * shared.
     */
    private static final class WriterAck implements SinkAck {

        private static final long serialVersionUID = 1L;

        private final transient SrsMetaStore meta;
        private final Map<String, SourceProgress> progressByTable;
        private final String pipelineId;
        private final String runId;
        private final String writerId;
        // Per table: how far this writer has landed it.
        private final transient Map<String, WriterProgress> progress = new HashMap<>();
        // Per table: what this writer last wrote into the source node's record, so an answer that has not moved
        // since is not written again.
        private final transient Map<String, Landed> landed = new HashMap<>();
        // The tables whose recorded progress has not all landed: the store turned a fenced write of the source
        // node's record away, so the next report lands it again, at the same position as much as at a later one.
        private final transient Set<String> owed = new HashSet<>();
        // Per mining chain: the source position last recorded as read, for the same reason.
        private final Map<String, ChainPosition> recordedRead = new HashMap<>();
        // Per source node: the acked position last raised, for the same reason.
        private final Map<SourceProgress, ChainPosition> raised = new HashMap<>();
        // Per source node: where its changes begin, read once rather than on every snapshot row.
        private final Map<SourceProgress, String> cdcStarts = new HashMap<>();
        // The tables already recorded as loaded. The mark is idempotent; this only saves the round trip.
        private final Set<String> loaded = new HashSet<>();
        // Whether a later run has taken over the accounting, after which nothing this writer says lands.
        private boolean replaced;

        WriterAck(SrsMetaStore meta, Map<String, SourceProgress> progressByTable, String pipelineId,
                String runId, String writerId) {
            this.meta = meta;
            this.progressByTable = progressByTable;
            this.pipelineId = pipelineId;
            this.runId = runId;
            this.writerId = Objects.requireNonNull(writerId, "writerId");
        }

        @Override
        public void advance(String chain, ChainPosition position) {
            advance(chain, position, null);
        }

        @Override
        public void advance(String chain, ChainPosition position, WorkloadClaimFence fence) {
            SourceProgress source = sourceOf(progressByTable, chain);
            WriterProgress was = progress.get(chain);
            SourceOrder durable = was == null || position.order().compareTo(was.durableThrough()) > 0
                    ? position.order() : was.durableThrough();
            ChainPosition tokened = was == null ? null : was.lastTokened();
            ChainPosition resumable = resumableAt(position, source);
            if (resumable != null && (tokened == null || resumable.order().compareTo(tokened.order()) > 0)) {
                tokened = resumable;
            }
            WriterProgress next = new WriterProgress(durable, tokened);
            if (!next.equals(was) || owed.contains(chain)) {
                report(chain, source, next, fence);
            }
        }

        @Override
        public void bounded(String chain, SourceOrder through) {
            bounded(chain, through, null);
        }

        @Override
        public void bounded(String chain, SourceOrder through, WorkloadClaimFence fence) {
            WriterProgress was = progress.get(chain);
            if (was != null && through.compareTo(was.durableThrough()) <= 0) {
                if (owed.contains(chain)) {
                    report(chain, sourceOf(progressByTable, chain), was, fence);
                }
                return;
            }
            report(chain, sourceOf(progressByTable, chain),
                    new WriterProgress(through, was == null ? null : was.lastTokened()), fence);
        }

        /**
         * Where a read can resume from once {@code position} is landed: the position itself where it carries
         * a token, the source node's cdc start for a snapshot row, and nowhere for a change the source stated no
         * position at.
         */
        private ChainPosition resumableAt(ChainPosition position, SourceProgress source) {
            if (position.token() != null) {
                return position;
            }
            if (!isSnapshotOf(position)) {
                return null;
            }
            String cdcStart = cdcStarts.computeIfAbsent(source,
                    node -> StoreBackedSinkAckFactory.cdcStart(meta, node.miningChainId(), node.consumerId()));
            return new ChainPosition(position.order(), cdcStart);
        }

        /**
         * Records {@code next} as this writer's progress and lands what it moves - or, where the store turned it
         * away while this run still holds the accounting, leaves it owed, so the next report carries it again.
         */
        private void report(String chain, SourceProgress source, WriterProgress next, WorkloadClaimFence fence) {
            if (replaced) {
                progress.put(chain, next);
                return;
            }
            Optional<WriterRun> run = fence == null
                    ? meta.advanceWriter(source.miningChainId(), source.consumerId(), runId, writerId, chain, next)
                    : meta.advanceWriter(
                            source.miningChainId(), source.consumerId(), runId, writerId, chain, next, fence);
            if (run.isEmpty()) {
                refused(source);
                if (replaced) {
                    progress.put(chain, next);
                }
                return;
            }
            progress.put(chain, next);
            if (land(chain, source, run.get(), fence)) {
                owed.remove(chain);
            } else {
                owed.add(chain);
            }
        }

        /**
         * Works out why the store turned this writer's progress away. A later run having taken the accounting
         * over is the ordinary case: this writer belongs to a run that is being stopped, and nothing it says
         * may land any more. No run at all is not: an execution starts its run before any of its writers
         * exists, so a writer reporting into none was wired to a run nothing started, and nothing would ever
         * wait on what it landed.
         *
         * <p>The accounting still being this run's is a third answer, which only a fenced report meets: the
         * claim it carried was not the live one when it arrived - a lease re-taken under a new topology, or a
         * report that left before a takeover and arrived after it. Nothing is written then and nothing is
         * decided; the progress stays owed, and the member's own guard answers the next report.
         */
        private void refused(SourceProgress source) {
            Optional<WriterRun> current = meta.writerRun(source.miningChainId(), source.consumerId());
            if (current.isEmpty()) {
                throw new IllegalStateException("writer '" + writerId + "' of pipeline '" + pipelineId
                        + "' reported into run '" + runId + "', which nothing started on mining chain '"
                        + source.miningChainId() + "'");
            }
            replaced = !current.get().runId().equals(runId);
        }

        /**
         * Moves the source node's record of {@code chain} as far as every writer the run expects has landed it.
         *
         * <p>The table's own confirmation moves with every landing: the order every writer has passed, and the
         * token of the change there where it carried one - with the table's place in its ring beneath it. A
         * position a read can resume from moves the consumer's acked position as far as what its progress is
         * measured against lets it ({@link #ackedCandidate}), and a direct channel's recorded batches are settled
         * against the tables now confirmed. And the table is recorded as loaded once every writer has passed the
         * one position every row of the load sits at.
         *
         * <p>A writer reporting a table the run does not route to it crashes bare: its progress would be left
         * out of the lowest one, so a resume could pass changes it still holds.
         *
         * @return false where the store turned a fenced write away, leaving the rest of the landing owed
         */
        private boolean land(String chain, SourceProgress source, WriterRun run, WorkloadClaimFence fence) {
            if (!run.expectedFor(chain).contains(writerId)) {
                throw new IllegalStateException("writer '" + writerId + "' landed changes of '" + chain
                        + "' that run '" + runId + "' of pipeline '" + pipelineId + "' does not route to it");
            }
            Optional<Landed> answer = SrsWriterFrontier.landed(run, chain);
            if (answer.isEmpty()) {
                return true;
            }
            Landed now = answer.get();
            Landed before = landed.get(chain);
            // Every writer reports on its own, so this may land after a later report of the table's - which the
            // store, comparing the two, leaves standing.
            if (before == null || now.durableThrough().compareTo(before.durableThrough()) > 0) {
                if (!advanceTableConfirmed(source, chain, confirmedAt(now), fence)) {
                    return false;
                }
                if (source.kind() == ConsumerProgressKind.DIRECT_SOURCE) {
                    meta.settleDirectBatches(source.miningChainId(), source.consumerId());
                }
            }
            ChainPosition resumable = now.resumableAt();
            if (resumable != null && (before == null || before.resumableAt() == null
                    || resumable.order().compareTo(before.resumableAt().order()) > 0)) {
                ChainPosition acked = ackedCandidate(source, run, resumable);
                if (acked != null && !acked.equals(raised.get(source))) {
                    if (!raiseSinkAcked(source, acked, fence)) {
                        return false;
                    }
                    raised.put(source, acked);
                }
                if (!isSnapshotOf(resumable) && carriesSourceReadBack(source, run)) {
                    recordHowFarTheSourceHasBeenRead(meta, source.miningChainId(), resumable, recordedRead);
                }
            }
            if (!loaded.contains(chain) && SrsWriterFrontier.passedLoad(now, run.snapshotEpoch())) {
                if (!markSnapshotComplete(source, chain, fence)) {
                    return false;
                }
                loaded.add(chain);
            }
            landed.put(chain, now);
            return true;
        }

        /**
         * Where the consumer's one acked position may move now that a table has landed as far as {@code resumable}:
         * the position itself where the run reads this one table of the source node; where it reads several, only
         * the load's seam, once every table has landed at least that far - a change of one table says nothing about
         * how far another has got; and nowhere for a direct channel, whose position the store moves as its
         * recorded batches complete.
         */
        private ChainPosition ackedCandidate(SourceProgress source, WriterRun run, ChainPosition resumable) {
            if (source.kind() == ConsumerProgressKind.DIRECT_SOURCE) {
                return null;
            }
            if (run.expected().size() == 1) {
                return resumable;
            }
            ChainPosition lowest = null;
            for (String table : run.expected().keySet()) {
                ChainPosition at = SrsWriterFrontier.landed(run, table).map(Landed::resumableAt).orElse(null);
                if (at == null) {
                    return null;
                }
                if (lowest == null || at.order().compareTo(lowest.order()) < 0) {
                    lowest = at;
                }
            }
            return lowest != null && isSnapshotOf(lowest) ? lowest : null;
        }

        // Each write of the source node's record, fenced where the report was. A fenced write the store turned
        // away answers false, and what it would have recorded is left for the next report, which lands it again.

        private boolean advanceTableConfirmed(SourceProgress source, String chain, ChainPosition confirmed,
                WorkloadClaimFence fence) {
            if (fence == null) {
                meta.advanceTableConfirmed(source.miningChainId(), source.consumerId(), chain, confirmed);
                return true;
            }
            return meta.advanceTableConfirmed(source.miningChainId(), source.consumerId(), chain, confirmed, fence);
        }

        private boolean raiseSinkAcked(SourceProgress source, ChainPosition acked, WorkloadClaimFence fence) {
            if (fence == null) {
                meta.raiseSinkAcked(source.miningChainId(), source.consumerId(), acked);
                return true;
            }
            return meta.raiseSinkAcked(source.miningChainId(), source.consumerId(), acked, fence);
        }

        private boolean markSnapshotComplete(SourceProgress source, String chain, WorkloadClaimFence fence) {
            if (fence == null) {
                meta.markSnapshotComplete(source.miningChainId(), source.consumerId(), chain);
                return true;
            }
            return meta.markSnapshotComplete(source.miningChainId(), source.consumerId(), chain, fence);
        }
    }

    /**
     * The table's confirmation once {@code landed}: the order every writer has passed, with the token of the
     * position there where a read can resume from exactly that order, and none where the change there stated no
     * position - an order a token from beneath it would be read back as reaching.
     */
    private static ChainPosition confirmedAt(Landed landed) {
        ChainPosition resumable = landed.resumableAt();
        String token = resumable != null && resumable.order().equals(landed.durableThrough())
                ? resumable.token() : null;
        return new ChainPosition(landed.durableThrough(), token);
    }

    /**
     * Whether the source node's sinks carry their progress back to its source: a direct channel's, whose source
     * nothing else tells, and one that reads a single table of its chain, whose one order is that table's. A shared
     * capture's source is told by the capture, once a batch is in the recoverable log, and tables counted on their
     * own cannot rank each other into one place in the source.
     */
    private static boolean carriesSourceReadBack(SourceProgress source, WriterRun run) {
        return source.kind() == ConsumerProgressKind.DIRECT_SOURCE
                || source.kind() == ConsumerProgressKind.LEGACY && run.expected().size() == 1;
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
     * target, which is why the mark is recorded against the consumer and not the chain -- every pipeline on a
     * chain writes somewhere of its own. The read side knows when it finished reading, which is a different
     * question again: a table read and never written looks finished to it, and a run that trusted that would
     * skip the table on its way back. The tail only replays what changed after the snapshot began, so a row
     * that never changed again would be absent from the target for good -- nothing thrown, nothing logged.
     */
    private static boolean isSnapshotOf(ChainPosition position) {
        return position.order() != null && position.order().seq() == SourceOrder.SNAPSHOT_SEQ;
    }

    /**
     * Where changes begin for {@code consumerId} on {@code miningChainId}, for a frontier that has only reached
     * snapshot rows. The capture writes it before it drains that source node's snapshot, so a snapshot row
     * reaching a sink without one means the consumer was never seeded — the same caller-ordering defect as
     * acking an unseeded chain, and it crashes bare rather than borrowing another consumer's position.
     */
    private static String cdcStart(SrsMetaStore meta, String miningChainId, String consumerId) {
        return meta.read(miningChainId)
                .flatMap(record -> record.consumerOffset(consumerId))
                .map(offset -> offset.cdcStartPosition())
                .orElseThrow(() -> new IllegalStateException("sink acked snapshot rows of mining chain '"
                        + miningChainId + "' for consumer '" + consumerId
                        + "', which has no recorded position for changes to begin at"));
    }
}
