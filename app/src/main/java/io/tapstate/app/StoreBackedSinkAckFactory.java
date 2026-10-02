package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.SinkAck;
import io.tapstate.runtime.engine.SinkAckFactory;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.runtime.srs.SrsDurableFrontier;
import io.tapstate.spi.store.ConsumerProgressKind;
import io.tapstate.spi.store.SrsMetaStore;
import io.tapstate.spi.store.WorkloadClaimFence;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The production sink-ack factory carried onto the DAG: it confirms each source node's own per-table
 * recovery progress as the sink confirms writes. It holds only serializable coordinates — a table map
 * naming the physical chain, source-scoped consumer and progress kind, plus the stable writer plan — and
 * resolves the durable store on the member that runs the sink, mirroring how the source's read-cursor
 * publisher binds its store member-side. An assembly-only store handle checks the complete writer plan
 * before the DAG can be submitted; it is transient, so the store itself never crosses the wire.
 *
 * <p>The sink knows a chain only by the {@code src} stream name its events carry — a table at L1 — so this
 * maps that stream to the mining chain that keys its durable record and advances that writer's position.
 * The store exposes the minimum over every writer expected on the stream as that table's position, so a
 * fast target cannot move a replacement run past changes another target has not written. A member with no
 * store bound resolves to a no-op ack, so a sink still runs before the assembly layer makes the member
 * SRS-capable. A stream the map does not carry is a builder-side wiring defect (the sink saw a chain the
 * pipeline never sourced) and crashes bare.
 *
 * <p>Shared SRS consumption never advances the physical source checkpoint here. Capture owns that
 * checkpoint after a recoverable log batch has landed; consumer ACKs own which table records may be
 * replayed or retired. A direct channel has one forwarding order across its tables and can carry its
 * confirmed source position back from here without borrowing another channel's progress.
 *
 * <p>A snapshot row is ordered but carries no token, and what is persisted for it is the source node's cdc
 * start position: the read has confirmed rows of a snapshot but no change at all, so a resume belongs
 * where changes begin. Resolving it here rather than at the sink is what keeps the durable store out of
 * the engine — the sink says which position it reached, this says what that spells on disk.
 *
 * <p>A change can carry no token too, and it is not the same case. A source names a position for a run
 * of changes when it has one and names none when it has not, and that absence is load-bearing: the
 * recipient carries on rather than inventing a position, because an invented one claims changes were
 * read that were not, and a later run would resume past them. So a change with no position of its own
 * is acked by its order alone — which is all a frontier ranks on, and all the durable record needs.
 * Reading the two cases as one reached for a cdc start that only a snapshot phase ever writes, so a
 * cdc_only pipeline, which runs no snapshot, died on its first acknowledged change with the target
 * still empty.
 */
final class StoreBackedSinkAckFactory implements SinkAckFactory {

    private static final long serialVersionUID = 1L;

    private final Map<String, SourceProgress> progressByTable;
    private final String writerId;
    private final Set<String> writerStreams;
    private final Map<String, List<String>> writerIdsByStream;
    private final AtomicInteger resolvedWriterSequence;
    private final Map<String, List<String>> resolvedWriterIdsByStream;
    private final transient SrsMetaStore assemblyStore;

    StoreBackedSinkAckFactory(Map<String, String> chainIdByTable, String pipelineId) {
        this(chainIdByTable, pipelineId, null);
    }

    StoreBackedSinkAckFactory(
            Map<String, String> chainIdByTable, String pipelineId, SrsMetaStore assemblyStore) {
        this(legacyProgress(chainIdByTable, pipelineId), assemblyStore);
    }

    StoreBackedSinkAckFactory(Map<String, SourceProgress> progressByTable, SrsMetaStore assemblyStore) {
        this(progressByTable, null, Set.of(), Map.of(),
                new AtomicInteger(), new ConcurrentHashMap<>(), assemblyStore);
    }

    private StoreBackedSinkAckFactory(
            Map<String, SourceProgress> progressByTable,
            String writerId,
            Set<String> writerStreams,
            Map<String, List<String>> writerIdsByStream,
            AtomicInteger resolvedWriterSequence,
            Map<String, List<String>> resolvedWriterIdsByStream,
            SrsMetaStore assemblyStore) {
        this.progressByTable = Map.copyOf(progressByTable);
        this.writerId = writerId;
        this.writerStreams = Set.copyOf(writerStreams);
        this.writerIdsByStream = copyPlan(writerIdsByStream);
        this.resolvedWriterSequence = resolvedWriterSequence;
        this.resolvedWriterIdsByStream = resolvedWriterIdsByStream;
        this.assemblyStore = assemblyStore;
    }

    /**
     * Checks and records every already-seeded chain before the DAG can run. A chain not seeded yet is
     * left to the member-side lazy registration; it has no retained cursor that could be ambiguous.
     */
    @Override
    public void prepareWriterPlan(Map<String, List<String>> writerIdsByStream) {
        if (assemblyStore == null) {
            return;
        }
        LinkedHashSet<SourceProgress> sourceProgresses = new LinkedHashSet<>();
        for (String stream : writerIdsByStream.keySet()) {
            SourceProgress progress = progressByTable.get(stream);
            if (progress == null) {
                throw new IllegalStateException(
                        "sink writer plan names a chain the pipeline never sourced: '" + stream + "'");
            }
            sourceProgresses.add(progress);
        }
        for (SourceProgress progress : sourceProgresses) {
            if (assemblyStore.read(progress.miningChainId()).isPresent()) {
                configure(assemblyStore, progress, writerIdsByStream);
            }
        }
    }

    @Override
    public SinkAckFactory forWriter(
            String writerId, List<String> streams, Map<String, List<String>> writerIdsByStream) {
        if (writerId == null || writerId.isBlank()) {
            throw new IllegalArgumentException("sink writer id must be non-blank");
        }
        return new StoreBackedSinkAckFactory(
                progressByTable,
                writerId,
                new LinkedHashSet<>(streams),
                writerIdsByStream,
                resolvedWriterSequence,
                resolvedWriterIdsByStream,
                assemblyStore);
    }

    @Override
    public SinkAck resolve(HazelcastInstance member) {
        Object bound = member.getUserContext().get(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY);
        if (!(bound instanceof SrsMetaStore meta)) {
            return (chain, position) -> { };
        }
        WriterBinding writer = writerId == null
                ? bindResolvedWriter()
                : new WriterBinding(writerId, writerStreams, writerIdsByStream);
        ConcurrentMap<SourceProgress, Optional<WorkloadClaimFence>> configuredMiningChains = new ConcurrentHashMap<>();
        Map<SourceProgress, ChainPosition> recorded = new ConcurrentHashMap<>();
        return new SinkAck() {
            @Override
            public void advance(String chain, ChainPosition position) {
                advance(chain, position, null);
            }

            @Override
            public void advance(String chain, ChainPosition position, WorkloadClaimFence fence) {
                acknowledge(meta, writer, configuredMiningChains, recorded, chain, position, fence);
            }
        };
    }

    /** Maps one engine acknowledgement to the durable consumer write under the claim that admitted it. */
    private void acknowledge(
            SrsMetaStore meta,
            WriterBinding writer,
            ConcurrentMap<SourceProgress, Optional<WorkloadClaimFence>> configuredMiningChains,
            Map<SourceProgress, ChainPosition> recorded,
            String chain,
            ChainPosition position,
            WorkloadClaimFence fence) {
        SourceProgress progress = progressByTable.get(chain);
        if (progress == null) {
            throw new IllegalStateException(
                    "sink acked a chain the pipeline never sourced: '" + chain + "'");
        }
        if (!writer.streams().contains(chain)) {
            throw new IllegalStateException("sink writer '" + writer.id()
                    + "' acked a chain it does not receive: '" + chain + "'");
        }
        ensureConfigured(meta, writer, progress, configuredMiningChains, fence);
        String token = position.token() != null ? position.token()
                : isSnapshotOf(position) ? cdcStart(meta, progress.miningChainId(), progress.consumerId()) : null;
        ChainPosition acked = new ChainPosition(position.order(), token);
        if (isSnapshotOf(position)) {
            advanceWriterAck(meta, progress, writer.id(), chain, acked, fence);
            markWriterSnapshotComplete(meta, progress, writer.id(), chain, fence);
        } else {
            // A change is also recorded against its own table's ring, at the sequence it sat at there,
            // so a run replacing this one carries on from it instead of from the head of the ring.
            advanceWriterAck(meta, progress, writer.id(), chain, acked, fence);
            if (progress.kind() == ConsumerProgressKind.DIRECT_SOURCE
                    || (progress.kind() == ConsumerProgressKind.LEGACY
                            && progressByTable.values().stream().filter(progress::equals).count() == 1)) {
                recordHowFarTheSourceHasBeenRead(meta, progress, acked, recorded);
            }
        }
    }

    /**
     * Gives direct users of the factory one identity per resolve. Production binds stable identities in
     * the DAG; this path keeps the factory seam truthful for tests and older callers that resolve it
     * directly.
     */
    private WriterBinding bindResolvedWriter() {
        synchronized (resolvedWriterIdsByStream) {
            String resolvedId = "resolved-" + resolvedWriterSequence.getAndIncrement();
            for (String stream : progressByTable.keySet()) {
                resolvedWriterIdsByStream.compute(stream, (ignored, writers) -> {
                    List<String> next = new ArrayList<>(writers == null ? List.of() : writers);
                    next.add(resolvedId);
                    return List.copyOf(next);
                });
            }
            return new WriterBinding(resolvedId, progressByTable.keySet(), null);
        }
    }

    /** Registers a writer's complete plan for one mining chain whenever its authorized fence changes. */
    private void ensureConfigured(
            SrsMetaStore meta,
            WriterBinding writer,
            SourceProgress progress,
            ConcurrentMap<SourceProgress, Optional<WorkloadClaimFence>> configuredMiningChains,
            WorkloadClaimFence fence) {
        Optional<WorkloadClaimFence> expected = Optional.ofNullable(fence);
        configuredMiningChains.compute(progress, (ignored, configured) -> {
            if (expected.equals(configured)) {
                return configured;
            }
            Map<String, List<String>> plan = writer.plan();
            if (plan == null) {
                synchronized (resolvedWriterIdsByStream) {
                    plan = copyPlan(resolvedWriterIdsByStream);
                }
            }
            configure(meta, progress, plan, fence);
            return expected;
        });
    }

    /** Registers only the tables belonging to this source node, even when another shares its capture. */
    private void configure(
            SrsMetaStore meta, SourceProgress progress, Map<String, List<String>> plan) {
        configure(meta, progress, plan, null);
    }

    private void configure(
            SrsMetaStore meta,
            SourceProgress progress,
            Map<String, List<String>> plan,
            WorkloadClaimFence fence) {
        Map<String, List<String>> writersByTable = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> entry : plan.entrySet()) {
            SourceProgress entryProgress = progressByTable.get(entry.getKey());
            if (entryProgress == null) {
                throw new IllegalStateException(
                        "sink writer plan names a chain the pipeline never sourced: '" + entry.getKey() + "'");
            }
            if (entryProgress.equals(progress)) {
                writersByTable.put(entry.getKey(), entry.getValue());
            }
        }
        if (writersByTable.isEmpty()) {
            throw new IllegalStateException(
                    "sink writer plan names no stream for source consumer '" + progress.consumerId() + "'");
        }
        if (fence == null) {
            meta.configureSinkWriters(progress.miningChainId(), progress.consumerId(), writersByTable, progress.kind());
        } else {
            meta.configureSinkWriters(
                    progress.miningChainId(), progress.consumerId(), writersByTable, progress.kind(), fence);
        }
    }

    private void advanceWriterAck(
            SrsMetaStore meta,
            SourceProgress progress,
            String writerId,
            String table,
            ChainPosition acked,
            WorkloadClaimFence fence) {
        if (fence == null) {
            meta.advanceSinkWriterAcked(progress.miningChainId(), progress.consumerId(), writerId, table, acked);
        } else {
            meta.advanceSinkWriterAcked(progress.miningChainId(), progress.consumerId(), writerId, table, acked, fence);
        }
    }

    private void markWriterSnapshotComplete(
            SrsMetaStore meta,
            SourceProgress progress,
            String writerId,
            String table,
            WorkloadClaimFence fence) {
        if (fence == null) {
            meta.markSinkWriterSnapshotComplete(progress.miningChainId(), progress.consumerId(), writerId, table);
        } else {
            meta.markSinkWriterSnapshotComplete(progress.miningChainId(), progress.consumerId(), writerId, table, fence);
        }
    }

    private static Map<String, List<String>> copyPlan(Map<String, List<String>> plan) {
        Map<String, List<String>> copy = new LinkedHashMap<>();
        plan.forEach((stream, writers) -> copy.put(stream, List.copyOf(writers)));
        return Map.copyOf(copy);
    }

    static Map<String, SourceProgress> legacyProgress(Map<String, String> chainIdByTable, String pipelineId) {
        Map<String, SourceProgress> progress = new LinkedHashMap<>();
        chainIdByTable.forEach((table, chainId) ->
                progress.put(table, new SourceProgress(chainId, pipelineId, ConsumerProgressKind.LEGACY)));
        return Map.copyOf(progress);
    }

    record SourceProgress(String miningChainId, String consumerId, ConsumerProgressKind kind)
            implements java.io.Serializable {
        SourceProgress {
            java.util.Objects.requireNonNull(miningChainId, "miningChainId");
            java.util.Objects.requireNonNull(consumerId, "consumerId");
            java.util.Objects.requireNonNull(kind, "kind");
        }
    }

    private record WriterBinding(String id, Set<String> streams, Map<String, List<String>> plan) {
        private WriterBinding {
            streams = Set.copyOf(streams);
            plan = plan == null ? null : copyPlan(plan);
        }
    }

    /**
     * Works out again, now that this acknowledgement has landed, how far the chain may say its source has
     * been read. Only isolated direct channels and proven single-table legacy callers use this path;
     * shared SRS capture checkpoints are advanced by recoverable log writes instead.
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
            SourceProgress progress,
            ChainPosition acked,
            Map<SourceProgress, ChainPosition> recorded) {
        SrsDurableFrontier.safeAdvance(acked, meta.consumerOffsets(progress.miningChainId())).ifPresent(safe -> {
            if (!safe.equals(recorded.get(progress))) {
                meta.advanceSourceReadOffset(progress.miningChainId(), safe);
                recorded.put(progress, safe);
            }
        });
    }

    /**
     * Whether {@code position} is where a table's snapshot sits: the one reserved position every row of a
     * snapshot carries, beneath every change of its generation. A frontier that has reached it has confirmed
     * the whole of that table's snapshot, because there is nothing of the snapshot above it left to wait on.
     *
     * <p>This is the only moment anyone learns that a table's rows are in <em>this pipeline's</em> target,
     * which is why the mark is recorded against the pipeline and not the chain -- every pipeline on a chain
     * writes somewhere of its own. The read side knows when it finished reading, which is a different
     * question again: a table read and never written looks finished to it, and a run that trusted that would
     * skip the table on its way back. The tail only replays what
     * changed after the snapshot began, so a row that never changed again would be absent from the target
     * for good -- nothing thrown, nothing logged.
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
