package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.SinkAck;
import io.tapstate.runtime.engine.SinkAckFactory;
import io.tapstate.runtime.srs.CaptureRunUnit;
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
 * The production sink-ack factory carried onto the DAG: as a sink confirms writes, it records what one
 * consumer pipeline has durably landed, table by table and writer by writer, so the reader of each chain has
 * something to release the chain's positions from. It holds only serializable coordinates — a
 * {@code table -> mining chain id} map for every source the pipeline reads, the consumer pipeline id, and the
 * stable writer plan compiled from the DAG — and resolves the durable store on the member that runs the sink,
 * mirroring how the source's read-cursor publisher binds its store member-side. An assembly-only store
 * handle checks the complete writer plan before the DAG can be submitted; it is transient, so the store
 * itself never crosses the wire.
 *
 * <p>The sink knows a chain only by the {@code src} stream name its events carry — a table at L1 — so this
 * maps that stream to the mining chain that keys its durable record and advances that writer's position.
 * The store takes the lowest every writer expected on the table has reached as what the table has landed, so
 * a fast target cannot let a replacement run pass changes another target has not written. A member with no
 * store bound resolves to a no-op ack, so a sink still runs before the assembly layer makes the member
 * SRS-capable. A stream the map does not carry is a builder-side wiring defect (the sink saw a chain the
 * pipeline never sourced) and crashes bare.
 *
 * <p><strong>A confirmation here never moves the chain.</strong> One source log feeds every table of a
 * chain, and a quiet table's change can land while an earlier change of another table is still in flight;
 * moving the chain on the quiet table's word would let a restart resume past the other one, and the source
 * never sends it again. What is written is the table's own confirmation, at the order it was read under. The
 * reader of the chain -- the only party that knows in what order the source handed its changes over -- moves
 * the chain's source read offset and this pipeline's chain-level position once every change up to a point of
 * the source log has landed for every table and every pipeline that reads it, and it re-checks these
 * confirmations on its own while the source is quiet.
 *
 * <p>A change can carry no token, and it is recorded by its order alone -- which is all the release ranks on.
 * A source names a position for a run of changes when it has one and names none when it has not, and that
 * absence is load-bearing: the recipient carries on rather than inventing a position, because an invented one
 * claims changes were read that were not, and a later run would resume past them.
 */
final class StoreBackedSinkAckFactory implements SinkAckFactory {

    private static final long serialVersionUID = 1L;

    private final Map<String, String> chainIdByTable;
    private final String pipelineId;
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
        this(chainIdByTable, pipelineId, null, Set.of(), Map.of(),
                new AtomicInteger(), new ConcurrentHashMap<>(), assemblyStore);
    }

    private StoreBackedSinkAckFactory(
            Map<String, String> chainIdByTable,
            String pipelineId,
            String writerId,
            Set<String> writerStreams,
            Map<String, List<String>> writerIdsByStream,
            AtomicInteger resolvedWriterSequence,
            Map<String, List<String>> resolvedWriterIdsByStream,
            SrsMetaStore assemblyStore) {
        this.chainIdByTable = Map.copyOf(chainIdByTable);
        this.pipelineId = pipelineId;
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
        LinkedHashSet<String> miningChainIds = new LinkedHashSet<>();
        for (String stream : writerIdsByStream.keySet()) {
            String miningChainId = chainIdByTable.get(stream);
            if (miningChainId == null) {
                throw new IllegalStateException(
                        "sink writer plan names a chain the pipeline never sourced: '" + stream + "'");
            }
            miningChainIds.add(miningChainId);
        }
        for (String miningChainId : miningChainIds) {
            if (assemblyStore.read(miningChainId).isPresent()) {
                configure(assemblyStore, miningChainId, writerIdsByStream);
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
                chainIdByTable,
                pipelineId,
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
        ConcurrentMap<String, Optional<WorkloadClaimFence>> configuredMiningChains = new ConcurrentHashMap<>();
        return new SinkAck() {
            @Override
            public void advance(String chain, ChainPosition position) {
                advance(chain, position, null);
            }

            @Override
            public void advance(String chain, ChainPosition position, WorkloadClaimFence fence) {
                acknowledge(meta, writer, configuredMiningChains, chain, position, fence);
            }
        };
    }

    /** Maps one engine acknowledgement to the durable consumer write under the claim that admitted it. */
    private void acknowledge(
            SrsMetaStore meta,
            WriterBinding writer,
            ConcurrentMap<String, Optional<WorkloadClaimFence>> configuredMiningChains,
            String chain,
            ChainPosition position,
            WorkloadClaimFence fence) {
        String miningChainId = chainIdByTable.get(chain);
        if (miningChainId == null) {
            throw new IllegalStateException(
                    "sink acked a chain the pipeline never sourced: '" + chain + "'");
        }
        if (!writer.streams().contains(chain)) {
            throw new IllegalStateException("sink writer '" + writer.id()
                    + "' acked a chain it does not receive: '" + chain + "'");
        }
        ensureConfigured(meta, writer, miningChainId, configuredMiningChains, fence);
        if (isSnapshotOf(position)) {
            markWriterSnapshotComplete(meta, miningChainId, writer.id(), chain, fence);
        } else {
            // Recorded against the change's own table, at the order it was read under, so that whoever
            // reads the chain can see which tables have landed what -- and so a run replacing this one
            // carries on from it instead of from the head of the table's ring.
            advanceWriterAck(meta, miningChainId, writer.id(), chain, position, fence);
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
            for (String stream : chainIdByTable.keySet()) {
                resolvedWriterIdsByStream.compute(stream, (ignored, writers) -> {
                    List<String> next = new ArrayList<>(writers == null ? List.of() : writers);
                    next.add(resolvedId);
                    return List.copyOf(next);
                });
            }
            return new WriterBinding(resolvedId, chainIdByTable.keySet(), null);
        }
    }

    /** Registers a writer's complete plan for one mining chain whenever its authorized fence changes. */
    private void ensureConfigured(
            SrsMetaStore meta,
            WriterBinding writer,
            String miningChainId,
            ConcurrentMap<String, Optional<WorkloadClaimFence>> configuredMiningChains,
            WorkloadClaimFence fence) {
        Optional<WorkloadClaimFence> expected = Optional.ofNullable(fence);
        configuredMiningChains.compute(miningChainId, (ignored, configured) -> {
            if (expected.equals(configured)) {
                return configured;
            }
            Map<String, List<String>> plan = writer.plan();
            if (plan == null) {
                synchronized (resolvedWriterIdsByStream) {
                    plan = copyPlan(resolvedWriterIdsByStream);
                }
            }
            configure(meta, miningChainId, plan, fence);
            return expected;
        });
    }

    /** Registers the complete table-to-writer plan for {@code miningChainId} without touching another. */
    private void configure(
            SrsMetaStore meta, String miningChainId, Map<String, List<String>> plan) {
        configure(meta, miningChainId, plan, null);
    }

    private void configure(
            SrsMetaStore meta,
            String miningChainId,
            Map<String, List<String>> plan,
            WorkloadClaimFence fence) {
        Map<String, List<String>> writersByTable = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> entry : plan.entrySet()) {
            String entryMiningChainId = chainIdByTable.get(entry.getKey());
            if (entryMiningChainId == null) {
                throw new IllegalStateException(
                        "sink writer plan names a chain the pipeline never sourced: '" + entry.getKey() + "'");
            }
            if (entryMiningChainId.equals(miningChainId)) {
                writersByTable.put(entry.getKey(), entry.getValue());
            }
        }
        if (writersByTable.isEmpty()) {
            throw new IllegalStateException(
                    "sink writer plan names no stream on mining chain '" + miningChainId + "'");
        }
        if (fence == null) {
            meta.configureSinkWriters(miningChainId, pipelineId, writersByTable);
        } else {
            meta.configureSinkWriters(miningChainId, pipelineId, writersByTable, fence);
        }
    }

    private void advanceWriterAck(
            SrsMetaStore meta,
            String miningChainId,
            String writerId,
            String table,
            ChainPosition acked,
            WorkloadClaimFence fence) {
        if (fence == null) {
            meta.advanceSinkWriterAcked(miningChainId, pipelineId, writerId, table, acked);
        } else {
            meta.advanceSinkWriterAcked(miningChainId, pipelineId, writerId, table, acked, fence);
        }
    }

    private void markWriterSnapshotComplete(
            SrsMetaStore meta,
            String miningChainId,
            String writerId,
            String table,
            WorkloadClaimFence fence) {
        if (fence == null) {
            meta.markSinkWriterSnapshotComplete(miningChainId, pipelineId, writerId, table);
        } else {
            meta.markSinkWriterSnapshotComplete(miningChainId, pipelineId, writerId, table, fence);
        }
    }

    private static Map<String, List<String>> copyPlan(Map<String, List<String>> plan) {
        Map<String, List<String>> copy = new LinkedHashMap<>();
        plan.forEach((stream, writers) -> copy.put(stream, List.copyOf(writers)));
        return Map.copyOf(copy);
    }

    private record WriterBinding(String id, Set<String> streams, Map<String, List<String>> plan) {
        private WriterBinding {
            streams = Set.copyOf(streams);
            plan = plan == null ? null : copyPlan(plan);
        }
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
}
