package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.SinkAck;
import io.tapstate.runtime.engine.SinkAckFactory;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.runtime.srs.SrsDurableFrontier;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The production sink-ack factory carried onto the DAG: it advances one consumer pipeline's durable
 * sink-acked source position as the sink confirms writes, so the source-read durable frontier has a real
 * input. It holds only serializable coordinates — a {@code table -> mining chain id} map for every source
 * the pipeline reads, plus the consumer pipeline id — and resolves the durable store on the member that
 * runs the sink, mirroring how the source's read-cursor publisher binds its store member-side. The store
 * itself is not serializable and never crosses the wire.
 *
 * <p>The sink knows a chain only by the {@code src} stream name its events carry — a table at L1 — so this
 * maps that stream to the mining chain that keys its durable record and advances
 * {@code (miningChainId, pipelineId, srcpos)}. A member with no store bound resolves to a no-op ack, so a
 * sink still runs before the assembly layer makes the member SRS-capable. A stream the map does not carry
 * is a builder-side wiring defect (the sink saw a chain the pipeline never sourced) and crashes bare.
 *
 * <p>A snapshot row is ordered but carries no token, and what is persisted for it is the chain's cdc
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

    private final Map<String, String> chainIdByTable;
    private final String pipelineId;

    StoreBackedSinkAckFactory(Map<String, String> chainIdByTable, String pipelineId) {
        this.chainIdByTable = Map.copyOf(chainIdByTable);
        this.pipelineId = pipelineId;
    }

    @Override
    public SinkAck resolve(HazelcastInstance member) {
        Object bound = member.getUserContext().get(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY);
        if (!(bound instanceof SrsMetaStore meta)) {
            return (chain, position) -> { };
        }
        Map<String, ChainPosition> recorded = new ConcurrentHashMap<>();
        return (chain, position) -> {
            String miningChainId = chainIdByTable.get(chain);
            if (miningChainId == null) {
                throw new IllegalStateException(
                        "sink acked a chain the pipeline never sourced: '" + chain + "'");
            }
            String token = position.token() != null ? position.token()
                    : isSnapshotOf(position) ? cdcStart(meta, miningChainId) : null;
            ChainPosition acked = new ChainPosition(position.order(), token);
            meta.advanceSinkAcked(miningChainId, pipelineId, acked);
            if (isSnapshotOf(position)) {
                meta.markSnapshotComplete(miningChainId, pipelineId, chain);
            } else {
                recordHowFarTheSourceHasBeenRead(meta, miningChainId, acked, recorded);
            }
        };
    }

    /**
     * Works out again, now that this acknowledgement has landed, how far the chain may say its source has
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
     * Where changes begin on {@code miningChainId}, for a frontier that has only reached snapshot rows. The
     * capture writes it before it drains a snapshot, so a snapshot row reaching a sink without one means the
     * chain was never seeded — the same caller-ordering defect as acking an unseeded chain, and it crashes
     * bare rather than writing an absent position over a real one.
     */
    private static String cdcStart(SrsMetaStore meta, String miningChainId) {
        return meta.read(miningChainId)
                .map(SrsMeta::cdcStartPosition)
                .orElseThrow(() -> new IllegalStateException("sink acked snapshot rows of mining chain '"
                        + miningChainId + "', which has no recorded position for changes to begin at"));
    }
}
