package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.runtime.engine.SinkAck;
import io.tapstate.runtime.engine.SinkAckFactory;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.SrsMetaStore;
import java.util.Map;

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
 * <p>A position that carries no token is a snapshot row, and what is persisted for it is the chain's cdc
 * start position: the read has confirmed rows of a snapshot but no change at all, so a resume belongs
 * where changes begin. Resolving it here rather than at the sink is what keeps the durable store out of
 * the engine — the sink says which position it reached, this says what that spells on disk.
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
        return (chain, position) -> {
            String miningChainId = chainIdByTable.get(chain);
            if (miningChainId == null) {
                throw new IllegalStateException(
                        "sink acked a chain the pipeline never sourced: '" + chain + "'");
            }
            String token = position.token() != null ? position.token() : cdcStart(meta, miningChainId);
            meta.advanceSinkAcked(miningChainId, pipelineId, new ChainPosition(position.order(), token));
        };
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
                        + miningChainId + "', which has no meta record to resume from"));
    }
}
