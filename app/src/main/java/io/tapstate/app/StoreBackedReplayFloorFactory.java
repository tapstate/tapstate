package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.ReplayFloor;
import io.tapstate.runtime.engine.ReplayFloorFactory;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.spi.store.SrsMetaStore;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The production replay-floor factory carried onto the graph: it reads back, for one consumer pipeline,
 * where a restart would resume each chain it reads. It is the exact mirror of the sink-ack factory - the
 * same {@code table -> mining chain id} coordinates, the same consumer pipeline id, the same store resolved
 * on the member that runs the vertex - only it reads what that one writes.
 *
 * <p>The position that matters is this pipeline's own. Another pipeline reading the same chain may be
 * further behind, which keeps the source from discarding what it still needs, but it has no bearing on
 * where <em>this</em> job resumes, and it is this job's replay that decides whether a change it already
 * applied can arrive a second time.
 *
 * <p>What it reads is the table's own confirmation, not the chain's. A deletion is weighed against the
 * floor by the order it was read under, and that order is a place in its own table's ring; the chain-level
 * position is a place in the order the source handed runs over, which ranks nothing in any one ring. Read
 * against it, a deletion on a quiet table could look safely behind a floor its table never reached.
 *
 * <p>Everything reads as "not known" rather than "nothing acked": a member with no store bound, a stream
 * the pipeline never sourced, a chain with no record for this pipeline, and a table with no confirmation
 * recorded all answer empty. Each of them leaves the caller keeping what it holds, which is the direction
 * that cannot corrupt anything.
 */
final class StoreBackedReplayFloorFactory implements ReplayFloorFactory {

    private static final long serialVersionUID = 1L;

    private final Map<String, String> chainIdByTable;
    private final String pipelineId;

    StoreBackedReplayFloorFactory(Map<String, String> chainIdByTable, String pipelineId) {
        this.chainIdByTable = Map.copyOf(chainIdByTable);
        this.pipelineId = Objects.requireNonNull(pipelineId, "pipelineId");
    }

    @Override
    public ReplayFloor resolve(HazelcastInstance member) {
        Object bound = member.getUserContext().get(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY);
        if (!(bound instanceof SrsMetaStore meta)) {
            return ReplayFloor.NONE;
        }
        return chain -> {
            String miningChainId = chainIdByTable.get(chain);
            return miningChainId == null ? Optional.empty() : ackedOrder(meta, miningChainId, chain);
        };
    }

    /** Where this pipeline's sink has confirmed {@code table}'s writes up to, as an order to compare on. */
    private Optional<SourceOrder> ackedOrder(SrsMetaStore meta, String miningChainId, String table) {
        return meta.consumerOffsets(miningChainId).stream()
                .filter(offset -> offset.pipelineId().equals(pipelineId))
                .map(offset -> offset.sinkAckedByTable().get(table))
                .filter(Objects::nonNull)
                .map(ChainPosition::order)
                .filter(Objects::nonNull)
                .findFirst();
    }
}
