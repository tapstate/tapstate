package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.app.StoreBackedSinkAckFactory.SourceProgress;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.ReplayFloor;
import io.tapstate.runtime.engine.ReplayFloorFactory;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.ConsumerProgressKind;
import io.tapstate.spi.store.SrsMetaStore;
import java.util.Map;
import java.util.Optional;

/**
 * The production replay-floor factory carried onto the graph: it reads back, for each source node,
 * where a restart would resume each table it reads. It is the exact mirror of the sink-ack factory - the
 * same table, physical chain and source-scoped consumer coordinates, the same store resolved
 * on the member that runs the vertex - only it reads what that one writes.
 *
 * <p>The position that matters is this pipeline's own. Another pipeline reading the same chain may be
 * further behind, which keeps the source from discarding what it still needs, but it has no bearing on
 * where <em>this</em> job resumes, and it is this job's replay that decides whether a change it already
 * applied can arrive a second time.
 *
 * <p>Everything reads as "not known" rather than "nothing acked": a member with no store bound, a stream
 * the pipeline never sourced, a chain with no record for this pipeline, and an acked position with no order
 * all answer empty. Each of them leaves the caller keeping what it holds, which is the direction that
 * cannot corrupt anything.
 */
final class StoreBackedReplayFloorFactory implements ReplayFloorFactory {

    private static final long serialVersionUID = 1L;

    private final Map<String, SourceProgress> progressByTable;

    StoreBackedReplayFloorFactory(Map<String, String> chainIdByTable, String pipelineId) {
        this(StoreBackedSinkAckFactory.legacyProgress(chainIdByTable, pipelineId));
    }

    StoreBackedReplayFloorFactory(Map<String, SourceProgress> progressByTable) {
        this.progressByTable = Map.copyOf(progressByTable);
    }

    @Override
    public ReplayFloor resolve(HazelcastInstance member) {
        Object bound = member.getUserContext().get(CaptureRunUnit.SRS_META_USER_CONTEXT_KEY);
        if (!(bound instanceof SrsMetaStore meta)) {
            return ReplayFloor.NONE;
        }
        return chain -> {
            SourceProgress progress = progressByTable.get(chain);
            return progress == null ? Optional.empty() : ackedOrder(meta, progress, chain);
        };
    }

    /** Where this pipeline's sink has confirmed writes up to on the chain, as an order to compare on. */
    private Optional<SourceOrder> ackedOrder(SrsMetaStore meta, SourceProgress progress, String table) {
        return meta.read(progress.miningChainId())
                .flatMap(record -> record.consumerOffset(progress.consumerId()))
                .map(offset -> tablePosition(offset, progress, table))
                .map(ChainPosition::order);
    }

    private ChainPosition tablePosition(ConsumerOffset offset, SourceProgress progress, String table) {
        ChainPosition tablePosition = offset.sinkAckedByTable().get(table);
        if (tablePosition != null) {
            return tablePosition;
        }
        // Old single-table callers have one order space. An aggregate spanning table rings is no floor.
        if (offset.progressKind() == ConsumerProgressKind.DIRECT_SOURCE
                || (offset.progressKind() == ConsumerProgressKind.LEGACY
                        && offset.perTableSeq().size() <= 1
                        && progressByTable.values().stream().filter(progress::equals).count() == 1)) {
            return offset.sinkAcked();
        }
        return null;
    }
}
