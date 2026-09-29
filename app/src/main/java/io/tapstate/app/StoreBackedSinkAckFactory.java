package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.SinkAck;
import io.tapstate.runtime.engine.SinkAckFactory;
import io.tapstate.runtime.srs.CaptureRunUnit;
import io.tapstate.spi.store.SrsMetaStore;
import java.util.Map;

/**
 * The production sink-ack factory carried onto the DAG: as a sink confirms writes, it records what one
 * consumer pipeline has durably landed, table by table, so the reader of each chain has something to release
 * the chain's positions from. It holds only serializable coordinates — a {@code table -> mining chain id} map
 * for every source the pipeline reads, plus the consumer pipeline id — and resolves the durable store on the
 * member that runs the sink, mirroring how the source's read-cursor publisher binds its store member-side.
 * The store itself is not serializable and never crosses the wire.
 *
 * <p>The sink knows a chain only by the {@code src} stream name its events carry — a table at L1 — so this
 * maps that stream to the mining chain that keys its durable record. A member with no store bound resolves
 * to a no-op ack, so a sink still runs before the assembly layer makes the member SRS-capable. A stream the
 * map does not carry is a builder-side wiring defect (the sink saw a chain the pipeline never sourced) and
 * crashes bare.
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
            if (isSnapshotOf(position)) {
                meta.markSnapshotComplete(miningChainId, pipelineId, chain);
            } else {
                // Recorded against the change's own table, at the order it was read under, so that whoever
                // reads the chain can see which tables have landed what -- and so a run replacing this one
                // carries on from it instead of from the head of the table's ring.
                meta.advanceTableSinkAcked(miningChainId, pipelineId, chain, position);
            }
        };
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
