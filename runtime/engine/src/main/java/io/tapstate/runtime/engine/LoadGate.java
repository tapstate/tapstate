package io.tapstate.runtime.engine;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * What a source's changes wait for before they leave it: every initial load they could overtake, landed at
 * the writers of each sink that spreads loads over its writers.
 *
 * <p>A sink running several writers hands a keyed table's load rows to whichever writer has room and every
 * change to the writer its key belongs to, so a change can reach its writer while a load row of the same key
 * is still being written by another - and the older value then lands last. The change is held at its source
 * until nothing of a load it could overtake is left in flight, which is why a load is awaited at the writers
 * of a sink rather than wherever it was read: of every table written into the same target table, since a
 * row of either can carry the same key there.
 *
 * <p>It is carried onto the source vertex with the graph, so it holds only what serializes: the loads, and
 * the ack factory that answers for them on the member.
 */
public final class LoadGate implements Serializable {

    private static final long serialVersionUID = 1L;

    private final SinkAckFactory acks;
    private final List<AwaitedLoad> awaited;

    public LoadGate(SinkAckFactory acks, List<AwaitedLoad> awaited) {
        this.acks = Objects.requireNonNull(acks, "acks");
        this.awaited = List.copyOf(Objects.requireNonNull(awaited, "awaited"));
        if (this.awaited.isEmpty()) {
            throw new IllegalArgumentException("a gate that awaits no load holds nothing back");
        }
    }

    /** The loads the source's changes wait for. */
    public List<AwaitedLoad> awaited() {
        return awaited;
    }

    /** Where the loads stand, as {@code member} can read it from the pipeline's durable record. */
    public LoadLandings landingsOn(HazelcastInstance member) {
        return acks.loadLandings(member);
    }

    /**
     * {@code source}, a source vertex's supplier, with every processor it makes holding its changes at this
     * gate - every one that {@linkplain HoldsChangesForLoads can}.
     */
    public ProcessorMetaSupplier appliedTo(ProcessorMetaSupplier source) {
        return LoadGatedSource.of(source, this);
    }
}
