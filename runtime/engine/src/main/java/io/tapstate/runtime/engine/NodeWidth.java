package io.tapstate.runtime.engine;

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import io.tapstate.core.model.BatchSpec;
import java.util.Objects;

/**
 * How wide one pipeline node runs in an execution, for a node that draws several vertices of its own - a nest or
 * a join - so that every one of them, and every edge into one of them, is drawn to match.
 *
 * <p>A node runs either as one processor for the whole cluster, or as the same number of processors on every
 * member taking part. The first is the engine's named exception: its vertices are pinned to one member, and every
 * edge into one of them has to deliver to that one processor, since the other members run only stand-ins that
 * refuse input. The second routes each row by the key of the state it is about to change, so that one key's rows
 * meet on one processor, and is held to the member count its width was worked out for.
 *
 * <p>{@code batch} is the batch the node's author asked for, if any: its vertices then take their input in it.
 */
public record NodeWidth(String node, int local, int plannedMembers, BatchSpec batch) {

    public NodeWidth {
        Objects.requireNonNull(node, "node");
        if (local < 0) {
            throw new IllegalArgumentException("a node runs no fewer than zero processors per member, got " + local);
        }
        if (plannedMembers < 1) {
            throw new IllegalArgumentException("a run is planned over at least one member, got " + plannedMembers);
        }
    }

    /** One processor for the whole cluster. */
    public static NodeWidth totalOne(String node, BatchSpec batch) {
        return new NodeWidth(node, 0, 1, batch);
    }

    /** Whether the node runs the same number of processors on every member rather than one in total. */
    public boolean isNative() {
        return local > 0;
    }

    /** The meta-supplier of the node's vertex named {@code vertex}, whose processors {@code processors} makes. */
    public ProcessorMetaSupplier metaSupplier(String vertex, ProcessorSupplier processors) {
        ProcessorMetaSupplier meta = isNative()
                ? PlannedMembersGuard.of(ProcessorMetaSupplier.of(processors), plannedMembers)
                : ProcessorMetaSupplier.forceTotalParallelismOne(processors, vertex);
        return InputBatches.around(meta, batch);
    }

    /** {@code vertex}, running as many processors on each member as the node does where it runs natively. */
    public Vertex sized(Vertex vertex) {
        return isNative() ? vertex.localParallelism(local) : vertex;
    }

    /**
     * {@code edge}, into the node's vertex named {@code destination}: routed by {@code key} where the node runs
     * natively, and to the one processor it runs otherwise.
     */
    public Edge into(Edge edge, String destination, FunctionEx<?, ?> key) {
        return isNative() ? edge.partitioned(key).distributed() : edge.distributed().allToOne(destination);
    }
}
