package io.tapstate.app;

import com.hazelcast.jet.config.EdgeConfig;
import io.tapstate.core.lifecycle.ExecutionPlan;
import io.tapstate.core.lifecycle.NodeParallelism;

import java.util.List;
import java.util.Map;

/**
 * What a sink of a run holds open and buffers at the width it was planned at, worked out from the plan before
 * anything is opened - so a reader sees the multiplication a width implies rather than the width alone.
 *
 * <p>Every number is an upper bound or a count, never a measurement: a pool grows on demand inside its connector,
 * a batch fills only as rows arrive, and a queue holds rows only while its consumer is behind.
 */
final class PlannedSinkResources {

    /** The records the queue from one processor to another holds when full: the engine's default. */
    static final int EDGE_QUEUE_RECORDS = EdgeConfig.DEFAULT_QUEUE_SIZE;

    private PlannedSinkResources() {
    }

    /**
     * {@code sink}'s resources, at the width {@code sink} was worked out for: its writers; the connectors they open -
     * one each, or one per member running writers where {@code shared} says every member loaded an artifact
     * certified to be shared; two batches of {@code maxRecords} per writer, one forming and one being written; and a
     * full queue from every processor of each vertex in {@code feeding} to every processor the sink takes its input
     * on. A sink running wide takes its input on a router as wide as itself, which hands each row to its writer over
     * two edges of its own, and their queues count too. A vertex {@code processorsByVertex} does not name runs as
     * one processor for the cluster.
     */
    static ExecutionPlan.Resources of(NodeParallelism sink, int maxRecords, boolean shared, List<String> feeding,
            Map<String, Integer> processorsByVertex) {
        int writers = sink.effective();
        boolean wide = sink.scope() == NodeParallelism.Scope.NATIVE;
        int connectors = !shared ? writers : wide ? sink.memberCount() : 1;
        long sending = feeding.stream().mapToLong(vertex -> processorsByVertex.getOrDefault(vertex, 1)).sum();
        long queues = sending * writers + (wide ? 2L * writers * writers : 0L);
        return new ExecutionPlan.Resources(writers,
                shared ? ExecutionPlan.Resources.SHARED : ExecutionPlan.Resources.ISOLATED, connectors,
                2L * writers * maxRecords, queues * EDGE_QUEUE_RECORDS);
    }
}
