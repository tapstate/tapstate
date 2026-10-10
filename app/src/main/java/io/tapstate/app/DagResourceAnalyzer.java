package io.tapstate.app;

import com.hazelcast.jet.config.EdgeConfig;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.Vertex;
import io.tapstate.adapters.pdk.PdkCapturePort;
import io.tapstate.core.lifecycle.ClusterCapacityDemand;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.ProcessorBufferBounds;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Counts the graph the central compiler actually drew, without opening or initializing a processor. */
final class DagResourceAnalyzer {

    /** Actual configured capacities, supplied by the composition root that owns the runtime instances. */
    record Limits(int cooperativeThreads, int snapshotHandoffCapacity, int changeRingCapacity,
            int defaultEdgeQueueCapacity) {
        Limits {
            if (cooperativeThreads < 1 || snapshotHandoffCapacity < 1 || changeRingCapacity < 1
                    || defaultEdgeQueueCapacity < 1) {
                throw new IllegalArgumentException("planning capacities must be positive");
            }
        }
    }

    private DagResourceAnalyzer() { }

    static DagSource.PlanningFacts analyze(DagSource.PlannedDag plan, PipelineResource pipeline,
            Set<String> sourceVertices, Set<String> selectedSourceIds, Set<String> writers, Set<String> blockingNodes,
            Map<String, ProcessorBufferBounds> bufferBounds, Limits limits) {
        Map<String, String> nodesByVertex = new LinkedHashMap<>();
        plan.vertices().forEach((node, names) -> names.forEach(name -> {
            if (nodesByVertex.put(name, node) != null) {
                throw new IllegalStateException("a vertex belongs to more than one node");
            }
        }));
        Map<String, Step.Inline> steps = new LinkedHashMap<>();
        if (pipeline.transforms() != null) {
            pipeline.transforms().stream().filter(Step.Inline.class::isInstance).map(Step.Inline.class::cast)
                    .forEach(step -> steps.put(step.id(), step));
        }
        int members = plan.members().size();
        if (members < 1 || new LinkedHashSet<>(plan.members()).size() != members
                || members != plan.shape().plannedMembers()) {
            throw new IllegalStateException("a capacity plan requires the exact distinct planned cohort");
        }
        Map<String, Integer> local = new LinkedHashMap<>();
        for (Vertex vertex : plan.dag()) {
            local.put(vertex.getName(), vertex.determineLocalParallelism(limits.cooperativeThreads()));
        }
        List<DagSource.EdgeResources> edges = new ArrayList<>();
        Map<String, Long> inboxRecords = new LinkedHashMap<>();
        long edgeRecords = 0;
        for (Vertex source : plan.dag()) {
            for (Edge edge : plan.dag().getOutboundEdges(source.getName())) {
                int queue = roundedQueueCapacity(edge.getConfig() == null
                        ? limits.defaultEdgeQueueCapacity() : edge.getConfig().getQueueSize());
                long sending = local.get(edge.getSourceName());
                long receiving = local.get(edge.getDestName());
                long remote = edge.isDistributed() ? members - 1L : 0L;
                // Each receiver has local-producer queues plus one per remote member. Each remote sender
                // has one queue per local producer. allToOne and isolated routing can only remove queues.
                long queues = Math.addExact(Math.multiplyExact(sending, receiving),
                        Math.multiplyExact(remote, Math.addExact(sending, receiving)));
                edgeRecords = Math.addExact(edgeRecords, Math.multiplyExact(queues, queue));
                edges.add(new DagSource.EdgeResources(edge.getSourceName(), edge.getSourceOrdinal(),
                        edge.getDestName(), edge.getDestOrdinal(), edge.isDistributed(), queue, queues));
                // Hazelcast drains at most one capacity from each producer queue into its processor inbox.
                inboxRecords.merge(edge.getDestName(), Math.multiplyExact(sending + remote, queue), Math::addExact);
            }
        }

        Set<String> unknown = new LinkedHashSet<>();
        Set<String> diagnostics = new LinkedHashSet<>();
        Map<String, DagSource.VertexResources> vertices = new LinkedHashMap<>();
        ClusterCapacityDemand member = ClusterCapacityDemand.ZERO;
        ClusterCapacityDemand cluster = ClusterCapacityDemand.ZERO;
        for (Vertex vertex : plan.dag()) {
            String name = vertex.getName();
            String node = nodesByVertex.get(name);
            boolean nativeNode = node != null && plan.shape().isNative(node);
            int slots = local.get(name);
            int effective = nativeNode ? plan.shape().effectiveOf(node) : 1;
            int expectedLocal = nativeNode ? plan.shape().localOf(node) : 1;
            if (slots != expectedLocal) {
                throw new IllegalStateException("compiled vertex width differs from its execution shape: " + name);
            }
            boolean writer = writers.contains(name);
            boolean source = sourceVertices.contains(name);
            boolean blocking = source || writer || (node != null && blockingNodes.contains(node));
            long buffered = inboxRecords.getOrDefault(name, 0L);
            if (source) {
                // The hand-off can refill while its last capacity is pending or gate-held in the source.
                // A shared-ring read can additionally fill one authored read batch on that same turn.
                buffered = Math.addExact(buffered, Math.addExact(
                        Math.multiplyExact(2L, limits.snapshotHandoffCapacity()),
                        plan.batches().get(node).effectiveMaxRecords()));
            } else if (writer) {
                // One forming batch and one in-flight write; the runtime's in-flight default is exactly one.
                buffered = Math.addExact(buffered, Math.multiplyExact(2L,
                        plan.batches().get(node).effectiveMaxRecords()));
            } else if (steps.containsKey(node)) {
                Step.Inline step = steps.get(node);
                if (step.execution() != null && step.execution().batch() != null) {
                    // InputBatches retains a bounded hand-over; its iterator can retain a second view of it.
                    buffered = Math.addExact(buffered, Math.multiplyExact(2L,
                            step.execution().batch().effectiveMaxRecords()));
                }
                ProcessorBufferBounds owned = bufferBounds.get(name);
                if (owned != null) {
                    long input = step.execution() != null && step.execution().batch() != null
                            ? step.execution().batch().effectiveMaxRecords() : inboxRecords.getOrDefault(name, 0L);
                    buffered = Math.addExact(buffered, owned.upperBound(input));
                    diagnostics.addAll(owned.diagnostics());
                } else if (step.body() instanceof TransformBody.Js) {
                    unknown.add("buffered-records:" + node + ":transform-output");
                    diagnostics.add("JavaScript VM and transactional output staging are unmeasured");
                } else if (step.body() instanceof TransformBody.Join) {
                    unknown.add("buffered-records:" + node + ":join-buffer-descriptor");
                } else if (step.body() instanceof TransformBody.Nest && blockingNodes.contains(node)) {
                    unknown.add("buffered-records:" + node + ":nest-output-and-working-state");
                } else {
                    // Filter, projection, union and passthrough each retain at most one produced row.
                    buffered = Math.addExact(buffered, 1L);
                }
            }
            vertices.put(name, new DagSource.VertexResources(node, slots, effective, blocking, writer, buffered));
            long snapshotQueues = Math.multiplyExact((long) slots, EdgeConfig.DEFAULT_QUEUE_SIZE);
            member = member.plus(new ClusterCapacityDemand(slots, blocking ? slots : 0,
                    writer ? slots : 0, writer ? slots : 0, Math.multiplyExact(slots, buffered), snapshotQueues));
            cluster = cluster.plus(new ClusterCapacityDemand(Math.multiplyExact((long) slots, members),
                    blocking ? effective : 0, writer ? effective : 0, writer ? effective : 0,
                    Math.multiplyExact((long) effective, buffered), Math.multiplyExact(snapshotQueues, members)));
        }
        // PDK snapshot sessions reuse one connector across their tables. A session can remain open while
        // its CDC tail opens: two instances per source, never two per selected table. Connector internals
        // are excluded from the owned-buffer guarantee and remain explicitly unmeasured below.
        long captureConnectors = Math.multiplyExact(2L, selectedSourceIds.size());
        long captureBuffers = Math.multiplyExact(selectedSourceIds.size(), Math.addExact(
                PdkCapturePort.snapshotBufferedRecordsUpperBound(), PdkCapturePort.MAX_DELIVERY_CHUNK_RECORDS));
        long ringBuffers = Math.multiplyExact((long) sourceVertices.size(), limits.changeRingCapacity());
        ClusterCapacityDemand extra = new ClusterCapacityDemand(0, 0, 0, captureConnectors,
                Math.addExact(captureBuffers, ringBuffers), edgeRecords);
        member = member.plus(extra);
        // Reserving the full ring capacity on every member covers every primary and backup placement.
        cluster = cluster.plus(new ClusterCapacityDemand(0, 0, 0, captureConnectors,
                Math.addExact(captureBuffers, Math.multiplyExact(ringBuffers, members)),
                Math.multiplyExact(edgeRecords, members)));
        Map<String, ClusterCapacityDemand> byMember = new LinkedHashMap<>();
        for (String stableId : plan.members()) {
            byMember.put(stableId, member);
        }
        diagnostics.addAll(List.of("connector-owned input allocations are unmeasured",
                "connector pool and connection ceilings are unavailable",
                "total-one owner placement is reserved on every planned member",
                "ordinary native jobs allocate one snapshot queue per physical processor slot"));
        return new DagSource.PlanningFacts(plan.members(), plan.shape(), byMember, cluster, vertices, edges,
                List.copyOf(unknown), List.copyOf(diagnostics), selectedSourceIds);
    }

    static void requireSameGeometry(DagSource.PlanningFacts facts, DagSource.PlannedDag built) {
        if (!facts.plannedStableIds().equals(built.members()) || !facts.shape().equals(built.shape())) {
            throw new IllegalStateException("activated topology differs from its admitted cohort or shape");
        }
        Set<String> vertices = new LinkedHashSet<>();
        Set<String> edges = new LinkedHashSet<>();
        for (Vertex vertex : built.dag()) {
            vertices.add(vertex.getName());
            DagSource.VertexResources planned = facts.vertices().get(vertex.getName());
            if (planned == null || vertex.determineLocalParallelism(planned.localProcessorSlots())
                    != planned.localProcessorSlots()) {
                throw new IllegalStateException("activated vertex exceeds its admitted processor slots");
            }
            for (Edge edge : built.dag().getOutboundEdges(vertex.getName())) {
                edges.add(edgeIdentity(edge.getSourceName(), edge.getSourceOrdinal(), edge.getDestName(),
                        edge.getDestOrdinal(), edge.isDistributed()));
            }
        }
        Set<String> plannedEdges = new LinkedHashSet<>();
        facts.edges().forEach(edge -> plannedEdges.add(edgeIdentity(edge.source(), edge.sourceOrdinal(),
                edge.destination(), edge.destinationOrdinal(), edge.distributed())));
        if (!facts.vertices().keySet().equals(vertices) || !plannedEdges.equals(edges)) {
            throw new IllegalStateException("activated topology differs from its admitted vertices or edges");
        }
    }

    private static String edgeIdentity(String source, int from, String destination, int to, boolean distributed) {
        return source + '#' + from + "->" + destination + '#' + to + ':' + distributed;
    }

    private static int roundedQueueCapacity(int capacity) {
        if (capacity < 1 || capacity > (1 << 30)) {
            throw new IllegalArgumentException("unsupported native queue capacity: " + capacity);
        }
        return capacity == 1 ? 1 : Integer.highestOneBit(capacity - 1) << 1;
    }
}
