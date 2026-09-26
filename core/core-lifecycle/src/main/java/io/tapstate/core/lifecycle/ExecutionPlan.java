package io.tapstate.core.lifecycle;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * How one execution of a pipeline was planned to run, written down as it was submitted: the width every node was
 * worked out to run at, for the members the execution takes part on, the batch each node takes its input in, and
 * which run it is.
 *
 * <p>It belongs to that execution and is replaced by the next one's. Nothing reads it to decide how a run goes: it
 * is what a reader asking why a node runs as wide as it does is answered from, and it says only what was decided
 * when the run was submitted. A value this run does not have is absent rather than zero - a run on a single member
 * is fenced by nothing, so it has no generations to name.
 *
 * @param pipelineId          the pipeline
 * @param claimGeneration     the generation of the claim the run was submitted under, or null where nothing is
 *                            fenced
 * @param executionGeneration the run's own generation, which every submission moves, or null likewise
 * @param topologyRevision    the committed topology the claim was held under, or null likewise
 * @param members             the members the widths were worked out for, by stable id
 * @param nodes               every node the run draws, in the order they were worked out
 * @param plannedAt           when the run was planned
 */
public record ExecutionPlan(
        String pipelineId,
        Long claimGeneration,
        Long executionGeneration,
        Long topologyRevision,
        List<String> members,
        List<Node> nodes,
        Instant plannedAt) implements Serializable {

    private static final long serialVersionUID = 1L;

    public ExecutionPlan {
        Objects.requireNonNull(pipelineId, "pipelineId");
        members = List.copyOf(Objects.requireNonNull(members, "members"));
        nodes = List.copyOf(Objects.requireNonNull(nodes, "nodes"));
        Objects.requireNonNull(plannedAt, "plannedAt");
    }

    /**
     * One node as it was planned: the target it was given and where that came from, whether it runs as one
     * processor for the cluster or the same number on every member, the members and per-member count it was
     * worked out for, the width that makes, why it is not the target where it is not, the batch it takes its
     * input in, the vertices of the run that run at that width, and - for a sink - what it holds open and
     * buffers at that width. {@code origin} and {@code scope} are their wire spellings; {@code computedLocal} is
     * absent for a node run as one processor for the cluster, and {@code resources} for any node but a sink.
     */
    public record Node(
            String node,
            int requested,
            String origin,
            String scope,
            int memberCount,
            Integer computedLocal,
            int effective,
            List<String> reasons,
            int maxRecords,
            long maxWaitMillis,
            List<String> vertices,
            Resources resources) implements Serializable {

        private static final long serialVersionUID = 1L;

        public Node {
            Objects.requireNonNull(node, "node");
            Objects.requireNonNull(origin, "origin");
            Objects.requireNonNull(scope, "scope");
            reasons = List.copyOf(Objects.requireNonNull(reasons, "reasons"));
            vertices = List.copyOf(Objects.requireNonNull(vertices, "vertices"));
        }

        /** A node that holds open and buffers nothing worked out here: any node but a sink. */
        public Node(String node, int requested, String origin, String scope, int memberCount, Integer computedLocal,
                int effective, List<String> reasons, int maxRecords, long maxWaitMillis, List<String> vertices) {
            this(node, requested, origin, scope, memberCount, computedLocal, effective, reasons, maxRecords,
                    maxWaitMillis, vertices, null);
        }

        /** The same node, holding open and buffering what {@code resources} says. */
        public Node withResources(Resources resources) {
            return new Node(node, requested, origin, scope, memberCount, computedLocal, effective, reasons,
                    maxRecords, maxWaitMillis, vertices, resources);
        }

        /**
         * The node {@code parallelism} worked out, taking its input in batches of the size and wait given, run by
         * {@code vertices}.
         */
        public static Node of(NodeParallelism parallelism, int maxRecords, long maxWaitMillis, List<String> vertices) {
            return new Node(parallelism.node(), parallelism.requested(), parallelism.origin().id(),
                    parallelism.scope().id(), parallelism.memberCount(), parallelism.computedLocal(),
                    parallelism.effective(), parallelism.reasons(), maxRecords, maxWaitMillis, vertices);
        }
    }

    /**
     * What a sink holds open and buffers at the width it was planned at, as upper bounds worked out before anything
     * is opened: its writers; whether each writer opens a connector of its own ({@code isolated}) or the writers on
     * one member share one ({@code shared}), and how many connectors that opens across the run; the most records
     * its writers hold between them, forming a batch and in flight; and the most records the queues of the edges
     * carrying its input can hold. A connector's own connection pool is sized inside the connector and is not
     * counted here.
     *
     * @param writers              processors writing the sink's rows, across the run
     * @param connectorMode        {@code isolated} or {@code shared}
     * @param connectorInstances   connectors opened across the run
     * @param bufferedRecords      records the writers hold at most: two batches each, one forming and one written
     * @param edgeQueueRecords     records the queues of the edges into the sink hold at most: every queue from a
     *                             processor sending into it to a processor it sends to, each full
     */
    public record Resources(
            int writers,
            String connectorMode,
            int connectorInstances,
            long bufferedRecords,
            long edgeQueueRecords) implements Serializable {

        private static final long serialVersionUID = 1L;

        /** Each writer opens a connector of its own. */
        public static final String ISOLATED = "isolated";

        /** The writers on one member share one connector. */
        public static final String SHARED = "shared";

        public Resources {
            Objects.requireNonNull(connectorMode, "connectorMode");
            if (!ISOLATED.equals(connectorMode) && !SHARED.equals(connectorMode)) {
                throw new IllegalArgumentException("a connector is isolated or shared, not " + connectorMode);
            }
        }
    }
}
