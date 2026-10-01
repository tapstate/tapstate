package io.tapstate.core.lifecycle;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * @param replaces            the run this one's plan replaced, or null where the pipeline had none planned
 *                            before
 */
public record ExecutionPlan(
        String pipelineId,
        Long claimGeneration,
        Long executionGeneration,
        Long topologyRevision,
        List<String> members,
        List<Node> nodes,
        Instant plannedAt,
        Replaced replaces) implements Serializable {

    private static final long serialVersionUID = 1L;

    public ExecutionPlan {
        Objects.requireNonNull(pipelineId, "pipelineId");
        members = List.copyOf(Objects.requireNonNull(members, "members"));
        nodes = List.copyOf(Objects.requireNonNull(nodes, "nodes"));
        Objects.requireNonNull(plannedAt, "plannedAt");
    }

    /** A plan that replaced no earlier one. */
    public ExecutionPlan(String pipelineId, Long claimGeneration, Long executionGeneration, Long topologyRevision,
            List<String> members, List<Node> nodes, Instant plannedAt) {
        this(pipelineId, claimGeneration, executionGeneration, topologyRevision, members, nodes, plannedAt, null);
    }

    /**
     * The members among {@code members} this plan's widths were not worked out for, in their order: members that
     * joined after the run was planned. A run keeps the members it was planned over, so these are given no part
     * of it until it is rebalanced - which is not a failure, and is told apart from a run rebuilt after losing a
     * member, whose plan names the run it replaced.
     */
    public List<String> notPlannedFor(List<String> members) {
        return members.stream().filter(member -> !this.members.contains(member)).toList();
    }

    /**
     * This plan as the one that replaced {@code previous}: naming the run it replaced, and saying of each node
     * whose width moved what it was and which of its inputs moved it. {@code previous} null is a pipeline with
     * no plan before, and leaves this one as it is.
     */
    public ExecutionPlan replacing(ExecutionPlan previous) {
        if (previous == null) {
            return this;
        }
        Map<String, Node> before = new HashMap<>();
        previous.nodes().forEach(node -> before.put(node.node(), node));
        List<Node> after = nodes.stream().map(node -> node.after(before.get(node.node()))).toList();
        return new ExecutionPlan(pipelineId, claimGeneration, executionGeneration, topologyRevision, members, after,
                plannedAt, new Replaced(previous.executionGeneration(), previous.members(), previous.plannedAt()));
    }

    /**
     * The run a plan replaced, as far as a reader asking what changed needs it: its generation, the members its
     * widths were worked out for, and when it was planned.
     *
     * @param executionGeneration the replaced run's generation, or null where nothing fenced it
     * @param members             the members its widths were worked out for, by stable id
     * @param plannedAt           when it was planned
     */
    public record Replaced(Long executionGeneration, List<String> members, Instant plannedAt)
            implements Serializable {

        private static final long serialVersionUID = 1L;

        public Replaced {
            members = List.copyOf(Objects.requireNonNull(members, "members"));
            Objects.requireNonNull(plannedAt, "plannedAt");
        }
    }

    /**
     * How a node's width moved from the run before: the width it ran at then, and each input of the working-out
     * that moved - named apart rather than folded into one reason, because each is answered by someone else.
     * {@code members-changed}: the run was worked out for a different number of members, as after one was lost.
     * {@code target-changed}: its author gave it a different target. {@code capability-changed}: what bounds the
     * node moved - whether its rows carry a key, or a budget it is held to.
     *
     * @param previousEffective the processors the node ran in total in the run before
     * @param causes            the inputs that moved, in that order; empty where none can be named
     */
    public record Change(int previousEffective, List<String> causes) implements Serializable {

        private static final long serialVersionUID = 1L;

        /** The run was worked out for a different number of members. */
        public static final String MEMBERS_CHANGED = "members-changed";

        /** The node's author gave it a different target. */
        public static final String TARGET_CHANGED = "target-changed";

        /** What bounds the node moved: whether its rows carry a key, or a budget it is held to. */
        public static final String CAPABILITY_CHANGED = "capability-changed";

        public Change {
            causes = List.copyOf(Objects.requireNonNull(causes, "causes"));
        }
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
            Resources resources,
            Change change) implements Serializable {

        private static final long serialVersionUID = 1L;

        public Node {
            Objects.requireNonNull(node, "node");
            Objects.requireNonNull(origin, "origin");
            Objects.requireNonNull(scope, "scope");
            reasons = List.copyOf(Objects.requireNonNull(reasons, "reasons"));
            vertices = List.copyOf(Objects.requireNonNull(vertices, "vertices"));
        }

        /** A node whose width did not move from a run before, holding open and buffering what is given. */
        public Node(String node, int requested, String origin, String scope, int memberCount, Integer computedLocal,
                int effective, List<String> reasons, int maxRecords, long maxWaitMillis, List<String> vertices,
                Resources resources) {
            this(node, requested, origin, scope, memberCount, computedLocal, effective, reasons, maxRecords,
                    maxWaitMillis, vertices, resources, null);
        }

        /** A node that holds open and buffers nothing worked out here: any node but a sink. */
        public Node(String node, int requested, String origin, String scope, int memberCount, Integer computedLocal,
                int effective, List<String> reasons, int maxRecords, long maxWaitMillis, List<String> vertices) {
            this(node, requested, origin, scope, memberCount, computedLocal, effective, reasons, maxRecords,
                    maxWaitMillis, vertices, null, null);
        }

        /** The same node, holding open and buffering what {@code resources} says. */
        public Node withResources(Resources resources) {
            return new Node(node, requested, origin, scope, memberCount, computedLocal, effective, reasons,
                    maxRecords, maxWaitMillis, vertices, resources, change);
        }

        /**
         * This node as worked out after {@code before} - the same node in the run before, or null where that run
         * had none: saying how its width moved, where it moved.
         */
        Node after(Node before) {
            if (before == null || before.effective() == effective) {
                return this;
            }
            List<String> causes = new ArrayList<>();
            if (before.memberCount() != memberCount) {
                causes.add(Change.MEMBERS_CHANGED);
            }
            if (before.requested() != requested) {
                causes.add(Change.TARGET_CHANGED);
            }
            if (!before.reasons().equals(reasons)) {
                causes.add(Change.CAPABILITY_CHANGED);
            }
            return new Node(node, requested, origin, scope, memberCount, computedLocal, effective, reasons,
                    maxRecords, maxWaitMillis, vertices, resources, new Change(before.effective(), causes));
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
