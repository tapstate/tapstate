package io.tapstate.control.restapi;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.tapstate.core.lifecycle.ExecutionPlan;

import java.util.List;

/**
 * The plan a run was submitted on, as every read face that carries it sends it, so that a client reads one shape
 * wherever it asks why each node runs as wide as it does: which run it is, the members the widths were worked out
 * for, and every node in the order it was worked out. A value the run does not have is omitted rather than sent as
 * zero or null - a run on a single member is fenced by nothing, so it names no generations.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
record ExecutionPlanResponse(Long claimGeneration, Long executionGeneration, Long topologyRevision,
        List<String> members, List<Node> nodes, String plannedAt) {

    /** {@code plan} as a read face sends it, or null where there is none. */
    static ExecutionPlanResponse of(ExecutionPlan plan) {
        return plan == null ? null : new ExecutionPlanResponse(plan.claimGeneration(), plan.executionGeneration(),
                plan.topologyRevision(), plan.members(), plan.nodes().stream().map(Node::of).toList(),
                plan.plannedAt().toString());
    }

    /**
     * One node of a plan: the target it was given and where that came from, whether it runs as one processor for
     * the cluster or the same number on every member, the member count and per-member count it was worked out
     * for, the width that makes, why that is not the target where it is not, the batch it takes its input in, and
     * for a sink what it holds open and buffers at that width. {@code computedLocal} is omitted for a node run as
     * one processor for the cluster, which has none, and {@code resources} for any node but a sink.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    record Node(String node, int requested, String requestedOrigin, String scope, int memberCount,
            Integer computedLocal, int effective, List<String> reasons, Batch batch, Resources resources) {

        static Node of(ExecutionPlan.Node node) {
            ExecutionPlan.Resources resources = node.resources();
            return new Node(node.node(), node.requested(), node.origin(), node.scope(), node.memberCount(),
                    node.computedLocal(), node.effective(), node.reasons(),
                    new Batch(node.maxRecords(), node.maxWaitMillis()),
                    resources == null ? null : new Resources(resources.writers(), resources.connectorMode(),
                            resources.connectorInstances(), resources.bufferedRecords(),
                            resources.edgeQueueRecords()));
        }
    }

    /**
     * What a sink holds open and buffers at its width, as upper bounds: its writers, whether each opens a connector
     * of its own or the writers on one member share one and how many that opens, the most records the writers
     * hold, and the most the queues of the edges into it hold.
     */
    record Resources(int writers, String connectorMode, int connectorInstances, long bufferedRecords,
            long edgeQueueRecords) {
    }

    /** The most records a node takes its input in at once, and the longest it waits for them. */
    record Batch(int maxRecords, long maxWaitMillis) {
    }
}
