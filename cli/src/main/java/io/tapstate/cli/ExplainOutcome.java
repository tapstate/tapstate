package io.tapstate.cli;

import java.util.List;

/** Outcome of the shared {@code pipeline.explain} projection. */
sealed interface ExplainOutcome {

    record Found(
            String pipelineId,
            String state,
            String kind,
            String message,
            String observedAt,
            Long observedAgeMillis,
            String freshness,
            List<Evidence> evidence,
            List<String> cannotSay,
            Next next,
            Pending pending,
            Plan plan,
            List<String> awaitingRebalance) implements ExplainOutcome {

        public Found {
            evidence = List.copyOf(evidence);
            cannotSay = List.copyOf(cannotSay);
            awaitingRebalance = awaitingRebalance == null ? List.of() : List.copyOf(awaitingRebalance);
        }

        /** An explanation beside {@code plan}, which every member of the cluster was planned for. */
        Found(String pipelineId, String state, String kind, String message, String observedAt,
                Long observedAgeMillis, String freshness, List<Evidence> evidence, List<String> cannotSay, Next next,
                Pending pending, Plan plan) {
            this(pipelineId, state, kind, message, observedAt, observedAgeMillis, freshness, evidence, cannotSay,
                    next, pending, plan, List.of());
        }

        /** An explanation with no plan beside it. */
        Found(String pipelineId, String state, String kind, String message, String observedAt,
                Long observedAgeMillis, String freshness, List<Evidence> evidence, List<String> cannotSay, Next next,
                Pending pending) {
            this(pipelineId, state, kind, message, observedAt, observedAgeMillis, freshness, evidence, cannotSay,
                    next, pending, null, List.of());
        }
    }

    record Evidence(String source, String field, Object value) {
    }

    record Next(String action, String message) {
    }

    record Pending(String reason) {
    }

    /**
     * The plan the pipeline's current run was submitted on, as the server sent it: the generations are absent
     * where nothing fences the run, and so is a node's per-member count where it runs as one processor for the
     * cluster.
     */
    record Plan(Long claimGeneration, Long executionGeneration, Long topologyRevision, List<String> members,
            List<PlanNode> nodes, String plannedAt, PlanReplaced replaces) {

        public Plan {
            members = List.copyOf(members);
            nodes = List.copyOf(nodes);
        }

        /** A plan that replaced no earlier one. */
        Plan(Long claimGeneration, Long executionGeneration, Long topologyRevision, List<String> members,
                List<PlanNode> nodes, String plannedAt) {
            this(claimGeneration, executionGeneration, topologyRevision, members, nodes, plannedAt, null);
        }
    }

    /** The run a plan replaced: its generation where it had one, its members, and when it was planned. */
    record PlanReplaced(Long executionGeneration, List<String> members, String plannedAt) {

        public PlanReplaced {
            members = List.copyOf(members);
        }
    }

    /** How a node's width moved from the run before: what it was, and the inputs that moved it. */
    record PlanChange(int previousEffective, List<String> causes) {

        public PlanChange {
            causes = List.copyOf(causes);
        }
    }

    /** One node of a plan, with the batch it takes its input in and, for a sink, what it holds open and buffers. */
    record PlanNode(String node, int requested, String requestedOrigin, String scope, int memberCount,
            Integer computedLocal, int effective, List<String> reasons, int maxRecords, long maxWaitMillis,
            PlanResources resources, PlanChange change) {

        public PlanNode {
            reasons = List.copyOf(reasons);
        }

        /** A node whose width did not move, holding open and buffering what is given. */
        PlanNode(String node, int requested, String requestedOrigin, String scope, int memberCount,
                Integer computedLocal, int effective, List<String> reasons, int maxRecords, long maxWaitMillis,
                PlanResources resources) {
            this(node, requested, requestedOrigin, scope, memberCount, computedLocal, effective, reasons, maxRecords,
                    maxWaitMillis, resources, null);
        }

        /** A node that is not a sink, whose width did not move. */
        PlanNode(String node, int requested, String requestedOrigin, String scope, int memberCount,
                Integer computedLocal, int effective, List<String> reasons, int maxRecords, long maxWaitMillis) {
            this(node, requested, requestedOrigin, scope, memberCount, computedLocal, effective, reasons, maxRecords,
                    maxWaitMillis, null, null);
        }
    }

    /** What a sink holds open and buffers at its width, as the server worked it out. */
    record PlanResources(int writers, String connectorMode, int connectorInstances, long bufferedRecords,
            long edgeQueueRecords) {
    }

    record Rejected(String code, String message) implements ExplainOutcome {
    }

    record Unreachable() implements ExplainOutcome {
    }
}
