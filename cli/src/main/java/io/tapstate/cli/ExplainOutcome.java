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
            Plan plan) implements ExplainOutcome {

        public Found {
            evidence = List.copyOf(evidence);
            cannotSay = List.copyOf(cannotSay);
        }

        /** An explanation with no plan beside it. */
        Found(String pipelineId, String state, String kind, String message, String observedAt,
                Long observedAgeMillis, String freshness, List<Evidence> evidence, List<String> cannotSay, Next next,
                Pending pending) {
            this(pipelineId, state, kind, message, observedAt, observedAgeMillis, freshness, evidence, cannotSay,
                    next, pending, null);
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
            List<PlanNode> nodes, String plannedAt) {

        public Plan {
            members = List.copyOf(members);
            nodes = List.copyOf(nodes);
        }
    }

    /** One node of a plan, with the batch it takes its input in and, for a sink, what it holds open and buffers. */
    record PlanNode(String node, int requested, String requestedOrigin, String scope, int memberCount,
            Integer computedLocal, int effective, List<String> reasons, int maxRecords, long maxWaitMillis,
            PlanResources resources) {

        public PlanNode {
            reasons = List.copyOf(reasons);
        }

        /** A node that is not a sink. */
        PlanNode(String node, int requested, String requestedOrigin, String scope, int memberCount,
                Integer computedLocal, int effective, List<String> reasons, int maxRecords, long maxWaitMillis) {
            this(node, requested, requestedOrigin, scope, memberCount, computedLocal, effective, reasons, maxRecords,
                    maxWaitMillis, null);
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
