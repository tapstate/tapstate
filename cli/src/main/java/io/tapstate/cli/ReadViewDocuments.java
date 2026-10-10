package io.tapstate.cli;

import java.util.LinkedHashMap;
import java.util.Map;

/** Machine documents rendered from the existing typed status and explanation reads. */
final class ReadViewDocuments {
    private ReadViewDocuments() {}

    static Map<String, Object> status(StatusOutcome.Found status) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("pipelineId", status.pipelineId());
        result.put("state", status.state());
        if (status.failureCode() != null) {
            result.put("failure", Map.of("code", status.failureCode(), "params", status.failureParams(), "message", status.failureMessage()));
        }
        present(result, "observedAt", status.observedAt());
        present(result, "observedAgeMillis", status.observedAgeMillis());
        present(result, "plan", status.plan() == null ? null : plan(status.plan()));
        if (!status.awaitingRebalance().isEmpty()) { result.put("awaitingRebalance", status.awaitingRebalance()); }
        present(result, "recovery", RecoveryWire.tree(status.recovery()));
        return result;
    }

    static Map<String, Object> explanation(ExplainOutcome.Found answer) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("pipelineId", answer.pipelineId());
        result.put("state", answer.state());
        result.put("kind", answer.kind());
        result.put("message", answer.message());
        present(result, "observedAt", answer.observedAt());
        present(result, "observedAgeMillis", answer.observedAgeMillis());
        result.put("freshness", answer.freshness());
        result.put("evidence", answer.evidence().stream().map(evidence -> {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("source", evidence.source());
            item.put("field", evidence.field());
            item.put("value", evidence.value());
            return item;
        }).toList());
        result.put("cannotSay", answer.cannotSay());
        result.put("next", answer.next() == null ? null : Map.of("action", answer.next().action(), "message", answer.next().message()));
        present(result, "pending", answer.pending() == null ? null : Map.of("reason", answer.pending().reason()));
        present(result, "plan", answer.plan() == null ? null : plan(answer.plan()));
        if (!answer.awaitingRebalance().isEmpty()) { result.put("awaitingRebalance", answer.awaitingRebalance()); }
        present(result, "recovery", RecoveryWire.tree(answer.recovery()));
        return result;
    }

    private static Map<String, Object> plan(ExplainOutcome.Plan plan) {
        Map<String, Object> result = new LinkedHashMap<>();
        present(result, "claimGeneration", plan.claimGeneration());
        present(result, "executionGeneration", plan.executionGeneration());
        present(result, "topologyRevision", plan.topologyRevision());
        result.put("members", plan.members());
        result.put("nodes", plan.nodes().stream().map(node -> {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("node", node.node());
            item.put("requested", node.requested());
            item.put("requestedOrigin", node.requestedOrigin());
            item.put("scope", node.scope());
            item.put("memberCount", node.memberCount());
            present(item, "computedLocal", node.computedLocal());
            item.put("effective", node.effective());
            item.put("reasons", node.reasons());
            item.put("batch", Map.of("maxRecords", node.maxRecords(), "maxWaitMillis", node.maxWaitMillis()));
            if (node.resources() != null) {
                var resources = node.resources();
                item.put("resources", Map.of("writers", resources.writers(), "connectorMode", resources.connectorMode(),
                        "connectorInstances", resources.connectorInstances(), "bufferedRecords", resources.bufferedRecords(),
                        "edgeQueueRecords", resources.edgeQueueRecords()));
            }
            if (node.change() != null) {
                item.put("change", Map.of("previousEffective", node.change().previousEffective(), "causes", node.change().causes()));
            }
            return item;
        }).toList());
        result.put("plannedAt", plan.plannedAt());
        if (plan.replaces() != null) {
            Map<String, Object> previous = new LinkedHashMap<>();
            present(previous, "executionGeneration", plan.replaces().executionGeneration());
            previous.put("members", plan.replaces().members());
            previous.put("plannedAt", plan.replaces().plannedAt());
            result.put("replaces", previous);
        }
        return result;
    }

    private static void present(Map<String, Object> document, String name, Object value) {
        if (value != null) { document.put(name, value); }
    }
}
