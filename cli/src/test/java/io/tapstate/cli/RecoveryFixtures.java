package io.tapstate.cli;

import io.tapstate.core.common.JsonReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Shared wire readings used by transport and command-rendering tests. */
final class RecoveryFixtures {
    private RecoveryFixtures() {}

    @SuppressWarnings("unchecked")
    static Map<String, Object> clusterRecovery() {
        try (var input = RecoveryFixtures.class.getResourceAsStream("/golden/cluster/recovery.golden.json")) {
            if (input == null) { throw new AssertionError("missing recovery wire fixture"); }
            return (Map<String, Object>) JsonReader.parse(new String(input.readAllBytes(), StandardCharsets.UTF_8));
        } catch (IOException error) {
            throw new AssertionError("cannot read recovery wire fixture", error);
        }
    }

    static Map<String, Object> pipelineRecovery() {
        Map<String, Object> cluster = clusterRecovery();
        Map<String, Object> pipeline = new LinkedHashMap<>();
        pipeline.put("pipelineId", "orders");
        pipeline.put("currentIncarnation", "incarnation-orders");
        pipeline.put("recoveryState", cluster.get("recoveryState"));
        pipeline.put("causes", cluster.get("causes"));
        pipeline.put("items", cluster.get("items"));
        pipeline.put("unavailable", null);
        pipeline.put("capacity", cluster.get("capacity"));
        return pipeline;
    }

    static Map<String, Object> status() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("pipelineId", "orders");
        status.put("state", "FAILED");
        status.put("failure", Map.of("code", "capture.start-from-outside-window", "params", Map.of(
                "requested", "resume-12", "earliest", "resume-15", "retention", "2h"),
                "message", "The requested position is outside the retained source window."));
        status.put("observedAt", "2026-10-10T09:01:00Z");
        status.put("observedAgeMillis", 1000L);
        status.put("recovery", pipelineRecovery());
        return status;
    }

    static Map<String, Object> explanation() {
        Map<String, Object> explanation = new LinkedHashMap<>();
        explanation.put("pipelineId", "orders");
        explanation.put("state", "FAILED");
        explanation.put("kind", "CODED_FAILURE");
        explanation.put("message", "The source rejected this attempt's retained position.");
        explanation.put("observedAt", "2026-10-10T09:01:00Z");
        explanation.put("observedAgeMillis", 1000L);
        explanation.put("freshness", "FRESH");
        explanation.put("evidence", List.of(Map.of("source", "status", "field", "failure", "value", status().get("failure"))));
        explanation.put("cannotSay", List.of());
        explanation.put("next", null);
        explanation.put("recovery", pipelineRecovery());
        return explanation;
    }

    static StatusOutcome.Found statusFound() {
        return new StatusOutcome.Found("orders", "FAILED", "capture.start-from-outside-window",
                "The requested position is outside the retained source window.", 1000L,
                RecoveryWire.pipeline(pipelineRecovery()), "2026-10-10T09:01:00Z",
                Map.of("requested", "resume-12", "earliest", "resume-15", "retention", "2h"), null, List.of());
    }

    static ExplainOutcome.Found explanationFound() {
        return new ExplainOutcome.Found("orders", "FAILED", "CODED_FAILURE",
                "The source rejected this attempt's retained position.", "2026-10-10T09:01:00Z", 1000L, "FRESH",
                List.of(new ExplainOutcome.Evidence("status", "failure", status().get("failure"))), List.of(),
                null, null, null, List.of(), RecoveryWire.pipeline(pipelineRecovery()));
    }

    static Map<String, Object> topology() {
        Map<String, Object> topology = new LinkedHashMap<>();
        topology.put("clusterId", "cluster-a");
        topology.put("topologyRevision", 9L);
        topology.put("profileGeneration", 2L);
        topology.put("profileHash", "profile-current");
        topology.put("members", List.of());
        topology.put("pipelines", List.of(Map.of("pipelineId", "orders", "controllerClaim", currentClaim(),
                "captureClaims", List.of(), "measuredAt", "2026-10-10T09:01:00Z", "measuredFrom", List.of("uuid-b2"),
                "awaitingRebalance", List.of(), "vertices", List.of(vertex()))));
        topology.put("recovery", clusterRecovery());
        return topology;
    }

    private static Map<String, Object> currentClaim() {
        Map<String, Object> claim = new LinkedHashMap<>();
        claim.put("resourceId", "orders");
        claim.put("ownerNodeId", "node-b");
        claim.put("ownerBootId", "boot-b2");
        claim.put("claimGeneration", 12L);
        claim.put("executionGeneration", 22L);
        claim.put("topologyRevision", 9L);
        claim.put("leased", true);
        claim.put("leaseUntil", "2026-10-10T09:01:20Z");
        claim.put("leaseRemainingMillis", 20000L);
        claim.put("profileGeneration", 2L);
        claim.put("contextExecutionGeneration", 21L);
        claim.put("executionClaimGeneration", 11L);
        claim.put("executionIncarnation", "incarnation-orders");
        claim.put("executionRevision", "artifact-previous");
        claim.put("executionTopologyRevision", 6L);
        claim.put("executionProfileGeneration", 1L);
        claim.put("executionProfileHash", "profile-original");
        claim.put("executionMembers", List.of(Map.of("nodeId", "node-b", "bootId", "boot-b1", "memberUuid", "uuid-b1")));
        claim.put("failureClaimGeneration", 11L);
        claim.put("failureAfterMemberLoss", true);
        claim.put("executionContextCurrent", false);
        return claim;
    }

    private static Map<String, Object> vertex() {
        Map<String, Object> unknown = new LinkedHashMap<>();
        unknown.put("index", 0L);
        unknown.put("localIndex", null);
        unknown.put("memberUuid", "uuid-a2");
        unknown.put("nodeId", "node-a");
        unknown.put("context", null);
        Map<String, Object> actual = new LinkedHashMap<>();
        actual.put("index", 3L);
        actual.put("localIndex", 1L);
        actual.put("memberUuid", "uuid-b2");
        actual.put("nodeId", "node-b");
        actual.put("context", processorContext());
        return Map.of("name", "serve-orders", "requested", 4L, "effective", 4L, "computedLocal", 2L,
                "executionId", "1729407264346316802", "processors", List.of(unknown, actual));
    }

    static Map<String, Object> processorContext() {
        Map<String, Object> context = new LinkedHashMap<>();
        context.put("pipelineId", "orders");
        context.put("vertex", "serve-orders");
        context.put("jobId", "1729407264346316801");
        context.put("runtimeExecutionId", "1729407264346316802");
        context.put("claimGeneration", 12L);
        context.put("executionGeneration", 22L);
        context.put("profileGeneration", 2L);
        context.put("nodeId", "node-b");
        context.put("bootId", "boot-b2");
        context.put("memberUuid", "uuid-b2");
        context.put("memberAddress", "node-b:5701");
        context.put("memberIndex", 1L);
        context.put("localProcessorIndex", 1L);
        context.put("globalProcessorIndex", 3L);
        context.put("localParallelism", 2L);
        context.put("totalParallelism", 4L);
        context.put("memberCount", 2L);
        context.put("initializedAt", "2026-10-10T09:00:45Z");
        return context;
    }
}
