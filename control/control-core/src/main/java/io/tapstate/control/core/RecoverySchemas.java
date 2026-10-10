package io.tapstate.control.core;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import static java.util.Map.entry;

/** Closed schemas for the existing typed recovery companion; dynamic maps retain their value types. */
final class RecoverySchemas {
    private RecoverySchemas() {}

    static Map<String, Object> pipeline() {
        Map<String, Object> text = Map.of("type", "string");
        Map<String, Object> number = Map.of("type", "integer");
        Map<String, Object> count = Map.of("type", "integer", "minimum", 0);
        Map<String, Object> flag = Map.of("type", "boolean");
        Map<String, Object> time = Map.of("type", "string", "format", "date-time");
        Map<String, Object> profile = object(Map.of(
                "generation", count, "formatVersion", count, "hash", text, "attributes", dictionary(text)));
        Map<String, Object> claim = object(Map.ofEntries(
                entry("clusterId", text), entry("type", text), entry("resourceId", text), entry("ownerNodeId", text),
                entry("ownerBootId", text), entry("claimGeneration", count), entry("executionGeneration", count),
                entry("topologyRevision", count), entry("profileGeneration", count)));
        Map<String, Object> reading = object(Map.of("claim", claim, "leaseUntil", time,
                "leaseRemainingMillis", number, "leased", flag));
        Map<String, Object> failure = object(Map.of("code", text, "params", dictionary(Map.of())));
        Map<String, Object> resources = object(Map.of(
                "processors", count, "blockingProcessors", count, "writers", count,
                "connectorInstances", count, "bufferedRecords", count, "edgeQueueRecords", count));
        Map<String, Object> capacity = object(Map.of(
                "availability", enumeration("AVAILABLE", "PROFILE_ABSENT", "UNAVAILABLE"), "provenance", text,
                "profile", nullable(profile), "configuredLimits", nullable(resources),
                "occupiedByNode", nullable(dictionary(resources)), "unavailable", nullable(failure)));
        Map<String, Object> position = object(Map.ofEntries(
                entry("connectorId", text), entry("captureId", text),
                entry("kind", enumeration("DURABLE_POSITION", "SNAPSHOT_REQUIRED")), entry("epoch", nullable(number)),
                entry("sequence", nullable(number)), entry("token", nullable(text)), entry("provenance", text),
                entry("reference", nullable(text))));
        Map<String, Object> positions = dictionary(position);
        Map<String, Object> diagnostic = object(Map.of(
                "reason", enumeration("CAPACITY_REFUSED", "REBUILD_REFUSED", "EXECUTION_FAILED", "SOURCE_POSITION_REJECTED"),
                "code", text, "params", dictionary(Map.of()), "positions", positions, "disposition", text));
        Map<String, Object> note = object(Map.of(
                "pipelineClaim", claim, "stage", enumeration("ALLOCATED_EXECUTION", "SOURCE_POSITION_REJECTION"),
                "diagnostic", diagnostic, "recordedAt", time));
        Map<String, Object> permit = object(Map.of(
                "reservationId", text, "reservedAt", time, "deadline", time, "coordinator", claim,
                "transferredExecutionGeneration", count, "demandByNode", dictionary(resources)));
        Map<String, Object> successor = object(Map.ofEntries(
                entry("pipelineClaim", claim), entry("profile", profile), entry("executionNodeIds", array(text)),
                entry("requiredSourceIds", array(text)), entry("sourceRequirementsRecorded", flag), entry("allocatedAt", time),
                entry("nativeJobId", nullable(text)), entry("submittedAt", nullable(time)),
                entry("nativeInitializedAt", nullable(time)), entry("sourcesAcceptedAt", nullable(time)),
                entry("executionCompleted", nullable(flag)), entry("requestedPositions", positions),
                entry("acceptedPositions", positions), entry("failureNote", nullable(note))));
        Map<String, Object> cause = enumeration("MEMBER_LOSS", "FULL_CLUSTER_RESTART");
        Map<String, Object> state = enumeration("WAITING_QUORUM", "WAITING_PERMIT", "REBUILDING", "RETRY_BACKOFF",
                "REBUILD_FAILED", "RECOVERED", "CANCELLED");
        Map<String, Object> item = object(Map.ofEntries(
                entry("pipelineId", text), entry("incarnation", text), entry("currentArtifactHash", text),
                entry("intentFingerprint", text), entry("cause", cause), entry("persistedStatus", state), entry("status", state),
                entry("enqueueSequence", count), entry("enqueuedAt", time), entry("updatedAt", time),
                entry("queuePosition", nullable(count)), entry("attempt", count), entry("maxAttempts", count),
                entry("nextEligibleAt", nullable(time)), entry("originalExecutionGeneration", count),
                entry("originalExecutionRevision", nullable(text)), entry("sourceTopologyRevision", nullable(count)),
                entry("originalProfile", nullable(profile)), entry("legacySourceProfile", flag), entry("targetProfile", profile),
                entry("targetTopologyRevision", count), entry("executionFrontier", count), entry("originalPositions", positions),
                entry("permit", nullable(permit)), entry("successor", nullable(successor)), entry("diagnostic", nullable(diagnostic)),
                entry("currentPipelineClaim", nullable(reading)), entry("claimUnavailable", nullable(failure))));
        return object(Map.of(
                "pipelineId", text, "currentIncarnation", nullable(text),
                "recoveryState", nullable(enumeration("RECOVERING", "IDLE")), "causes", array(cause), "items", array(item),
                "unavailable", nullable(failure), "capacity", capacity));
    }

    private static Map<String, Object> object(Map<String, Object> properties) {
        Map<String, Object> sorted = new TreeMap<>(properties);
        return Map.of("type", "object", "properties", java.util.Collections.unmodifiableMap(sorted), "required", List.copyOf(sorted.keySet()),
                "additionalProperties", false);
    }

    private static Map<String, Object> dictionary(Map<String, Object> value) {
        return Map.of("type", "object", "additionalProperties", value);
    }

    private static Map<String, Object> nullable(Map<String, Object> value) {
        return Map.of("oneOf", List.of(value, Map.of("type", "null")));
    }

    private static Map<String, Object> array(Map<String, Object> value) { return Map.of("type", "array", "items", value); }

    private static Map<String, Object> enumeration(String... values) {
        return Map.of("type", "string", "enum", List.of(values));
    }
}
