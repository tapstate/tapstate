package io.tapstate.cli;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** Explicit typed decoding and machine rendering; neither path infers recovery state. */
final class RecoveryWire {
    private RecoveryWire() {}

    static RemoteRecovery.Cluster cluster(Object raw) {
        return read(raw, m -> new RemoteRecovery.Cluster(text(m, "clusterId"), bool(m, "quorumReady"),
                required(text(m, "recoveryState")), strings(m, "causes"), objects(m, "items", RecoveryWire::item),
                profile(m.get("currentProfile")), failure(m.get("profileUnavailable")),
                claimReading(m.get("coordinatorClaim")), failure(m.get("claimUnavailable")),
                capacity(m.get("capacity")), failure(m.get("queueUnavailable"))));
    }

    static RemoteRecovery.Pipeline pipeline(Object raw) {
        return read(raw, m -> new RemoteRecovery.Pipeline(text(m, "pipelineId"), text(m, "currentIncarnation"),
                required(text(m, "recoveryState")), strings(m, "causes"), objects(m, "items", RecoveryWire::item),
                failure(m.get("unavailable")), capacity(m.get("capacity"))));
    }

    private static RemoteRecovery.Item item(Object raw) {
        return read(raw, m -> new RemoteRecovery.Item(text(m, "pipelineId"), text(m, "incarnation"),
                text(m, "currentArtifactHash"), text(m, "intentFingerprint"), text(m, "cause"),
                text(m, "persistedStatus"), text(m, "status"), number(m, "enqueueSequence"),
                text(m, "enqueuedAt"), text(m, "updatedAt"), integer(m, "queuePosition"),
                integer(m, "attempt"), integer(m, "maxAttempts"), text(m, "nextEligibleAt"),
                number(m, "originalExecutionGeneration"), text(m, "originalExecutionRevision"),
                number(m, "sourceTopologyRevision"), profile(m.get("originalProfile")), bool(m, "legacySourceProfile"),
                profile(m.get("targetProfile")), number(m, "targetTopologyRevision"), number(m, "executionFrontier"),
                keyed(m.get("originalPositions"), RecoveryWire::position), permit(m.get("permit")),
                successor(m.get("successor")), diagnostic(m.get("diagnostic")),
                claimReading(m.get("currentPipelineClaim")), failure(m.get("claimUnavailable"))));
    }

    private static RemoteRecovery.Profile profile(Object raw) {
        return read(raw, m -> new RemoteRecovery.Profile(number(m, "generation"), integer(m, "formatVersion"),
                text(m, "hash"), keyed(m.get("attributes"), value -> {
                    if (!(value instanceof String string)) { throw shape(); }
                    return string;
                })));
    }

    private static RemoteRecovery.Claim claim(Object raw) {
        return read(raw, m -> new RemoteRecovery.Claim(text(m, "clusterId"), text(m, "type"), text(m, "resourceId"),
                text(m, "ownerNodeId"), text(m, "ownerBootId"), number(m, "claimGeneration"),
                number(m, "executionGeneration"), number(m, "topologyRevision"), number(m, "profileGeneration")));
    }

    private static RemoteRecovery.ClaimReading claimReading(Object raw) {
        return read(raw, m -> new RemoteRecovery.ClaimReading(required(claim(m.get("claim"))), text(m, "leaseUntil"),
                number(m, "leaseRemainingMillis"), bool(m, "leased")));
    }

    private static RemoteRecovery.Permit permit(Object raw) {
        return read(raw, m -> new RemoteRecovery.Permit(text(m, "reservationId"), text(m, "reservedAt"),
                text(m, "deadline"), required(claim(m.get("coordinator"))), number(m, "transferredExecutionGeneration"),
                keyed(m.get("demandByNode"), RecoveryWire::counts)));
    }

    private static RemoteRecovery.Successor successor(Object raw) {
        return read(raw, m -> new RemoteRecovery.Successor(required(claim(m.get("pipelineClaim"))), required(profile(m.get("profile"))),
                strings(m, "executionNodeIds"), strings(m, "requiredSourceIds"), bool(m, "sourceRequirementsRecorded"),
                text(m, "allocatedAt"), text(m, "nativeJobId"), text(m, "submittedAt"), text(m, "nativeInitializedAt"),
                text(m, "sourcesAcceptedAt"), bool(m, "executionCompleted"),
                keyed(m.get("requestedPositions"), RecoveryWire::position),
                keyed(m.get("acceptedPositions"), RecoveryWire::position), note(m.get("failureNote"))));
    }

    private static RemoteRecovery.FailureNote note(Object raw) {
        return read(raw, m -> new RemoteRecovery.FailureNote(required(claim(m.get("pipelineClaim"))), required(text(m, "stage")),
                required(diagnostic(m.get("diagnostic"))), required(text(m, "recordedAt"))));
    }

    private static RemoteRecovery.Position position(Object raw) {
        return read(raw, m -> new RemoteRecovery.Position(text(m, "connectorId"), text(m, "captureId"),
                text(m, "kind"), number(m, "epoch"), number(m, "sequence"), text(m, "token"),
                text(m, "provenance"), text(m, "reference")));
    }

    private static RemoteRecovery.Diagnostic diagnostic(Object raw) {
        return read(raw, m -> new RemoteRecovery.Diagnostic(required(text(m, "reason")), required(text(m, "code")),
                objectMap(m.get("params")), keyed(m.get("positions"), RecoveryWire::position), text(m, "disposition")));
    }

    private static RemoteRecovery.Capacity capacity(Object raw) {
        return read(raw, m -> new RemoteRecovery.Capacity(text(m, "availability"), text(m, "provenance"),
                profile(m.get("profile")), counts(m.get("configuredLimits")),
                m.get("occupiedByNode") == null ? null : keyed(m.get("occupiedByNode"), RecoveryWire::counts),
                failure(m.get("unavailable"))));
    }

    private static RemoteRecovery.Counts counts(Object raw) {
        return read(raw, m -> new RemoteRecovery.Counts(number(m, "processors"), number(m, "blockingProcessors"),
                number(m, "writers"), number(m, "connectorInstances"), number(m, "bufferedRecords"), number(m, "edgeQueueRecords")));
    }

    private static RemoteRecovery.ReadFailure failure(Object raw) {
        return read(raw, m -> new RemoteRecovery.ReadFailure(required(text(m, "code")), objectMap(m.get("params"))));
    }

    static RemoteProcessorContext processorContext(Object raw) {
        return read(raw, m -> new RemoteProcessorContext(text(m, "pipelineId"), text(m, "vertex"), text(m, "jobId"),
                text(m, "runtimeExecutionId"), number(m, "claimGeneration"), number(m, "executionGeneration"),
                number(m, "profileGeneration"), text(m, "nodeId"), text(m, "bootId"), text(m, "memberUuid"),
                text(m, "memberAddress"), integer(m, "memberIndex"), integer(m, "localProcessorIndex"),
                integer(m, "globalProcessorIndex"), integer(m, "localParallelism"), integer(m, "totalParallelism"),
                integer(m, "memberCount"), text(m, "initializedAt")));
    }

    static List<RemoteClaim.Member> members(Object raw) {
        if (raw == null) { return null; }
        return list(raw, value -> read(value, m -> new RemoteClaim.Member(text(m, "nodeId"), text(m, "bootId"), text(m, "memberUuid"))));
    }

    static Map<String, Object> objectMap(Object raw) {
        return keyed(raw, Function.identity());
    }

    static String text(Map<?, ?> m, String name) {
        Object value = m.get(name);
        if (value == null || value instanceof String) { return (String) value; }
        throw shape();
    }

    static Long number(Map<?, ?> m, String name) {
        Object value = m.get(name);
        if (value == null) { return null; }
        if (value instanceof Number n) { return new BigDecimal(n.toString()).longValueExact(); }
        throw shape();
    }

    static Integer integer(Map<?, ?> m, String name) {
        Long value = number(m, name);
        return value == null ? null : Math.toIntExact(value);
    }

    static Boolean bool(Map<?, ?> m, String name) {
        Object value = m.get(name);
        if (value == null || value instanceof Boolean) { return (Boolean) value; }
        throw shape();
    }

    private static List<String> strings(Map<?, ?> m, String name) {
        return list(m.get(name), value -> {
            if (!(value instanceof String string)) { throw shape(); }
            return string;
        });
    }

    private static <T> List<T> objects(Map<?, ?> m, String name, Function<Object, T> decoder) {
        return list(m.get(name), decoder);
    }

    private static <T> List<T> list(Object raw, Function<Object, T> decoder) {
        if (!(raw instanceof List<?> list)) { throw shape(); }
        List<T> result = new ArrayList<>();
        for (Object value : list) {
            T decoded = decoder.apply(value);
            if (decoded == null) { throw shape(); }
            result.add(decoded);
        }
        return List.copyOf(result);
    }

    private static <T> Map<String, T> keyed(Object raw, Function<Object, T> decoder) {
        if (!(raw instanceof Map<?, ?> map)) { throw shape(); }
        Map<String, T> result = new LinkedHashMap<>();
        map.forEach((key, value) -> {
            if (!(key instanceof String name)) { throw shape(); }
            result.put(name, decoder.apply(value));
        });
        return result;
    }

    private static <T> T read(Object raw, Function<Map<?, ?>, T> decoder) {
        if (raw == null) { return null; }
        if (!(raw instanceof Map<?, ?> map)) { throw shape(); }
        return decoder.apply(map);
    }

    private static IllegalArgumentException shape() {
        return new IllegalArgumentException("invalid recovery wire shape");
    }

    private static <T> T required(T value) {
        if (value == null) { throw shape(); }
        return value;
    }

    static Object tree(Object value) {
        if (value == null) { return null; }
        return switch (value) {
            case RemoteRecovery.Cluster v -> fields("clusterId", v.clusterId(), "quorumReady", v.quorumReady(),
                    "recoveryState", v.recoveryState(), "causes", v.causes(), "items", v.items(),
                    "currentProfile", v.currentProfile(), "profileUnavailable", v.profileUnavailable(),
                    "coordinatorClaim", v.coordinatorClaim(), "claimUnavailable", v.claimUnavailable(),
                    "capacity", v.capacity(), "queueUnavailable", v.queueUnavailable());
            case RemoteRecovery.Pipeline v -> fields("pipelineId", v.pipelineId(), "currentIncarnation", v.currentIncarnation(),
                    "recoveryState", v.recoveryState(), "causes", v.causes(), "items", v.items(),
                    "unavailable", v.unavailable(), "capacity", v.capacity());
            case RemoteRecovery.Item v -> fields("pipelineId", v.pipelineId(), "incarnation", v.incarnation(),
                    "currentArtifactHash", v.currentArtifactHash(), "intentFingerprint", v.intentFingerprint(),
                    "cause", v.cause(), "persistedStatus", v.persistedStatus(), "status", v.status(),
                    "enqueueSequence", v.enqueueSequence(), "enqueuedAt", v.enqueuedAt(), "updatedAt", v.updatedAt(),
                    "queuePosition", v.queuePosition(), "attempt", v.attempt(), "maxAttempts", v.maxAttempts(),
                    "nextEligibleAt", v.nextEligibleAt(), "originalExecutionGeneration", v.originalExecutionGeneration(),
                    "originalExecutionRevision", v.originalExecutionRevision(), "sourceTopologyRevision", v.sourceTopologyRevision(),
                    "originalProfile", v.originalProfile(), "legacySourceProfile", v.legacySourceProfile(),
                    "targetProfile", v.targetProfile(), "targetTopologyRevision", v.targetTopologyRevision(),
                    "executionFrontier", v.executionFrontier(), "originalPositions", v.originalPositions(),
                    "permit", v.permit(), "successor", v.successor(), "diagnostic", v.diagnostic(),
                    "currentPipelineClaim", v.currentPipelineClaim(), "claimUnavailable", v.claimUnavailable());
            case RemoteRecovery.Profile v -> fields("generation", v.generation(), "formatVersion", v.formatVersion(), "hash", v.hash(), "attributes", v.attributes());
            case RemoteRecovery.Claim v -> fields("clusterId", v.clusterId(), "type", v.type(), "resourceId", v.resourceId(),
                    "ownerNodeId", v.ownerNodeId(), "ownerBootId", v.ownerBootId(), "claimGeneration", v.claimGeneration(),
                    "executionGeneration", v.executionGeneration(), "topologyRevision", v.topologyRevision(), "profileGeneration", v.profileGeneration());
            case RemoteRecovery.ClaimReading v -> fields("claim", v.claim(), "leaseUntil", v.leaseUntil(), "leaseRemainingMillis", v.leaseRemainingMillis(), "leased", v.leased());
            case RemoteRecovery.Permit v -> fields("reservationId", v.reservationId(), "reservedAt", v.reservedAt(),
                    "deadline", v.deadline(), "coordinator", v.coordinator(), "transferredExecutionGeneration", v.transferredExecutionGeneration(), "demandByNode", v.demandByNode());
            case RemoteRecovery.Successor v -> fields("pipelineClaim", v.pipelineClaim(), "profile", v.profile(),
                    "executionNodeIds", v.executionNodeIds(), "requiredSourceIds", v.requiredSourceIds(),
                    "sourceRequirementsRecorded", v.sourceRequirementsRecorded(), "allocatedAt", v.allocatedAt(),
                    "nativeJobId", v.nativeJobId(), "submittedAt", v.submittedAt(), "nativeInitializedAt", v.nativeInitializedAt(),
                    "sourcesAcceptedAt", v.sourcesAcceptedAt(), "executionCompleted", v.executionCompleted(),
                    "requestedPositions", v.requestedPositions(), "acceptedPositions", v.acceptedPositions(), "failureNote", v.failureNote());
            case RemoteRecovery.FailureNote v -> fields("pipelineClaim", v.pipelineClaim(), "stage", v.stage(), "diagnostic", v.diagnostic(), "recordedAt", v.recordedAt());
            case RemoteRecovery.Position v -> fields("connectorId", v.connectorId(), "captureId", v.captureId(),
                    "kind", v.kind(), "epoch", v.epoch(), "sequence", v.sequence(), "token", v.token(), "provenance", v.provenance(), "reference", v.reference());
            case RemoteRecovery.Diagnostic v -> fields("reason", v.reason(), "code", v.code(), "params", v.params(), "positions", v.positions(), "disposition", v.disposition());
            case RemoteRecovery.Capacity v -> fields("availability", v.availability(), "provenance", v.provenance(),
                    "profile", v.profile(), "configuredLimits", v.configuredLimits(), "occupiedByNode", v.occupiedByNode(), "unavailable", v.unavailable());
            case RemoteRecovery.Counts v -> fields("processors", v.processors(), "blockingProcessors", v.blockingProcessors(),
                    "writers", v.writers(), "connectorInstances", v.connectorInstances(), "bufferedRecords", v.bufferedRecords(), "edgeQueueRecords", v.edgeQueueRecords());
            case RemoteRecovery.ReadFailure v -> fields("code", v.code(), "params", v.params());
            case RemoteProcessorContext v -> fields("pipelineId", v.pipelineId(), "vertex", v.vertex(), "jobId", v.jobId(),
                    "runtimeExecutionId", v.runtimeExecutionId(), "claimGeneration", v.claimGeneration(),
                    "executionGeneration", v.executionGeneration(), "profileGeneration", v.profileGeneration(),
                    "nodeId", v.nodeId(), "bootId", v.bootId(), "memberUuid", v.memberUuid(), "memberAddress", v.memberAddress(),
                    "memberIndex", v.memberIndex(), "localProcessorIndex", v.localProcessorIndex(), "globalProcessorIndex", v.globalProcessorIndex(),
                    "localParallelism", v.localParallelism(), "totalParallelism", v.totalParallelism(), "memberCount", v.memberCount(), "initializedAt", v.initializedAt());
            case RemoteClaim.Member v -> fields("nodeId", v.nodeId(), "bootId", v.bootId(), "memberUuid", v.memberUuid());
            case List<?> list -> list.stream().map(RecoveryWire::tree).toList();
            case Map<?, ?> map -> {
                Map<String, Object> result = new LinkedHashMap<>();
                map.forEach((key, entry) -> result.put((String) key, tree(entry)));
                yield result;
            }
            default -> value;
        };
    }

    private static Map<String, Object> fields(Object... values) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < values.length; i += 2) { result.put((String) values[i], tree(values[i + 1])); }
        return result;
    }
}
