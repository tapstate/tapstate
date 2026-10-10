package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.lifecycle.ClusterCapacityDemand;
import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ClusterRecoveryCause;
import io.tapstate.spi.store.ClusterRecoveryDiagnostic;
import io.tapstate.spi.store.ClusterRecoveryEvent;
import io.tapstate.spi.store.ClusterRecoveryItem;
import io.tapstate.spi.store.ClusterRecoveryKey;
import io.tapstate.spi.store.ClusterRecoveryPermit;
import io.tapstate.spi.store.ClusterRecoveryPosition;
import io.tapstate.spi.store.ClusterRecoveryStartupReceipt;
import io.tapstate.spi.store.ClusterRecoveryStatus;
import io.tapstate.spi.store.ClusterRecoverySuccessor;
import io.tapstate.spi.store.ExecutionProfile;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.bson.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Strict versioned queue mapping; persisted strings never deserialize error-code enums. */
final class ClusterRecoveryDocuments {
    private ClusterRecoveryDocuments() {}

    static Document id(ClusterRecoveryKey key) {
        return new Document("clusterId", key.clusterId()).append("pipelineId", key.pipelineId())
                .append("incarnation", key.incarnation());
    }

    static Document item(ClusterRecoveryItem item) {
        return new Document("_id", id(item.event().key())).append("clusterId", item.event().key().clusterId())
                .append("pipelineId", item.event().key().pipelineId()).append("incarnation", item.event().key().incarnation())
                .append("schemaVersion", item.schemaVersion()).append("event", event(item.event()))
                .append("itemRevision", item.itemRevision()).append("enqueueSequence", item.enqueueSequence())
                .append("enqueuedAt", date(item.enqueuedAt())).append("updatedAt", date(item.updatedAt()))
                .append("targetProfile", profile(item.targetProfile())).append("targetTopologyRevision", item.targetTopologyRevision())
                .append("status", item.status().name()).append("attempt", item.attempt()).append("maxAttempts", item.maxAttempts())
                .append("nextEligibleAt", date(item.nextEligibleAt()))
                .append("executionAliases", item.executionAliases().stream().sorted().toList())
                .append("permit", item.permit() == null ? null : permit(item.permit()))
                .append("successor", item.successor() == null ? null : successor(item.successor()))
                .append("diagnostic", item.diagnostic() == null ? null : diagnostic(item.diagnostic()));
    }

    static ClusterRecoveryItem item(Document document) {
        try {
            ClusterRecoveryItem item = new ClusterRecoveryItem(integer(document, "schemaVersion"),
                    event(required(document, "event")), number(document, "itemRevision"), number(document, "enqueueSequence"),
                    instant(document, "enqueuedAt"), instant(document, "updatedAt"), profile(required(document, "targetProfile")),
                    number(document, "targetTopologyRevision"), ClusterRecoveryStatus.valueOf(document.getString("status")),
                    integer(document, "attempt"), integer(document, "maxAttempts"), instantOrNull(document, "nextEligibleAt"),
                    Set.copyOf(document.getList("executionAliases", Number.class).stream().map(Number::longValue).toList()),
                    document.get("permit") == null ? null : permit(required(document, "permit")),
                    document.get("successor") == null ? null : successor(required(document, "successor")),
                    document.get("diagnostic") == null ? null : diagnostic(required(document, "diagnostic")));
            if (!id(item.event().key()).equals(document.get("_id"))
                    || !item.event().key().clusterId().equals(document.getString("clusterId"))
                    || !item.event().key().pipelineId().equals(document.getString("pipelineId"))
                    || !item.event().key().incarnation().equals(document.getString("incarnation"))) {
                throw unreadable(document, "identity", null);
            }
            return item;
        } catch (RuntimeException invalid) {
            if (invalid instanceof TapstateException) {
                throw invalid;
            }
            throw unreadable(document, "recoveryItem", invalid);
        }
    }

    private static Document event(ClusterRecoveryEvent event) {
        return new Document("key", id(event.key())).append("cause", event.cause().name())
                .append("originalExecutionGeneration", event.originalExecutionGeneration())
                .append("originalExecutionRevision", event.originalExecutionRevision())
                .append("sourceTopologyRevision", event.sourceTopologyRevision())
                .append("sourceProfile", event.sourceProfile() == null ? null : profile(event.sourceProfile()))
                .append("legacySourceProfile", event.legacySourceProfile()).append("targetProfile", profile(event.targetProfile()))
                .append("targetTopologyRevision", event.targetTopologyRevision()).append("intentFingerprint", event.intentFingerprint())
                .append("resumePositions", positions(event.resumePositions()));
    }

    private static ClusterRecoveryEvent event(Document document) {
        Document key = required(document, "key");
        return new ClusterRecoveryEvent(new ClusterRecoveryKey(key.getString("clusterId"), key.getString("pipelineId"),
                key.getString("incarnation")), ClusterRecoveryCause.valueOf(document.getString("cause")),
                number(document, "originalExecutionGeneration"), document.getString("originalExecutionRevision"),
                document.get("sourceTopologyRevision") == null ? null : number(document, "sourceTopologyRevision"),
                document.get("sourceProfile") == null ? null : profile(required(document, "sourceProfile")),
                Boolean.TRUE.equals(document.getBoolean("legacySourceProfile")), profile(required(document, "targetProfile")),
                number(document, "targetTopologyRevision"), document.getString("intentFingerprint"),
                positions(document.getList("resumePositions", Document.class)));
    }

    static Document profile(ClusterExecutionProfile profile) {
        return new Document("clusterId", profile.clusterId()).append("generation", profile.generation())
                .append("formatVersion", profile.profile().formatVersion()).append("attributes", new Document(profile.profile().attributes()))
                .append("hash", profile.profile().hash());
    }

    static ClusterExecutionProfile profile(Document document) {
        Document attributes = required(document, "attributes");
        Map<String, String> fields = new TreeMap<>();
        attributes.forEach((key, value) -> fields.put(key, (String) value));
        ExecutionProfile profile = new ExecutionProfile(integer(document, "formatVersion"), fields);
        if (!profile.hash().equals(document.getString("hash"))) {
            throw unreadable(document, "profile.hash", null);
        }
        return new ClusterExecutionProfile(document.getString("clusterId"), number(document, "generation"), profile);
    }

    static Document permit(ClusterRecoveryPermit permit) {
        return new Document("reservationId", permit.reservationId())
                .append("recoveryClaim", WorkloadClaimDocuments.stored(permit.recoveryClaim()))
                .append("reservedAt", date(permit.reservedAt())).append("deadline", date(permit.deadline()))
                .append("demandByNode", demands(permit.demandByNode()))
                .append("transferredExecutionGeneration", permit.transferredExecutionGeneration());
    }

    private static ClusterRecoveryPermit permit(Document document) {
        return new ClusterRecoveryPermit(document.getString("reservationId"), fence(required(document, "recoveryClaim")),
                instant(document, "reservedAt"), instant(document, "deadline"), demands(document.getList("demandByNode", Document.class)),
                number(document, "transferredExecutionGeneration"));
    }

    private static Document successor(ClusterRecoverySuccessor successor) {
        return new Document("pipelineClaim", WorkloadClaimDocuments.stored(successor.pipelineClaim()))
                .append("profile", profile(successor.profile())).append("executionNodeIds", successor.executionNodeIds().stream().sorted().toList())
                .append("allocatedAt", date(successor.allocatedAt())).append("nativeJobId", successor.nativeJobId())
                .append("submittedAt", date(successor.submittedAt()))
                .append("startupReceipt", successor.startupReceipt() == null ? null : receipt(successor.startupReceipt()));
    }

    private static ClusterRecoverySuccessor successor(Document document) {
        return new ClusterRecoverySuccessor(fence(required(document, "pipelineClaim")), profile(required(document, "profile")),
                Set.copyOf(document.getList("executionNodeIds", String.class)), instant(document, "allocatedAt"),
                document.getString("nativeJobId"), instantOrNull(document, "submittedAt"),
                document.get("startupReceipt") == null ? null : receipt(required(document, "startupReceipt")));
    }

    private static Document receipt(ClusterRecoveryStartupReceipt receipt) {
        return new Document("pipelineClaim", WorkloadClaimDocuments.stored(receipt.pipelineClaim()))
                .append("nativeJobId", receipt.nativeJobId()).append("nativeInitializedAt", date(receipt.nativeInitializedAt()))
                .append("acceptedPositions", positions(receipt.acceptedPositions())).append("positionsAcceptedAt", date(receipt.positionsAcceptedAt()))
                .append("executionCompleted", receipt.executionCompleted());
    }

    private static ClusterRecoveryStartupReceipt receipt(Document document) {
        return new ClusterRecoveryStartupReceipt(fence(required(document, "pipelineClaim")), document.getString("nativeJobId"),
                instant(document, "nativeInitializedAt"), positions(document.getList("acceptedPositions", Document.class)),
                instant(document, "positionsAcceptedAt"), Boolean.TRUE.equals(document.getBoolean("executionCompleted")));
    }

    static Document diagnostic(ClusterRecoveryDiagnostic diagnostic) {
        return new Document("reason", diagnostic.reason().name()).append("code", diagnostic.code())
                .append("params", new Document(diagnostic.params())).append("positions", positions(diagnostic.positions()))
                .append("disposition", diagnostic.disposition());
    }

    private static ClusterRecoveryDiagnostic diagnostic(Document document) {
        return new ClusterRecoveryDiagnostic(ClusterRecoveryDiagnostic.Reason.valueOf(document.getString("reason")),
                document.getString("code"), required(document, "params"), positions(document.getList("positions", Document.class)),
                document.getString("disposition"));
    }

    private static List<Document> positions(Map<String, ClusterRecoveryPosition> positions) {
        List<Document> records = new ArrayList<>();
        new TreeMap<>(positions).forEach((key, value) -> {
            ChainPosition position = value.position();
            records.add(new Document("sourceId", key).append("connectorId", value.connectorId()).append("captureId", value.captureId())
                    .append("kind", value.kind().name()).append("provenance", value.provenance())
                    .append("durableStateReference", value.durableStateReference())
                    .append("position", position == null ? null : new Document("token", position.token()).append("order",
                            position.order() == null ? null : new Document("epoch", position.order().epoch()).append("seq", position.order().seq()))));
        });
        return records;
    }

    private static Map<String, ClusterRecoveryPosition> positions(List<Document> documents) {
        Map<String, ClusterRecoveryPosition> positions = new LinkedHashMap<>();
        for (Document document : documents) {
            Document stored = document.get("position", Document.class);
            Document order = stored == null ? null : stored.get("order", Document.class);
            ChainPosition position = stored == null ? null : new ChainPosition(order == null ? null
                    : new SourceOrder(number(order, "epoch"), number(order, "seq")), stored.getString("token"));
            ClusterRecoveryPosition value = new ClusterRecoveryPosition(document.getString("sourceId"), document.getString("connectorId"),
                    document.getString("captureId"), ClusterRecoveryPosition.Kind.valueOf(document.getString("kind")), position,
                    document.getString("provenance"), document.getString("durableStateReference"));
            if (positions.put(value.sourceId(), value) != null) {
                throw unreadable(document, "duplicateResumePosition", null);
            }
        }
        return Map.copyOf(positions);
    }

    static List<Document> demands(Map<String, ClusterCapacityDemand> demands) {
        return new TreeMap<>(demands).entrySet().stream().map(entry -> new Document("nodeId", entry.getKey())
                .append("processors", entry.getValue().processors()).append("blockingProcessors", entry.getValue().blockingProcessors())
                .append("writers", entry.getValue().writers()).append("connectorInstances", entry.getValue().connectorInstances())
                .append("bufferedRecords", entry.getValue().bufferedRecords()).append("edgeQueueRecords", entry.getValue().edgeQueueRecords())).toList();
    }

    static Map<String, ClusterCapacityDemand> demands(List<Document> documents) {
        Map<String, ClusterCapacityDemand> demands = new LinkedHashMap<>();
        for (Document document : documents) {
            ClusterCapacityDemand demand = new ClusterCapacityDemand(number(document, "processors"), number(document, "blockingProcessors"),
                    number(document, "writers"), number(document, "connectorInstances"), number(document, "bufferedRecords"), number(document, "edgeQueueRecords"));
            if (demands.put(document.getString("nodeId"), demand) != null) {
                throw unreadable(document, "duplicateCapacityNode", null);
            }
        }
        return Map.copyOf(demands);
    }

    static WorkloadClaimFence fence(Document document) {
        return new WorkloadClaimFence(new WorkloadClaimKey(document.getString("clusterId"), WorkloadClaimType.valueOf(document.getString("resourceType")),
                document.getString("resourceId")), new WorkloadOwner(document.getString("ownerNodeId"), document.getString("ownerBootId")),
                number(document, "claimGeneration"), number(document, "executionGeneration"), number(document, "topologyRevision"), number(document, "profileGeneration"));
    }

    static long number(Document document, String field) {
        Number number = document.get(field, Number.class);
        if (number == null) {
            throw unreadable(document, field, null);
        }
        return number.longValue();
    }

    private static int integer(Document document, String field) {
        return Math.toIntExact(number(document, field));
    }

    private static Document required(Document document, String field) {
        Document value = document.get(field, Document.class);
        if (value == null) {
            throw unreadable(document, field, null);
        }
        return value;
    }

    private static Date date(Instant instant) {
        return instant == null ? null : Date.from(instant);
    }

    private static Instant instant(Document document, String field) {
        Instant value = instantOrNull(document, field);
        if (value == null) {
            throw unreadable(document, field, null);
        }
        return value;
    }

    private static Instant instantOrNull(Document document, String field) {
        Date value = document.getDate(field);
        return value == null ? null : value.toInstant();
    }

    static TapstateException unreadable(Document document, String field, Throwable cause) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", String.valueOf(document == null ? "unknown" : document.get("_id")), "field", field), cause);
    }
}
