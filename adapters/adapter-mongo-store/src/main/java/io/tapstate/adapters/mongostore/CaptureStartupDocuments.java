package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.model.ReadMode;
import io.tapstate.spi.store.CaptureReadAttempt;
import io.tapstate.spi.store.CaptureReadState;
import io.tapstate.spi.store.CaptureResumeWitness;
import io.tapstate.spi.store.ClusterRecoveryPosition;
import io.tapstate.spi.store.ConsumerProgressKind;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.bson.Document;
import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The bounded persisted source-start proof shapes; no identity or source position is generated on read. */
final class CaptureStartupDocuments {
    private CaptureStartupDocuments() {}

    static Document witness(CaptureResumeWitness witness) {
        Document tables = new Document();
        new java.util.TreeMap<>(witness.sinkAckedByTable()).forEach((table, value) -> tables.put(table, position(value)));
        return new Document("sourceId", witness.sourceId()).append("connectorId", witness.connectorId())
                .append("miningChainId", witness.miningChainId()).append("consumerId", witness.consumerId())
                .append("readMode", witness.readMode().name()).append("srsEnabled", witness.srsEnabled())
                .append("tables", witness.tables()).append("chainPresent", witness.chainPresent())
                .append("chainEpoch", witness.chainEpoch()).append("sourceRead", position(witness.sourceRead()))
                .append("sourceReadDurable", witness.sourceReadDurable()).append("consumerPresent", witness.consumerPresent())
                .append("snapshotCompletedTables", witness.snapshotCompletedTables())
                .append("cdcStartPosition", witness.cdcStartPosition()).append("snapshotEpoch", witness.snapshotEpoch())
                .append("progressKind", witness.progressKind() == null ? null : witness.progressKind().name())
                .append("sinkAcked", position(witness.sinkAcked())).append("sinkAckedByTable", tables)
                .append("priorReader", reader(witness.priorReader()));
    }

    static CaptureResumeWitness witness(Document stored) {
        try {
            Map<String, ChainPosition> tables = new LinkedHashMap<>();
            stored.get("sinkAckedByTable", Document.class).forEach((table, value) -> tables.put(table, position((Document) value)));
            String kind = stored.getString("progressKind");
            return new CaptureResumeWitness(stored.getString("sourceId"), stored.getString("connectorId"),
                    stored.getString("miningChainId"), stored.getString("consumerId"), ReadMode.valueOf(stored.getString("readMode")),
                    stored.getBoolean("srsEnabled"), stored.getList("tables", String.class), stored.getBoolean("chainPresent"),
                    number(stored, "chainEpoch"), position(stored.get("sourceRead", Document.class)),
                    stored.getBoolean("sourceReadDurable"), stored.getBoolean("consumerPresent"),
                    stored.getList("snapshotCompletedTables", String.class), stored.getString("cdcStartPosition"),
                    number(stored, "snapshotEpoch"), kind == null ? null : ConsumerProgressKind.valueOf(kind),
                    position(stored.get("sinkAcked", Document.class)), tables, reader(stored.get("priorReader", Document.class)));
        } catch (RuntimeException invalid) {
            throw unreadable("witness", invalid);
        }
    }

    static Document position(ChainPosition point) {
        return point == null ? null : new Document("token", point.token())
                .append("epoch", point.order() == null ? null : point.order().epoch())
                .append("seq", point.order() == null ? null : point.order().seq());
    }

    static ChainPosition position(Document stored) {
        if (stored == null) { return null; }
        SourceOrder order = stored.get("epoch") == null ? null
                : new SourceOrder(number(stored, "epoch"), number(stored, "seq"));
        return new ChainPosition(order, stored.getString("token"));
    }

    static Document requested(ClusterRecoveryPosition point) {
        return point == null ? null : new Document("sourceId", point.sourceId()).append("connectorId", point.connectorId())
                .append("captureId", point.captureId()).append("kind", point.kind().name())
                .append("position", position(point.position())).append("provenance", point.provenance())
                .append("durableStateReference", point.durableStateReference());
    }

    static ClusterRecoveryPosition requested(Document point) {
        if (point == null) { return null; }
        try {
            return new ClusterRecoveryPosition(point.getString("sourceId"), point.getString("connectorId"),
                    point.getString("captureId"), ClusterRecoveryPosition.Kind.valueOf(point.getString("kind")),
                    position(point.get("position", Document.class)), point.getString("provenance"), point.getString("durableStateReference"));
        } catch (RuntimeException invalid) { throw unreadable("requestedPosition", invalid); }
    }

    static WorkloadClaimFence fence(Document stored) {
        return new WorkloadClaimFence(new WorkloadClaimKey(stored.getString("clusterId"),
                WorkloadClaimType.valueOf(stored.getString("resourceType")), stored.getString("resourceId")),
                new WorkloadOwner(stored.getString("ownerNodeId"), stored.getString("ownerBootId")),
                number(stored, "claimGeneration"), number(stored, "executionGeneration"), number(stored, "topologyRevision"),
                stored.get("profileGeneration") == null ? 0 : number(stored, "profileGeneration"));
    }

    static Document reader(CaptureReadState state) {
        if (state == null) { return null; }
        CaptureReadAttempt attempt = state.attempt();
        return new Document("schemaVersion", 1).append("miningChainId", attempt.miningChainId())
                .append("chainEpoch", attempt.chainEpoch()).append("version", attempt.version())
                .append("captureClaim", WorkloadClaimDocuments.stored(attempt.captureClaim())).append("tables", attempt.tables())
                .append("requestedKind", attempt.requestedKind().name()).append("requestedToken", attempt.requestedToken())
                .append("requestedInstant", attempt.requestedInstant() == null ? null : Date.from(attempt.requestedInstant()))
                .append("allocatedAt", Date.from(attempt.allocatedAt())).append("resolvedAnchor", state.resolvedAnchor())
                .append("anchorResolvedAt", state.anchorResolvedAt() == null ? null : Date.from(state.anchorResolvedAt()))
                .append("firstDeliveredAt", state.firstDeliveredAt() == null ? null : Date.from(state.firstDeliveredAt()))
                .append("failed", state.failed()).append("failureCode", state.failureCode())
                .append("failureParams", namedParams(state.failureParams())).append("disposition", state.disposition())
                .append("failedAt", state.failedAt() == null ? null : Date.from(state.failedAt()));
    }

    static CaptureReadState reader(Document stored) {
        if (stored == null) { return null; }
        try {
            if (number(stored, "schemaVersion") != 1 || stored.getBoolean("failed") == null) {
                throw unreadable("captureReadAttempt", null);
            }
            CaptureReadAttempt attempt = new CaptureReadAttempt(stored.getString("miningChainId"),
                    number(stored, "chainEpoch"), number(stored, "version"), fence(stored.get("captureClaim", Document.class)),
                    stored.getList("tables", String.class), CaptureReadAttempt.Kind.valueOf(stored.getString("requestedKind")),
                    stored.getString("requestedToken"), instant(stored, "requestedInstant"), instant(stored, "allocatedAt"));
            return new CaptureReadState(attempt, stored.getString("resolvedAnchor"), instant(stored, "anchorResolvedAt"),
                    instant(stored, "firstDeliveredAt"), Boolean.TRUE.equals(stored.getBoolean("failed")), stored.getString("failureCode"),
                    stored.get("failureParams") == null ? Map.of() : new LinkedHashMap<>(stored.get("failureParams", Document.class)),
                    stored.getString("disposition"), instant(stored, "failedAt"));
        } catch (RuntimeException invalid) {
            throw unreadable("captureReadAttempt", invalid);
        }
    }

    static Document namedParams(Map<String, Object> params) {
        Document result = new Document();
        new java.util.TreeMap<>(params).forEach((name, value) -> result.put(name, canonicalValue(value)));
        return result;
    }

    private static Object canonicalValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            Document result = new Document();
            java.util.TreeMap<String, Object> sorted = new java.util.TreeMap<>();
            map.forEach((name, item) -> sorted.put(String.valueOf(name), item));
            sorted.forEach((name, item) -> result.put(name, canonicalValue(item)));
            return result;
        }
        if (value instanceof List<?> list) { return list.stream().map(CaptureStartupDocuments::canonicalValue).toList(); }
        return value;
    }

    static Instant instant(Document stored, String key) {
        Date value = stored.getDate(key);
        return value == null ? null : value.toInstant();
    }

    static long number(Document stored, String key) {
        Object value = stored.get(key);
        if (!(value instanceof Number number)) { throw unreadable(key, null); }
        return number.longValue();
    }

    static Document prepared(Document consumer) {
        if (consumer == null || consumer.get("preparedSourceStart") == null) { return null; }
        try {
            Document prepared = consumer.get("preparedSourceStart", Document.class);
            if (number(prepared, "schemaVersion") != 1 || instant(prepared, "preparedAt") == null) {
                throw unreadable("preparedSourceStart", null);
            }
            fence(prepared.get("pipelineClaim", Document.class));
            witness(prepared.get("witness", Document.class));
            requiredSources(prepared);
            requested(prepared.get("requestedPosition", Document.class));
            return prepared;
        } catch (RuntimeException invalid) { throw unreadable("preparedSourceStart", invalid); }
    }

    static java.util.Set<String> requiredSources(Document prepared) {
        try {
            List<String> ids = prepared.getList("requiredSourceIds", String.class);
            if (ids == null || ids.isEmpty() || ids.stream().anyMatch(id -> id == null || id.isBlank())
                    || java.util.Set.copyOf(ids).size() != ids.size()) { throw unreadable("requiredSourceIds", null); }
            return java.util.Set.copyOf(ids);
        } catch (RuntimeException invalid) { throw unreadable("requiredSourceIds", invalid); }
    }

    private static TapstateException unreadable(String field, Throwable cause) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "capture-startup", "field", field), cause);
    }
}
