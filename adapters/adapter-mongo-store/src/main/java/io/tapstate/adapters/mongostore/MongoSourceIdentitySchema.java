package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import org.bson.Document;

/** Fixed Source-identity preparation state inside the original CONFIG schema aggregate. */
final class MongoSourceIdentitySchema {
    static final String FIELD = "sourceIdentity";
    static final int VERSION = 1;
    static final int MAX_CONTROL_BYTES = 2 * 1024;
    enum Phase { PREPARING, READY }
    record State(Phase phase, String operationId, String clusterId, String keyId, String lastSourceIncarnation, long preparedCount) { }
    private static final Set<String> FIELDS = Set.of("installedVersion", "phase", "operationId", "clusterId",
            "layoutId", "keyId", "lastSourceIncarnation", "preparedCount");

    private MongoSourceIdentitySchema() { }

    static Document initial(String clusterId, String keyId, String operationId) {
        return encode(new State(Phase.READY, operationId, clusterId, keyId, null, 0));
    }

    static Document encode(State state) {
        if (!text(state.clusterId(), 256) || !text(state.keyId(), 128) || !text(state.operationId(), 256)
                || state.lastSourceIncarnation() != null && !incarnation(state.lastSourceIncarnation())
                || state.preparedCount() < 0 || (state.preparedCount() == 0) != (state.lastSourceIncarnation() == null)) {
            throw new IllegalArgumentException("Source identity preparation state is not bounded or consistent");
        }
        return new Document("installedVersion", VERSION).append("phase", state.phase().name())
                .append("operationId", state.operationId()).append("clusterId", state.clusterId())
                .append("layoutId", MongoConfigurationDomain.LAYOUT).append("keyId", state.keyId())
                .append("lastSourceIncarnation", state.lastSourceIncarnation()).append("preparedCount", state.preparedCount());
    }

    /** Null is a valid legacy state, never a usable identity capability. */
    static State read(ClientSession session, MongoDatabase database, String expectedKeyId) {
        Document identity = SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database)
                .find(session, new Document("_id", "cluster")).first();
        Document schema = SystemCollections.SCHEMA_MIGRATIONS.on(database)
                .find(session, new Document("_id", "config")).first();
        if (identity == null || !(identity.get("clusterId") instanceof String clusterId)
                || !text(clusterId, 256) || !MongoConfigurationDomain.LAYOUT.equals(identity.get("layoutId"))
                || schema == null || !(schema.get("installedVersion") instanceof Integer installed)
                || installed < 1 || installed > MongoConfigurationDomain.SCHEMA_VERSION) { throw unreadable("identity"); }
        if (!schema.containsKey(FIELD)) { return null; }
        if (!(schema.get(FIELD) instanceof Document row)) { throw unreadable("marker"); }
        return decode(row, clusterId, expectedKeyId);
    }

    static State decode(Document row, String clusterId, String keyId) {
        if (!row.keySet().equals(FIELDS) || !Integer.valueOf(VERSION).equals(row.get("installedVersion"))
                || !clusterId.equals(row.get("clusterId")) || !keyId.equals(row.get("keyId"))
                || !MongoConfigurationDomain.LAYOUT.equals(row.get("layoutId"))
                || !(row.get("operationId") instanceof String operation) || !text(operation, 256)
                || !(row.get("preparedCount") instanceof Long count) || count < 0
                || row.get("lastSourceIncarnation") != null && !(row.get("lastSourceIncarnation") instanceof String)) {
            throw unreadable("marker");
        }
        String cursor = (String) row.get("lastSourceIncarnation");
        if (cursor != null && !incarnation(cursor) || (count == 0) != (cursor == null)) { throw unreadable("cursor"); }
        Phase phase = switch (String.valueOf(row.get("phase"))) {
            case "PREPARING" -> Phase.PREPARING;
            case "READY" -> Phase.READY;
            default -> throw unreadable("phase");
        };
        return new State(phase, operation, clusterId, keyId, cursor, count);
    }

    static boolean text(String value, int bytes) {
        return value != null && !value.isBlank() && value.length() <= bytes * 2
                && value.codePoints().noneMatch(Character::isISOControl)
                && StandardCharsets.UTF_8.newEncoder().canEncode(value)
                && value.getBytes(StandardCharsets.UTF_8).length <= bytes;
    }
    static boolean incarnation(String value) {
        try {
            var parsed = java.util.UUID.fromString(value);
            return value.length() == 36 && parsed.toString().equals(value) && parsed.version() == 4 && parsed.variant() == 2;
        } catch (IllegalArgumentException | NullPointerException invalid) { return false; }
    }
    static TapstateException unavailable(String id) {
        return new TapstateException(IoError.SOURCE_IDENTITY_UNAVAILABLE, Map.of("id", id), null);
    }
    static TapstateException unreadable(String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", "source-identity", "field", field), null);
    }
}
