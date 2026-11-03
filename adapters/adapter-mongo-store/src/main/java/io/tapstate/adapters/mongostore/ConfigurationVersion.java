package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import org.bson.Document;

import java.util.Map;
import java.util.Set;

/** One actual document orders authoring changes and alert transactions without invalidating import preflight. */
final class ConfigurationVersion {
    static final String ID = "configuration";
    static final String ALERT_FENCE = "alertWriteFence";
    private static final Set<String> LEGACY_FIELDS = Set.of("_id", "revision");
    private static final Set<String> ALERT_FIELDS = Set.of("_id", "revision", ALERT_FENCE);

    private ConfigurationVersion() {}

    static long read(ClientSession session, MongoCollection<Document> versions) {
        Document current = versions.find(session, new Document("_id", ID)).first();
        if (current == null || !(current.get("revision") instanceof Long revision)
                || revision < 0 || revision == Long.MAX_VALUE) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", ID, "field", "revision"), null);
        }
        return revision;
    }

    static void advance(ClientSession session, MongoCollection<Document> versions) {
        long revision = read(session, versions);
        if (revision >= Long.MAX_VALUE - 1) throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", ID, "field", "revision"), null);
        long modified = versions.updateOne(session, new Document("_id", ID).append("revision", revision),
                new Document("$inc", new Document("revision", 1L))).getModifiedCount();
        if (modified != 1) throw new IllegalStateException("revision changed within one transaction snapshot");
    }

    /** Pure alerts contend with every authoring/token/user mutation of this same document. */
    static long advanceAlert(ClientSession session, MongoCollection<Document> versions) {
        MongoAlertStateStore.requireTransaction(session);
        Document current = versions.find(session, new Document("_id", ID)).first();
        validateAlertState(current, true);
        long fence = (Long) current.get(ALERT_FENCE);
        if (fence >= Long.MAX_VALUE - 1) { throw unreadable(ALERT_FENCE); }
        if (versions.updateOne(session, new Document("_id", ID).append("revision", current.get("revision"))
                        .append(ALERT_FENCE, fence), new Document("$inc", new Document(ALERT_FENCE, 1L)))
                .getModifiedCount() != 1) {
            throw new IllegalStateException("alert fence changed within one transaction snapshot");
        }
        return fence + 1;
    }

    /** Only an explicit, same-session component preparation may enroll an absent counter. */
    static void prepareAlertFence(ClientSession session, MongoCollection<Document> versions) {
        MongoAlertStateStore.requireTransaction(session);
        Document current = versions.find(session, new Document("_id", ID)).first();
        validateAlertState(current, false);
        if (versions.updateOne(session, new Document("_id", ID).append("revision", current.get("revision"))
                        .append(ALERT_FENCE, new Document("$exists", false)),
                        new Document("$set", new Document(ALERT_FENCE, 0L))).getModifiedCount() != 1) {
            throw unreadable(ALERT_FENCE);
        }
    }

    static void validateAlertState(Document current, boolean prepared) {
        if (current == null || !current.keySet().equals(prepared ? ALERT_FIELDS : LEGACY_FIELDS)
                || !ID.equals(current.get("_id")) || !(current.get("revision") instanceof Long revision)
                || revision < 0 || revision == Long.MAX_VALUE) { throw unreadable("revision"); }
        if (prepared && (!(current.get(ALERT_FENCE) instanceof Long fence) || fence < 0 || fence == Long.MAX_VALUE)) {
            throw unreadable(ALERT_FENCE);
        }
        // Closed keys and fixed BSON types keep this singleton below its 256-byte bound.
    }

    static Document initial() {
        return new Document("_id", ID).append("revision", 0L).append(ALERT_FENCE, 0L);
    }

    private static TapstateException unreadable(String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", ID, "field", field), null);
    }
}
