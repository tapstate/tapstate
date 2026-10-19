package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import org.bson.Document;

import java.util.Map;

/** The single write-contention point for configuration mutations that affect import preflight. */
final class ConfigurationVersion {
    static final String ID = "configuration";

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
        long matched = versions.updateOne(session, new Document("_id", ID).append("revision", revision),
                new Document("$inc", new Document("revision", 1L))).getMatchedCount();
        if (matched != 1) throw new IllegalStateException("revision changed within one transaction snapshot");
    }
}
