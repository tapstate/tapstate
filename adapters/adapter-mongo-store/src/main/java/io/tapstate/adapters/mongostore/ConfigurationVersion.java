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
        requireNoValidationMaintenance(session, versions, null);
        advanceRaw(session, versions);
    }

    static void advanceValidationPreparation(ClientSession session, MongoCollection<Document> versions, String operation) {
        requireNoValidationMaintenance(session, versions, java.util.Objects.requireNonNull(operation));
        advanceRaw(session, versions);
    }

    private static void requireNoValidationMaintenance(ClientSession session, MongoCollection<Document> versions, String operation) {
        // This read uses the original collection's database and the SAME session. No second client
        // or guessed endpoint can certify the migration gate that protects this revision write.
        var row = versions.aggregate(session, java.util.List.of(new Document("$match", new Document("_id", ID)),
                new Document("$lookup", new Document("from", SystemCollections.SCHEMA_MIGRATIONS.collectionName())
                        .append("pipeline", java.util.List.of(new Document("$match", new Document("_id", "config")),
                                new Document("$project", new Document("installedVersion", 1).append("validation", 1))))
                        .append("as", "schema")),
                new Document("$lookup", new Document("from", SystemCollections.CONFIG_CLUSTER_IDENTITY.collectionName())
                        .append("pipeline", java.util.List.of(new Document("$match", new Document("_id", "cluster")),
                                new Document("$project", new Document("clusterId", 1).append("layoutId", 1))))
                        .append("as", "identity")))).first();
        if (row == null) throw MongoValidationSchema.unreadable("configuration-version");
        var schemas = row.getList("schema", Document.class);
        if (schemas.isEmpty()) { if (operation != null) throw MongoValidationSchema.refused(); return; }
        var schema = schemas.getFirst();
        if (schema.containsKey("validation") || Integer.valueOf(MongoValidationSchema.VERSION).equals(schema.get("installedVersion"))) {
            if (!Integer.valueOf(MongoValidationSchema.VERSION).equals(schema.get("installedVersion"))) throw MongoValidationSchema.unreadable("version");
            if (!(schema.get("validation") instanceof Document value)) throw MongoValidationSchema.unreadable("marker");
            var marker = MongoValidationSchema.validateMarker(value);
            var identities = row.getList("identity", Document.class);
            if (identities.size() != 1 || !MongoValidationSchema.scope(marker).clusterId().equals(identities.getFirst().get("clusterId"))
                    || !MongoConfigurationDomain.LAYOUT.equals(identities.getFirst().get("layoutId"))) throw MongoValidationSchema.unreadable("identity");
            if (operation != null) {
                if (!operation.equals(marker.get("operationId")) || !"PREPARING".equals(marker.get("phase"))) throw MongoValidationSchema.refused();
            } else if (!"READY".equals(marker.get("phase"))) throw MongoValidationSchema.refused();
        } else if (operation != null) throw MongoValidationSchema.refused();
    }

    private static void advanceRaw(ClientSession session, MongoCollection<Document> versions) {
        long revision = read(session, versions);
        if (revision >= Long.MAX_VALUE - 1) throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", ID, "field", "revision"), null);
        long matched = versions.updateOne(session, new Document("_id", ID).append("revision", revision),
                new Document("$inc", new Document("revision", 1L))).getMatchedCount();
        if (matched != 1) throw new IllegalStateException("revision changed within one transaction snapshot");
    }
}
