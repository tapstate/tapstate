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
    private static final String FIELD = "field";
    private static final String REVISION = "revision";
    static final String ALERT_FENCE = "alertWriteFence";
    private static final Set<String> LEGACY_FIELDS = Set.of("_id", REVISION);
    private static final Set<String> ALERT_FIELDS = Set.of("_id", REVISION, ALERT_FENCE);

    private ConfigurationVersion() {}

    static long read(ClientSession session, MongoCollection<Document> versions) {
        return revision(versions.find(session, new Document("_id", ID)).first());
    }

    static long revision(Document current) {
        if (current == null || !(current.get(REVISION) instanceof Long revision)
                || revision < 0 || revision == Long.MAX_VALUE) {
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", ID, FIELD, REVISION), null);
        }
        return revision;
    }

    static void advance(ClientSession session, MongoCollection<Document> versions, com.mongodb.client.MongoClient originalClient) {
        advance(session, versions, originalClient.getDatabase(versions.getNamespace().getDatabaseName()));
    }

    static void advance(ClientSession session, MongoCollection<Document> versions, com.mongodb.client.MongoDatabase originalDatabase) {
        long revision = requireNoValidationMaintenance(session, versions, null, originalDatabase);
        advanceRaw(session, versions, revision);
    }

    /** Claims the ordering document before publication reads its dependency snapshot. */
    static void advanceForPublication(ClientSession session, MongoCollection<Document> versions,
            com.mongodb.client.MongoClient originalClient) {
        if (!session.hasActiveTransaction()) throw new IllegalStateException("publication requires an active transaction");
        if (reserveRevision(session, versions, Long.MAX_VALUE - 1) == null) throw unreadable(REVISION);
        requireWritable(session, versions, originalClient);
    }

    /** The update and all later validation share one transaction and roll back together. */
    static Document reserveRevision(ClientSession session, MongoCollection<Document> versions, long exclusiveLimit) {
        var legalRevision = new Document("$gte", 0L).append("$lt", exclusiveLimit);
        var filter = new Document("_id", ID).append(REVISION, legalRevision)
                .append("$expr", new Document("$eq", java.util.List.of(new Document("$type", "$revision"), "long")));
        return versions.findOneAndUpdate(session, filter, new Document("$inc", new Document(REVISION, 1L)));
    }

    static void requireWritable(ClientSession session, MongoCollection<Document> versions,
            com.mongodb.client.MongoClient originalClient) {
        requireNoValidationMaintenance(session, versions, null,
                originalClient.getDatabase(versions.getNamespace().getDatabaseName()));
    }

    static void advanceValidationPreparation(ClientSession session, MongoCollection<Document> versions, String operation,
            com.mongodb.client.MongoClient originalClient) {
        long revision = requireNoValidationMaintenance(session, versions, java.util.Objects.requireNonNull(operation),
                originalClient.getDatabase(versions.getNamespace().getDatabaseName()));
        advanceRaw(session, versions, revision);
    }

    private static long requireNoValidationMaintenance(ClientSession session, MongoCollection<Document> versions, String operation,
            com.mongodb.client.MongoDatabase originalDatabase) {
        // This read uses the original collection's database and the SAME session. No second client
        // or guessed endpoint can certify the migration gate that protects this revision write.
        var row = versions.aggregate(session, java.util.List.of(new Document("$match", new Document("_id", ID)),
                new Document("$lookup", new Document("from", SystemCollections.SCHEMA_MIGRATIONS.collectionName())
                        .append("pipeline", java.util.List.of(new Document("$match", new Document("_id", "config")),
                                new Document("$project", new Document("installedVersion", 1).append("componentVersion", 1)
                                        .append("firstClearingStop", 1).append("validation", 1))))
                        .append("as", "schema")),
                new Document("$lookup", new Document("from", SystemCollections.CONFIG_CLUSTER_IDENTITY.collectionName())
                        .append("pipeline", java.util.List.of(new Document("$match", new Document("_id", "cluster")),
                                new Document("$project", new Document("clusterId", 1).append("layoutId", 1))))
                        .append("as", "identity")))).first();
        return requireNoValidationMaintenance(row, operation, versions, originalDatabase);
    }

    static void advanceStart(ClientSession session, MongoCollection<Document> versions,
            com.mongodb.client.MongoDatabase database, Document version, Document schema, Document identity) {
        long revision = revision(version);
        var captured = new Document(REVISION, revision)
                .append("schema", schema == null ? java.util.List.of() : java.util.List.of(schema))
                .append("identity", java.util.List.of(identity));
        advanceRaw(session, versions, requireNoValidationMaintenance(captured, null, versions, database));
    }

    private static long requireNoValidationMaintenance(Document row, String operation,
            MongoCollection<Document> versions, com.mongodb.client.MongoDatabase originalDatabase) {
        long revision = revision(row);
        var schemas = row.getList("schema", Document.class);
        if (schemas.isEmpty()) { if (operation != null) throw MongoValidationSchema.refused(); return revision; }
        var schema = schemas.getFirst();
        // Metadata probes use the SAME original client and namespace, never an independently opened endpoint.
        if (!versions.getNamespace().getDatabaseName().equals(originalDatabase.getName()))
            throw new IllegalArgumentException("configuration version database mismatch");
        // Ordinary writes contend with maintenance through the live marker and the revision CAS.
        // Controlled preparation still verifies the physical schema.
        boolean hasValidation = operation != null
                ? MongoValidationSchema.configurationHasValidation(originalDatabase, schema)
                : MongoValidationSchema.configurationDeclaresValidation(schema);
        if (hasValidation) {
            var marker = MongoValidationSchema.configurationMarker(schema);
            var identities = row.getList("identity", Document.class);
            if (identities.size() != 1 || !MongoValidationSchema.scope(marker).clusterId().equals(identities.getFirst().get("clusterId"))
                    || !MongoConfigurationDomain.LAYOUT.equals(identities.getFirst().get("layoutId"))) throw MongoValidationSchema.unreadable("identity");
            if (operation != null) {
                if (!operation.equals(marker.get("operationId")) || !"PREPARING".equals(marker.get("phase"))) throw MongoValidationSchema.refused();
            } else if (!"READY".equals(marker.get("phase")) || Integer.valueOf(2).equals(marker.get("formatVersion"))
                    && !"READY".equals(marker.get("scheduledAdmission", Document.class).get("phase"))) throw MongoValidationSchema.refused();
        } else if (operation != null) throw MongoValidationSchema.refused();
        return revision;
    }

    static void advanceScheduledPreparation(ClientSession session, com.mongodb.client.MongoDatabase database, String operation) {
        var row = SystemCollections.SCHEMA_MIGRATIONS.on(database).find(session, new Document("_id", "config")).first();
        var marker = MongoValidationSchema.requireConfiguration(row, "READY");
        var upgrade = marker.get("scheduledAdmission", Document.class);
        if (!Integer.valueOf(2).equals(marker.get("formatVersion")) || upgrade == null
                || !operation.equals(upgrade.get("operationId")) || !"PREPARING".equals(upgrade.get("phase"))) throw MongoValidationSchema.refused();
        advanceRaw(session, SystemCollections.CONFIGURATION_VERSION.on(database));
    }

    private static void advanceRaw(ClientSession session, MongoCollection<Document> versions) {
        advanceRaw(session, versions, read(session, versions));
    }

    private static void advanceRaw(ClientSession session, MongoCollection<Document> versions, long revision) {
        if (revision >= Long.MAX_VALUE - 1) throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                Map.of("id", ID, FIELD, REVISION), null);
        long modified = versions.updateOne(session, new Document("_id", ID).append(REVISION, revision),
                new Document("$inc", new Document(REVISION, 1L))).getModifiedCount();
        if (modified != 1) throw new IllegalStateException("revision changed within one transaction snapshot");
    }

    /** Pure alerts contend with every authoring/token/user mutation of this same document. */
    static long advanceAlert(ClientSession session, MongoCollection<Document> versions) {
        return advanceAlert(session, versions, alertWritePreimage(session, versions));
    }

    static Document alertWritePreimage(ClientSession session, MongoCollection<Document> versions) {
        MongoAlertStateStore.requireTransaction(session);
        Document current = versions.find(session, new Document("_id", ID)).first();
        requireAlertWriteHeadroom(current);
        return current;
    }

    /** The preimage belongs to this same callback; the actual write still arbitrates concurrent changes. */
    static long advanceAlert(ClientSession session, MongoCollection<Document> versions, Document current) {
        MongoAlertStateStore.requireTransaction(session);
        requireAlertWriteHeadroom(current);
        long fence = current.getLong(ALERT_FENCE);
        if (versions.updateOne(session, new Document("_id", ID).append(REVISION, current.get(REVISION))
                        .append(ALERT_FENCE, fence), new Document("$inc", new Document(ALERT_FENCE, 1L)))
                .getModifiedCount() != 1) {
            throw new IllegalStateException("alert fence changed within one transaction snapshot");
        }
        return fence + 1;
    }

    private static void requireAlertWriteHeadroom(Document current) {
        validateAlertState(current, true);
        if (current.getLong(ALERT_FENCE) >= Long.MAX_VALUE - 1) throw unreadable(ALERT_FENCE);
    }

    /** Only an explicit, same-session component preparation may enroll an absent counter. */
    static void prepareAlertFence(ClientSession session, MongoCollection<Document> versions) {
        MongoAlertStateStore.requireTransaction(session);
        Document current = versions.find(session, new Document("_id", ID)).first();
        validateAlertState(current, false);
        if (versions.updateOne(session, new Document("_id", ID).append(REVISION, current.get(REVISION))
                        .append(ALERT_FENCE, new Document("$exists", false)),
                        new Document("$set", new Document(ALERT_FENCE, 0L))).getModifiedCount() != 1) {
            throw unreadable(ALERT_FENCE);
        }
    }

    static void validateAlertState(Document current, boolean prepared) {
        if (current == null || !current.keySet().equals(prepared ? ALERT_FIELDS : LEGACY_FIELDS)
                || !ID.equals(current.get("_id")) || !(current.get(REVISION) instanceof Long revision)
                || revision < 0 || revision == Long.MAX_VALUE) { throw unreadable(REVISION); }
        if (prepared && (!(current.get(ALERT_FENCE) instanceof Long fence) || fence < 0 || fence == Long.MAX_VALUE)) {
            throw unreadable(ALERT_FENCE);
        }
        // Closed keys and fixed BSON types keep this singleton below its 256-byte bound.
    }

    static Document initial() {
        return new Document("_id", ID).append(REVISION, 0L).append(ALERT_FENCE, 0L);
    }

    private static TapstateException unreadable(String field) {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", ID, FIELD, field), null);
    }
}
