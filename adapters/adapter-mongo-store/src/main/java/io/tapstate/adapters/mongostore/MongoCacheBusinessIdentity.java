package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import org.bson.Document;
import java.util.Map;
import java.util.List;

/** Business readiness is independent from authentication-only generation changes. */
final class MongoCacheBusinessIdentity {
    private final MongoDatabase database;
    private final CacheBusinessScope scope;
    MongoCacheBusinessIdentity(MongoDatabase database, CacheBusinessScope scope) { this.database = database; this.scope = scope; }
    void requireReady(ClientSession session) { readyIdentity(session); }

    private Document readyIdentity(ClientSession session) {
        Document identity = SystemCollections.CACHE_IDENTITY.on(database).find(session, new Document("_id", "cluster")).first();
        if (identity == null || !scope.clusterId().equals(identity.get("clusterId")) || !Long.valueOf(scope.bindingVersion()).equals(identity.get("bindingVersion"))
                || !scope.cacheIncarnation().equals(identity.get("cacheIncarnation")) || !MongoConfigurationDomain.LAYOUT.equals(identity.get("layoutId"))
                || !MongoCacheIdentity.READY.equals(identity.get("businessPhase"))) throw businessFenced();
        Document schema = SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).find(session, new Document("_id", "business")).first();
        if (schema == null || !(schema.get("installedVersion") instanceof Integer version)) throw unreadable();
        if (version > 1) throw new TapstateException(MigrationError.DATA_NEWER_THAN_BINARY, Map.of("installed", String.valueOf(version), "supported", "1"), null);
        if (version != 1 || !MongoCacheIdentity.READY.equals(schema.get("phase"))) throw businessFenced();
        if (!(identity.get("businessPreparationId") instanceof String operation) || operation.isBlank()
                || !operation.equals(schema.get("operationId")) || !(schema.get("epoch") instanceof Long epoch) || epoch < 1) throw unreadable();
        return identity;
    }

    /** Claim admission is infrequent; protected event writes continue to contend on their own claim. */
    void admitClaim(ClientSession session) {
        Document row = readyIdentity(session);
        long revision = admissionRevision(row);
        if (revision == Long.MAX_VALUE - 1) throw businessFenced();
        if (SystemCollections.CACHE_IDENTITY.on(database).updateOne(session,
                identityFilter().append("businessPhase", MongoCacheIdentity.READY).append("claimAdmissionFence", revision),
                new Document("$inc", new Document("claimAdmissionFence", 1L))).getMatchedCount() != 1) {
            throw new IllegalStateException("cache claim admission changed within its transaction snapshot");
        }
    }

    void requireAdmissionState(ClientSession session) { admissionRevision(readyIdentity(session)); }

    /** Publish retirement and expire the original domain's claims in this same cache transaction. */
    void retire(ClientSession session, String operationId) {
        Document row = SystemCollections.CACHE_IDENTITY.on(database).find(session, identityFilter()).first();
        if (row == null || !MongoConfigurationDomain.LAYOUT.equals(row.get("layoutId"))) throw businessFenced();
        if ("RETIRED".equals(row.get("businessPhase"))) {
            if (!operationId.equals(row.get("businessRetirementId"))) throw businessFenced();
            admissionRevision(row);
            return;
        }
        row = readyIdentity(session);
        long revision = admissionRevision(row);
        if (SystemCollections.CACHE_IDENTITY.on(database).updateOne(session,
                identityFilter().append("businessPhase", MongoCacheIdentity.READY).append("claimAdmissionFence", revision),
                new Document("$set", new Document("businessPhase", "RETIRED").append("businessRetirementId", operationId)))
                .getMatchedCount() != 1) {
            throw new IllegalStateException("cache retirement changed within its transaction snapshot");
        }
        // Address the canonical key, including claims whose duplicated payload fields are damaged.
        SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateMany(session,
                new Document("_id.clusterId", scope.clusterId()),
                List.of(new Document("$set", new Document("leaseUntil", "$$NOW"))));
    }

    private Document identityFilter() {
        return new Document("_id", "cluster").append("clusterId", scope.clusterId())
                .append("bindingVersion", scope.bindingVersion()).append("cacheIncarnation", scope.cacheIncarnation());
    }
    private static long admissionRevision(Document row) {
        if (!(row.get("claimAdmissionFence") instanceof Long revision) || revision < 0 || revision == Long.MAX_VALUE) throw unreadable();
        return revision;
    }
    static TapstateException businessFenced() { return new TapstateException(IoError.CACHE_BUSINESS_FENCED, Map.of(), null); }
    private static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "cache-business", "field", "identity"), null); }
}
