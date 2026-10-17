package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import org.bson.Document;
import java.util.Map;

/** Business readiness is independent from authentication-only generation changes. */
final class MongoCacheBusinessIdentity {
    private final MongoDatabase database;
    private final CacheBusinessScope scope;
    MongoCacheBusinessIdentity(MongoDatabase database, CacheBusinessScope scope) { this.database = database; this.scope = scope; }
    void requireReady(ClientSession session) {
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
    }
    private static TapstateException businessFenced() { return new TapstateException(IoError.CACHE_BUSINESS_FENCED, Map.of(), null); }
    private static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "cache-business", "field", "identity"), null); }
}
