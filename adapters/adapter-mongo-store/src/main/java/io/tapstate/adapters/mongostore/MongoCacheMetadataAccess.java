package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import org.bson.Document;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/** One metadata facet's original CACHE client and business scope; mutations contend with retirement. */
final class MongoCacheMetadataAccess {
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private static final String SCOPE = "cacheScope";
    private final MongoClient client;
    private final MongoDatabase database;
    private final CacheBusinessScope scope;
    private final SystemCollections table;
    private final Runnable requireActive;
    final MongoCollection<Document> collection;

    MongoCacheMetadataAccess(MongoClient client, MongoDatabase database, CacheBusinessScope scope, SystemCollections table, Runnable requireActive) {
        this.client = Objects.requireNonNull(client, "client"); this.database = Objects.requireNonNull(database, "database");
        this.scope = Objects.requireNonNull(scope, "scope"); this.table = Objects.requireNonNull(table, "table");
        this.requireActive = Objects.requireNonNull(requireActive, "requireActive"); this.collection = table.on(database);
        if (table.database() != SystemCollections.Database.CACHE) throw new IllegalArgumentException("metadata must use its CACHE resource");
    }
    void verify() { read(session -> null); }
    <T> T read(Function<ClientSession, T> action) { return transaction(false, action); }
    <T> T write(Function<ClientSession, T> action) { return transaction(true, action); }
    Document stamp(Document payload) { return new Document(payload).append(SCOPE, scopeDocument()); }
    Document payload(Document row) { var payload = new Document(row); payload.remove(SCOPE); return payload; }

    /** Other valid incarnations are absent to this view; damaged or unscoped records are not adopted. */
    boolean inScope(Document row, String id) {
        try {
            if (!(row.get(SCOPE) instanceof Document stored) || !stored.keySet().equals(Set.of("clusterId", "bindingVersion", "cacheIncarnation"))
                    || !(stored.get("bindingVersion") instanceof Long binding)) throw unreadable(id, "cache-scope");
            var found = new CacheBusinessScope(stored.getString("clusterId"), binding, stored.getString("cacheIncarnation"));
            return scope.equals(found);
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(id, "cache-scope"); }
    }
    private Document scopeDocument() { return new Document("clusterId", scope.clusterId()).append("bindingVersion", scope.bindingVersion()).append("cacheIncarnation", scope.cacheIncarnation()); }
    static TapstateException unreadable(String id, String field) { return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", id, "field", field), null); }

    private <T> T transaction(boolean mutation, Function<ClientSession, T> action) {
        return StoreIo.call(() -> {
            try {
                requireActive.run();
                try (var session = client.startSession()) {
                    T result = session.withTransaction(() -> {
                        requireActive.run();
                        var identity = new MongoCacheBusinessIdentity(database, scope);
                        if (mutation) identity.admitMetadataWrite(session); else identity.requireAdmissionState(session);
                        new MongoCacheMetadataStructure(database, MongoRateHistoryStore.DEFAULT_RETENTION).verifyFacet(table);
                        T value = action.apply(session); requireActive.run(); return value;
                    }, TRANSACTION);
                    requireActive.run(); return result;
                }
            } catch (RuntimeException failure) { requireActive.run(); throw failure; }
        });
    }
}
