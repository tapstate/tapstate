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

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.concurrent.TimeUnit;

/** One metadata facet's original CACHE client and business scope; mutations contend with retirement. */
final class MongoCacheMetadataAccess {
    private static final TransactionOptions TRANSACTION = transactionOptions().build();
    private static TransactionOptions.Builder transactionOptions() {
        return TransactionOptions.builder().readConcern(ReadConcern.SNAPSHOT)
                .writeConcern(WriteConcern.MAJORITY.withJournal(true));
    }
    private static final String SCOPE = "cacheScope";
    private final MongoClient client;
    private final MongoDatabase database;
    private final CacheBusinessScope scope;
    private final Runnable requireActive;
    final MongoCollection<Document> collection;

    MongoCacheMetadataAccess(MongoClient client, MongoDatabase database, CacheBusinessScope scope,
            SystemCollections table, Runnable requireActive) {
        this.client = Objects.requireNonNull(client, "client");
        this.database = Objects.requireNonNull(database, "database");
        this.scope = Objects.requireNonNull(scope, "scope");
        this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
        if (table.database() != SystemCollections.Database.CACHE) throw new IllegalArgumentException("metadata must use its CACHE resource");
        this.collection = table.indexTargetOn(database);
    }

    <T> T read(Function<ClientSession, T> action) { return transaction(false, TRANSACTION, null, action); }

    <T> T read(Duration timeout, Function<ClientSession, T> action) {
        var budget = new MongoReadBudget(timeout);
        return transaction(false, transactionOptions()
                .timeout(Math.max(1L, timeout.toMillis()), TimeUnit.MILLISECONDS).build(), budget, action);
    }

    <T> T write(Function<ClientSession, T> action) { return transaction(true, TRANSACTION, null, action); }

    /** Each non-transactional payload batch stays between checks of its original business owner. */
    void writeBatch(String id, Runnable action) {
        read(session -> null);
        MongoStructureFailureScope.inspectOnFailure(client, () -> StoreIo.call(id, () -> {
            action.run();
            return null;
        }));
        read(session -> null);
    }

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

    private <T> T transaction(boolean mutation, TransactionOptions options, MongoReadBudget budget, Function<ClientSession, T> action) {
        return MongoStructureFailureScope.inspectOnFailure(client, budget, () -> StoreIo.call(() -> {
            try {
                requireActive.run();
                try (var session = client.startSession()) {
                    T result = session.withTransaction(() -> {
                        MongoStructureFailureScope.beginCallback();
                        requireActive.run();
                        if (budget != null) budget.remainingMillis();
                        var identity = new MongoCacheBusinessIdentity(database, scope);
                        if (mutation) identity.admitMetadataWrite(session); else identity.requireAdmissionState(session);
                        T value = action.apply(session);
                        if (budget != null) budget.remainingMillis();
                        requireActive.run(); return value;
                    }, options);
                    requireActive.run(); return result;
                }
            } catch (RuntimeException failure) { requireActive.run(); throw failure; }
        }));
    }
}
