package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.SrsModelKey;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

/** Dependency reads shared only by one read operation in its original transaction callback. */
final class MongoSrsReadFacts {
    private record Lookup(SystemCollections table, String filter, boolean idOnly) {}
    private static final JsonWriterSettings EXACT_BSON = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();
    private final ClientSession session;
    private final MongoDatabase database;
    private final CacheBusinessScope scope;
    private final Map<Lookup, Optional<Document>> rows = new HashMap<>();
    private final Map<Set<SrsModelKey>, Optional<Map<SrsModelKey, MongoSrsModelStore.ModelReference>>> currentReferences = new HashMap<>();
    private boolean businessChecked;

    MongoSrsReadFacts(ClientSession session, MongoDatabase database, CacheBusinessScope scope,
            Document root, boolean businessChecked) {
        this(session,database,scope,businessChecked);
        seed(SystemCollections.SRS_CHAINS, root);
    }

    MongoSrsReadFacts(ClientSession session, MongoDatabase database, CacheBusinessScope scope, boolean businessChecked) {
        this.session = session;
        this.database = database;
        this.scope = scope;
        this.businessChecked = businessChecked;
    }

    void requireBusiness(ClientSession original, CacheBusinessScope expected, Runnable verify) {
        requireSession(original, database);
        if (!scope.equals(expected)) throw new IllegalStateException("checkpoint facts belong to another business scope");
        if (!businessChecked) {
            verify.run();
            businessChecked = true;
        }
    }

    Optional<Map<SrsModelKey, MongoSrsModelStore.ModelReference>> currentReferences(ClientSession original,
            MongoDatabase selected, Set<SrsModelKey> keys,
            Supplier<Optional<Map<SrsModelKey, MongoSrsModelStore.ModelReference>>> read) {
        requireSession(original, selected);
        return currentReferences.computeIfAbsent(Set.copyOf(keys), ignored -> read.get());
    }

    void seed(SystemCollections table, Document row) {
        rows.put(lookup(table, new Document("_id", row.get("_id")), false), Optional.of(row));
    }

    /** Only positive, exact rows enter this callback's memo; absent and ambiguous rows keep their point reads. */
    void prefetchArrival(String chain, Document consumerId) {
        requireSession(session, database);
        var bytes = new Document("$bsonSize", "$$ROOT");
        var stages = new java.util.ArrayList<Document>();
        stages.add(new Document("$match", new Document("_id", chain)));
        stages.add(new Document("$project", new Document("root", new Document("$cond", java.util.List.of(
                new Document("$lte", java.util.List.of(bytes, 64 * 1024)), "$$ROOT", false)))));
        var requested = java.util.List.of(SystemCollections.SRS_CONSUMERS, SystemCollections.CACHE_SRS_CONSUMER_OFFSETS);
        for (var table : requested) {
            stages.add(new Document("$lookup", new Document("from", table.collectionName()).append("as", table.collectionName())
                    .append("pipeline", java.util.List.of(new Document("$match", new Document("_id", consumerId)),
                            new Document("$limit", 2), new Document("$project", new Document("value", new Document("$cond", java.util.List.of(
                                    new Document("$lte", java.util.List.of(bytes, 64 * 1024)), "$$ROOT", false))))))));
        }
        var captured = SystemCollections.SRS_CHAINS.on(database).aggregate(session, stages).first();
        if (captured == null || !(captured.get("root") instanceof Document root) || !chain.equals(root.get("_id"))) return;
        seed(SystemCollections.SRS_CHAINS, root);
        for (var table : requested) {
            var found = captured.getList(table.collectionName(), Document.class);
            if (found.size() == 1 && found.getFirst().get("value") instanceof Document row && exact(consumerId, row.get("_id")))
                seed(table, row);
        }
    }

    /** Prefetches the bounded predicates of one retained checkpoint without weakening their decoders. */
    void prefetchStartupCheckpoint(Document root, io.tapstate.spi.store.SrsCaptureCheckpoint checkpoint) {
        requireSession(session, database);
        if (checkpoint == null || checkpoint.tables().isEmpty() || checkpoint.tables().size() > 8) return;
        var requested = new java.util.LinkedHashMap<Lookup, Query>();
        var contexts = new MongoSrsModelContexts(database, scope);
        for (var at : checkpoint.tables().values()) {
            var key = at.model();
            var head = contexts.head(root, key.table());
            if (head == null || !(head.get("ringName") instanceof String ring)) return;
            addQuery(requested, SystemCollections.SRS_MODEL_CONTEXTS,
                    new Document("_id", MongoSrsModelContexts.contextId(key)), false);
            addQuery(requested, SystemCollections.SRS_MODEL_CONTEXTS,
                    new Document("_id", head.get("contextId")), false);
            addQuery(requested, SystemCollections.SRS_MODELS,
                    new Document("_id", MongoSrsModelStore.key(key)), false);
            addQuery(requested, SystemCollections.SRS_MODEL_CONTEXTS,
                    new Document("cacheIncarnation", scope.cacheIncarnation()).append("ringName", ring), false);
            addQuery(requested, SystemCollections.SRS_MODELS,
                    new Document("cacheIncarnation", scope.cacheIncarnation()).append("ringName", ring), true);
            addQuery(requested, SystemCollections.SRS_LOG_BOUNDS,
                    new Document("_id", MongoSrsLogStore.boundsId(ring)), false);
        }
        prefetchStartupProofs(root, requested);
    }

    private void prefetchStartupProofs(Document root, Map<Lookup, Query> requested) {
        if (requested.isEmpty()) return;
        var stages = new java.util.ArrayList<Document>();
        stages.add(new Document("$match", new Document("_id", root.get("_id"))));
        stages.add(new Document("$project", new Document("_id", 1)));
        var queries = java.util.List.copyOf(requested.values());
        for (int i = 0; i < queries.size(); i++) {
            var query = queries.get(i);
            var bytes = new Document("$bsonSize", "$$ROOT");
            stages.add(new Document("$lookup", new Document("from", query.table().collectionName())
                    .append("as", "proof" + i).append("pipeline", java.util.List.of(
                            new Document("$match", query.filter()), new Document("$limit", 2),
                            new Document("$project", new Document("value", new Document("$cond", java.util.List.of(
                                    new Document("$lte", java.util.List.of(bytes, 64 * 1024)), "$$ROOT", false))))))));
        }
        var captured = SystemCollections.SRS_CHAINS.on(database).aggregate(session, stages).first();
        if (captured == null || !exact(root.get("_id"), captured.get("_id"))) return;
        for (int i = 0; i < queries.size(); i++) {
            var query = queries.get(i);
            var found = captured.getList("proof" + i, Document.class);
            if (found.isEmpty() || !query.idOnly() && found.size() != 1
                    || !(found.getFirst().get("value") instanceof Document value)) continue;
            if (query.filter().entrySet().stream().anyMatch(entry -> !exact(entry.getValue(), value.get(entry.getKey()))))
                continue;
            Document result = query.idOnly() ? new Document("_id", value.get("_id")) : value;
            rows.put(lookup(query.table(), query.filter(), query.idOnly()), Optional.of(result));
        }
    }

    private record Query(SystemCollections table, Document filter, boolean idOnly) {}

    private void addQuery(Map<Lookup, Query> requested, SystemCollections table, Document filter, boolean idOnly) {
        var key = lookup(table, filter, idOnly);
        if (!rows.containsKey(key)) requested.putIfAbsent(key, new Query(table, filter, idOnly));
    }

    private static boolean exact(Object expected, Object actual) {
        if (expected instanceof Document key)
            return actual instanceof Document value && new java.util.ArrayList<>(key.keySet()).equals(
                    new java.util.ArrayList<>(value.keySet())) && key.equals(value);
        return java.util.Objects.equals(expected, actual);
    }

    /** Seeds only a complete batch of exact IDs; aliases and missing rows fall back to point proofs. */
    boolean prefetchExactIds(SystemCollections table, Collection<?> ids, Runnable requireActive) {
        requireSession(session, database);
        var remaining = new java.util.LinkedHashMap<Lookup, Object>();
        for (Object id : ids) {
            var key = lookup(table, new Document("_id", id), false);
            var known = rows.get(key);
            if (known == null) remaining.put(key, id);
            else if (known.isEmpty() || !key.equals(lookup(table, new Document("_id", known.orElseThrow().get("_id")), false)))
                return false;
        }
        if (remaining.isEmpty()) return true;
        var requested = java.util.List.copyOf(remaining.values());
        var found = new HashMap<Lookup, Optional<Document>>();
        try (var cursor = table.on(database).find(session, new Document("_id", new Document("$in", requested)))
                .limit(remaining.size() + 1).batchSize(64).iterator()) {
            while (cursor.hasNext()) {
                requireActive.run();
                var row = cursor.next();
                var key = lookup(table, new Document("_id", row.get("_id")), false);
                if (remaining.remove(key) == null) return false;
                found.put(key, Optional.of(row));
            }
        }
        if (!remaining.isEmpty()) return false;
        rows.putAll(found);
        return true;
    }

    static Document find(MongoSrsReadFacts facts, ClientSession session, MongoDatabase database,
            SystemCollections table, Document filter) {
        return find(facts, session, database, table, filter, false);
    }

    static Document find(MongoSrsReadFacts facts, ClientSession session, MongoDatabase database,
            SystemCollections table, Document filter, boolean idOnly) {
        if (facts == null) return query(session, database, table, filter, idOnly);
        facts.requireSession(session, database);
        return facts.rows.computeIfAbsent(facts.lookup(table, filter, idOnly),
                ignored -> Optional.ofNullable(query(session, database, table, filter, idOnly))).orElse(null);
    }

    private static Document query(ClientSession session, MongoDatabase database,
            SystemCollections table, Document filter, boolean idOnly) {
        var selected = table.on(database).find(session, filter);
        return (idOnly ? selected.projection(new Document("_id", 1)) : selected).first();
    }

    Document bounds(ClientSession original, MongoCollection<Document> selected, Document filter) {
        requireSession(original, database);
        if (!selected.getNamespace().equals(SystemCollections.SRS_LOG_BOUNDS.on(database).getNamespace()))
            throw new IllegalStateException("checkpoint bounds belong to another collection");
        return rows.computeIfAbsent(lookup(SystemCollections.SRS_LOG_BOUNDS, filter, false),
                ignored -> Optional.ofNullable(selected.find(original, filter).first())).orElse(null);
    }

    private Lookup lookup(SystemCollections table, Document filter, boolean idOnly) {
        // Embedded document equality in MongoDB includes field order and BSON types.
        return new Lookup(table, filter.toBsonDocument(Document.class, database.getCodecRegistry()).toJson(EXACT_BSON), idOnly);
    }

    private void requireSession(ClientSession original, MongoDatabase selected) {
        if (session != original || database != selected || !session.hasActiveTransaction())
            throw new IllegalStateException("checkpoint facts require their original active transaction callback");
    }
}
