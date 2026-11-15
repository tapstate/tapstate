package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.spi.store.CacheBusinessScope;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.bson.Document;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

/** Dependency reads shared only by one checkpoint proof in its original transaction callback. */
final class MongoSrsReadFacts {
    private record Lookup(SystemCollections table, String filter, boolean idOnly) {}
    private static final JsonWriterSettings EXACT_BSON = JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).build();
    private final ClientSession session;
    private final MongoDatabase database;
    private final CacheBusinessScope scope;
    private final Map<Lookup, Optional<Document>> rows = new HashMap<>();
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

    void seed(SystemCollections table, Document row) {
        rows.put(lookup(table, new Document("_id", row.get("_id")), false), Optional.of(row));
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
