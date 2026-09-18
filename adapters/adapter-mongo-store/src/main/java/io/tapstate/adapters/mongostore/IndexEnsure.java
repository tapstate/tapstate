package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;

import java.util.concurrent.TimeUnit;

/**
 * Brings one declared index into being, or into line. An index that does not exist is created with the
 * declared options; one that exists with a different expiry is altered in place; one that already matches
 * is left alone.
 *
 * <p>The alteration is the point. Creating an index whose name exists with different options is refused
 * by the server outright, so a startup path that only ever created would, on the day the retention was
 * changed, either fail to start or — worse — catch that refusal and carry on with the old expiry under
 * the new configuration. Neither is caught by anything that watches the data: the samples keep being
 * written and keep expiring, on the old schedule. Reading the existing options back first is what makes
 * a changed retention a change to the index rather than a note in a log.
 *
 * <p>The expiry is the one option an existing index is brought into line on; every other one is
 * compared and refused. Unconditional creation used to do that comparison for free -- the server refuses
 * a create whose name exists with different options, loudly -- and reading the options back first would
 * otherwise quietly accept them: a unique index that exists non-unique would be found by name, compared
 * on its expiry alone, and left as it is. The duplicate check beside it only looks at the rows present at
 * that moment; the constraint is what stops the duplicate that arrives next week.
 *
 * <p>Nothing here swallows a failure: a refusal to create or alter is the caller's to see.
 */
public final class IndexEnsure {

    private IndexEnsure() {
    }

    /**
     * Creates {@code index} on {@code collection}, or alters its expiry to match, or leaves it as it is.
     * {@code database} is the one the collection lives in; an alteration is a database command, and a
     * collection handle cannot hand its database back.
     */
    public static void ensure(MongoDatabase database, MongoCollection<Document> collection,
            SystemCollections.IndexSpec index) {
        Document existing = existing(collection, index);
        if (existing == null) {
            create(collection, index);
            return;
        }
        boolean uniqueThere = existing.getBoolean("unique", false);
        if (uniqueThere != index.unique()) {
            throw new IllegalStateException("index '" + index.indexName() + "' on '"
                    + collection.getNamespace().getFullName() + "' exists with unique=" + uniqueThere
                    + " where it is declared unique=" + index.unique()
                    + "; uniqueness cannot be altered in place, so the index has to be dropped and rebuilt."
                    + " Starting over it either way would mean running without a constraint the store's"
                    + " own declaration asks for, or with one it does not");
        }
        Long declared = index.expireAfterSeconds();
        Number found = existing.get("expireAfterSeconds", Number.class);
        if (declared == null || (found != null && found.longValue() == declared)) {
            return;
        }
        // The one option this store changes at runtime. The server refuses a create over it, so the index
        // is altered in place, by name, to the declared value.
        database.runCommand(new Document("collMod", collection.getNamespace().getCollectionName())
                .append("index", new Document("name", index.indexName()).append("expireAfterSeconds", declared)));
    }

    /** Creates {@code index} when no index of its name exists; an existing one is left exactly as it is. */
    public static void createIfAbsent(MongoCollection<Document> collection, SystemCollections.IndexSpec index) {
        if (existing(collection, index) == null) {
            create(collection, index);
        }
    }

    private static void create(MongoCollection<Document> collection, SystemCollections.IndexSpec index) {
        Document keys = new Document();
        for (String key : index.keys()) {
            keys.append(key, 1);
        }
        IndexOptions options = new IndexOptions().name(index.indexName()).unique(index.unique());
        if (index.expireAfterSeconds() != null) {
            options.expireAfter(index.expireAfterSeconds(), TimeUnit.SECONDS);
        }
        collection.createIndex(keys, options);
    }

    /** The existing index of that name, as the server describes it, or {@code null}. */
    public static Document existing(MongoCollection<Document> collection, SystemCollections.IndexSpec index) {
        for (Document candidate : collection.listIndexes()) {
            if (index.indexName().equals(candidate.getString("name"))) {
                return candidate;
            }
        }
        return null;
    }
}
