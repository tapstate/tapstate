package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The {@link Endpoints} driver for a Mongo store, addressed by the connection string the resource
 * carries as its {@code uri}. Rule R3 locks this driver to adapter-mongo-store for the rings; this
 * module is deliberately not one of them.
 *
 * <p>Row shape is deliberately minimal - {@code _id} and a sequence - because the {@code seed}
 * generator vocabulary is not decided yet. What is decided is that it must be deterministic: a
 * specification that seeds three rows and counts three rows may not depend on what those rows held.
 */
final class MongoEndpoints implements Endpoints {

    private static final String SEQUENCE_FIELD = "seq";
    private static final String TOUCHED_FIELD = "touched";

    private final Map<String, MongoClient> clientsByUri = new LinkedHashMap<>();

    @Override
    public void seed(String uri, String table, long rows) {
        MongoCollection<Document> collection = collection(uri, table);
        collection.drop();
        insertRange(collection, 1, rows);
    }

    @Override
    public void cdc(String uri, String table, CdcOp op, long rows) {
        MongoCollection<Document> collection = collection(uri, table);
        switch (op) {
            case INSERT -> insertRange(collection, highestId(collection) + 1, rows);
            case UPDATE -> collection.updateMany(
                    Filters.lte("_id", rows), Updates.set(TOUCHED_FIELD, true));
            case DELETE -> collection.deleteMany(Filters.lte("_id", rows));
        }
    }

    /**
     * Every document in the collection, read the way a user would - from outside the product, by a
     * driver that is not it. Whole documents rather than one field: what a value should be is the
     * caller's assertion to make, and a reader that already knows which field to look at cannot report
     * a document that landed in an unexpected shape.
     */
    public List<Document> documents(String uri, String table) {
        return collection(uri, table).find().into(new ArrayList<>());
    }

    @Override
    public long count(String uri, String table) {
        return collection(uri, table).countDocuments();
    }

    /**
     * The collections the target holds. An empty expected collection and rows written to a differently
     * named one look identical from a count, and the second is the likelier mistake - so a witness that
     * finds nothing where it looked can say what is actually there.
     */
    List<String> collections(String uri) {
        ConnectionString connectionString = new ConnectionString(uri);
        String database = connectionString.getDatabase();
        if (database == null) {
            throw new EnvelopeException(
                    "the endpoint at " + uri + " names no database, so there are no collections to list");
        }
        return client(uri).getDatabase(database).listCollectionNames().into(new ArrayList<>());
    }

    @Override
    public void close() {
        clientsByUri.values().forEach(MongoClient::close);
        clientsByUri.clear();
    }

    private void insertRange(MongoCollection<Document> collection, long firstId, long rows) {
        List<Document> documents = new ArrayList<>();
        for (long id = firstId; id < firstId + rows; id++) {
            documents.add(new Document("_id", id).append(SEQUENCE_FIELD, id));
        }
        if (!documents.isEmpty()) {
            collection.insertMany(documents);
        }
    }

    private long highestId(MongoCollection<Document> collection) {
        Document highest = collection.find().sort(new Document("_id", -1)).limit(1).first();
        return highest == null ? 0L : highest.getLong("_id");
    }

    private MongoCollection<Document> collection(String uri, String table) {
        ConnectionString connectionString = new ConnectionString(uri);
        String database = connectionString.getDatabase();
        if (database == null) {
            throw new EnvelopeException(
                    "the endpoint at " + uri + " names no database, so there is no table to address");
        }
        return client(uri).getDatabase(database).getCollection(table);
    }

    private MongoClient client(String uri) {
        return clientsByUri.computeIfAbsent(uri, MongoClients::create);
    }
}
