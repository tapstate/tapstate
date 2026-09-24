package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.spi.store.NestDeadLetterRecord;
import io.tapstate.spi.store.NestDeadLetterStore;
import org.bson.Document;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * The MongoDB channel holding what a nest could never assemble: one document per element, per namespace.
 *
 * <p>The document id is the namespace and the element as two fields rather than as one joined string, for
 * the reason the state beside it uses the same shape: a separator would have to be a character neither part
 * can contain, and an element identity is a rendering that can contain anything a business key can.
 *
 * <p>Filing per element is what makes a replay cost nothing. The same discarded change arriving a second
 * time replaces its own record rather than adding one, so a channel read after an hour of replay says the
 * same thing it said before the replay - which is what a reader is entitled to assume of a set of rows that
 * never reached a document.
 *
 * <p>The row is stored as a sub-document rather than as bytes, because unlike the state next door this is
 * meant to be read: an operator looking into what a pipeline threw away wants to query the rows, and bytes
 * would make this module the only thing that could open them. The one consequence to know about is that the
 * field names in it are the source's, so a source column named with a leading {@code $} is stored under that
 * name and is awkward to project on - awkward to read, never wrong to write, and preferable to renaming a
 * user's columns behind their back.
 *
 * <p>Reads are ordered by when the change was discarded, newest first, and take the namespace's stretch of
 * the id index the same way the state store's count does. This module creates no index anywhere, so the sort
 * is done by the server over that stretch; it is a read for whoever is looking rather than one on the event
 * path, and nothing on the event path performs it.
 */
public final class MongoNestDeadLetterStore implements NestDeadLetterStore {

    /** The half of the id naming which namespace a discarded change belongs to. */
    static final String NAMESPACE = "ns";

    /** The half of the id naming which element within that namespace. */
    static final String ELEMENT = "el";

    /** The streams the change covered, which is where a reader goes looking for it. */
    static final String CHAIN = "chain";

    /** The order the change carried, which is what anything about it is decided by. */
    static final String ORDER = "order";

    /** How long it had been waiting when it was given up on. */
    static final String HELD_FOR_MILLIS = "heldForMillis";

    /** When it was given up on, which is what orders a listing. */
    static final String DISCARDED_AT = "discardedAt";

    /** The row itself, absent where the change removed the element rather than putting a row there. */
    static final String ROW = "row";

    private final MongoCollection<Document> collection;

    public MongoNestDeadLetterStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    @Override
    public void record(NestDeadLetterRecord record) {
        Objects.requireNonNull(record, "record");
        Document document = new Document("_id", id(record.namespace(), record.element()))
                .append(CHAIN, record.chain())
                .append(ORDER, record.order())
                .append(HELD_FOR_MILLIS, record.heldForMillis())
                .append(DISCARDED_AT, record.discardedAt());
        if (!record.deletion()) {
            document.append(ROW, RowImages.toDocument(record.row()));
        }
        StoreIo.run(() -> collection.replaceOne(byId(record.namespace(), record.element()), document,
                new ReplaceOptions().upsert(true)));
    }

    @Override
    public List<NestDeadLetterRecord> read(String namespace, int limit) {
        Objects.requireNonNull(namespace, "namespace");
        if (limit <= 0) {
            return List.of();
        }
        return StoreIo.call(() -> {
            List<NestDeadLetterRecord> held = new ArrayList<>();
            collection.find(withinNamespace(namespace))
                    .sort(new Document(DISCARDED_AT, -1))
                    .limit(limit)
                    .forEach(document -> held.add(recordOf(namespace, document)));
            return held;
        });
    }

    @Override
    public void dropNamespace(String namespace) {
        Objects.requireNonNull(namespace, "namespace");
        StoreIo.run(() -> collection.deleteMany(new Document("_id." + NAMESPACE, namespace)));
    }

    /**
     * The namespace's own stretch of the id index. The id is a document of namespace then element, and
     * documents compare field by field in the order they are written, so every entry of one namespace is
     * contiguous and a range with the smallest and largest possible element at its ends is exactly it.
     * Matching {@code _id.ns} instead reads the same entries as a scan of the whole collection, because the
     * index is on the id and not on a path inside it.
     */
    private static Document withinNamespace(String namespace) {
        Document lower = new Document(NAMESPACE, namespace).append(ELEMENT, new MinKey());
        Document upper = new Document(NAMESPACE, namespace).append(ELEMENT, new MaxKey());
        return new Document("_id", new Document("$gte", lower).append("$lte", upper));
    }

    private static NestDeadLetterRecord recordOf(String namespace, Document document) {
        Document id = document.get("_id", Document.class);
        Document row = document.get(ROW, Document.class);
        return new NestDeadLetterRecord(
                namespace,
                id.getString(ELEMENT),
                document.getString(CHAIN),
                document.getString(ORDER),
                document.getLong(HELD_FOR_MILLIS),
                document.getLong(DISCARDED_AT),
                row == null ? null : RowImages.toRow(row));
    }

    private static Document byId(String namespace, String element) {
        return new Document("_id", id(namespace, element));
    }

    private static Document id(String namespace, String element) {
        return new Document(NAMESPACE, namespace).append(ELEMENT, element);
    }
}
