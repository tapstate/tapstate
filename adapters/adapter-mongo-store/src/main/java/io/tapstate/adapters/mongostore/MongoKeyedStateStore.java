package io.tapstate.adapters.mongostore;

import com.mongodb.ErrorCategory;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.KeyedStateStore;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The MongoDB cold layer under a stateful operator: one document per key, per namespace, holding that
 * key's state as opaque bytes.
 *
 * <p>The document id is the namespace and the key as two fields rather than as one joined string. A
 * separator would have to be a character neither part can contain, and the key is a rendering that can
 * contain anything a business key can — so joining them is a way for one namespace's entry to be read as
 * another's, silently and with a plausible-looking result. Two fields cannot be misread as each other.
 *
 * <p>Dropping a namespace whole is the one bulk operation here, and it is what lets nothing else need to
 * enumerate: a pipeline being taken down for good lets go of its state by naming the namespace, never by
 * listing the keys in it. It matches on the id's namespace half, which this module creates no index for
 * (as it creates none anywhere), so it is a scan — acceptable for a teardown and not for anything on the
 * event path, which is why nothing on the event path uses it.
 *
 * <p>A save is a replace-with-upsert and returns once the server has acknowledged it: the caller keeps
 * no queue and no replica of what it handed over, so anything still in flight when a process dies is
 * lost with nothing reporting it. Driver IO failures are translated into coded io diagnostics, so no
 * driver type escapes the module (rule R3).
 *
 * <p>What is inside the bytes belongs to the operator that wrote them and is never looked into here. A
 * store that parsed them would be a second place that has to agree about a shape it does not own.
 */
public final class MongoKeyedStateStore implements KeyedStateStore {

    /** The half of the id naming which namespace an entry belongs to. */
    static final String NAMESPACE = "ns";

    /** The half of the id naming which key within that namespace. */
    static final String KEY = "k";

    /** The field holding the state itself. */
    static final String STATE = "state";

    private final MongoCollection<Document> collection;

    public MongoKeyedStateStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    @Override
    public Optional<byte[]> load(String namespace, String key) {
        Document document = StoreIo.call(() -> collection.find(byId(namespace, key)).first());
        if (document == null) {
            return Optional.empty();
        }
        Binary state = document.get(STATE, Binary.class);
        return state == null ? Optional.empty() : Optional.of(state.getData());
    }

    /**
     * One query for however many keys were asked for. The ids are matched with {@code $in} on {@code _id}
     * itself rather than on a path inside it, for the reason {@link #count} matches a range of it: the
     * index is on the id, and a filter naming {@code _id.ns} would read the whole collection instead.
     *
     * <p>What comes back is keyed by the key half of each id, so a caller gets back the names it asked
     * with. Entries that are not there are simply not in the cursor, which is the same absence the
     * per-key form reports as an empty answer.
     */
    @Override
    public Map<String, byte[]> loadAll(String namespace, Collection<String> keys) {
        Objects.requireNonNull(namespace, "namespace");
        if (keys.isEmpty()) {
            return Map.of();
        }
        List<Document> ids = new ArrayList<>(keys.size());
        for (String key : keys) {
            ids.add(id(namespace, key));
        }
        Map<String, byte[]> found = new LinkedHashMap<>();
        StoreIo.run(() -> collection.find(new Document("_id", new Document("$in", ids)))
                .forEach(document -> {
                    Binary state = document.get(STATE, Binary.class);
                    if (state != null) {
                        found.put(document.get("_id", Document.class).getString(KEY), state.getData());
                    }
                }));
        return found;
    }

    @Override
    public void save(String namespace, String key, byte[] state) {
        Objects.requireNonNull(state, "state");
        Document document = new Document("_id", id(namespace, key)).append(STATE, new Binary(state));
        StoreIo.run(() -> collection.replaceOne(byId(namespace, key), document,
                new ReplaceOptions().upsert(true)));
    }

    /**
     * One round trip that both tests and writes: an upsert whose only change is applied on insert, asking
     * for the document as it was beforehand. Absent means this call inserted it; present means it was
     * already there and nothing was overwritten.
     *
     * <p>Two writers racing on a key that does not exist yet is the case this exists for, and the unique
     * id index is what settles it: one insert wins and the other is rejected, which the driver reports as
     * a duplicate key. That loser has not written anything, so what it owes its caller is the winner's
     * value -- read back rather than assumed, because assuming would hand back the candidate this method
     * exists to avoid handing back.
     */
    @Override
    public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] state) {
        Objects.requireNonNull(state, "state");
        Document onInsert = new Document("$setOnInsert", new Document(STATE, new Binary(state)));
        FindOneAndUpdateOptions asItWas = new FindOneAndUpdateOptions()
                .upsert(true)
                .returnDocument(ReturnDocument.BEFORE);
        Document before;
        try {
            before = StoreIo.call(
                    () -> collection.findOneAndUpdate(byId(namespace, key), onInsert, asItWas));
        } catch (TapstateException e) {
            if (!(e.getCause() instanceof MongoWriteException write)
                    || write.getError().getCategory() != ErrorCategory.DUPLICATE_KEY) {
                throw e;
            }
            // Lost the race: somebody else inserted between the check and the write. Their value is the
            // one that stands, and it is now readable.
            return load(namespace, key);
        }
        if (before == null) {
            return Optional.empty();
        }
        Binary existing = before.get(STATE, Binary.class);
        return existing == null ? Optional.empty() : Optional.of(existing.getData());
    }

    @Override
    public void delete(String namespace, String key) {
        StoreIo.run(() -> collection.deleteOne(byId(namespace, key)));
    }

    @Override
    public void dropNamespace(String namespace) {
        Objects.requireNonNull(namespace, "namespace");
        StoreIo.run(() -> collection.deleteMany(new Document("_id." + NAMESPACE, namespace)));
    }

    /**
     * Counted over a range of the id rather than by matching its namespace half, which is what lets this
     * be asked repeatedly where dropping a namespace may not. The id is a document of namespace then key,
     * and documents compare field by field in the order they are written - so every entry of one namespace
     * is a contiguous stretch of the id index, and a range with the smallest and largest possible key at
     * its ends is exactly that stretch. Matching {@code _id.ns} instead reads the same entries as a scan
     * of the whole collection, because the index is on the id and not on a path inside it.
     *
     * <p>Still an index walk rather than a stored total, so it is linear in what the namespace holds. That
     * is why nothing on the event path asks it.
     */
    @Override
    public long count(String namespace) {
        Objects.requireNonNull(namespace, "namespace");
        Document lower = new Document(NAMESPACE, namespace).append(KEY, new MinKey());
        Document upper = new Document(NAMESPACE, namespace).append(KEY, new MaxKey());
        Document withinNamespace = new Document("_id",
                new Document("$gte", lower).append("$lte", upper));
        return StoreIo.call(() -> collection.countDocuments(withinNamespace));
    }

    private static Document byId(String namespace, String key) {
        return new Document("_id", id(namespace, key));
    }

    private static Document id(String namespace, String key) {
        Objects.requireNonNull(namespace, "namespace");
        Objects.requireNonNull(key, "key");
        return new Document(NAMESPACE, namespace).append(KEY, key);
    }
}
