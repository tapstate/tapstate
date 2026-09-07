package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.ReplaceOptions;
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

    /**
     * How many keys one batch read asks for at most. A request is a single BSON document with a hard
     * server-side ceiling, so a batch has to be split at some size; splitting at a count keeps the split
     * predictable for the common case, where keys are short and this bound is the one that is reached.
     */
    static final int MAX_KEYS_PER_READ = 1_000;

    /**
     * How large the ids of one batch may get before it is split regardless of how many there are. Keys
     * are renderings of business keys and have no length limit of their own, so a count alone does not
     * bound the request: a thousand long ones exceed the server's document ceiling, and what comes back
     * is a driver failure rather than a short answer. Half the ceiling, because the ids are not the whole
     * request.
     */
    private static final int MAX_ID_BYTES_PER_READ = 8 * 1024 * 1024;

    /** What one id costs beyond its two strings: the sub-document, its two field names, their lengths. */
    private static final int ID_OVERHEAD_BYTES = 48;

    /** The most bytes one Java character can become in UTF-8. Deliberately the bound, not the average. */
    private static final int WORST_CASE_BYTES_PER_CHAR = 3;

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
     * The states of {@code keys}, in as few round trips as the request ceiling allows. This is the read
     * a recompute makes — one key per row it is about to re-emit — and the difference between making it
     * here and making it a key at a time is three orders of magnitude on a large one, invisible in every
     * way but the clock.
     *
     * <p><b>The filter matches the whole id, never a path inside it.</b> The only index here is on
     * {@code _id}; a filter written {@code _id.k: {$in: [...]}} is not covered by it and answers by
     * reading every document in the collection — the same rows returned, at whole-collection cost, which
     * is exactly the failure this method exists to avoid while looking like the fix for it.
     *
     * <p><b>The id sub-documents are built by {@link #id}, which is not a tidiness.</b> BSON compares
     * sub-documents field by field in order, so an id assembled key-half-first matches nothing at all —
     * and nothing at all is what a namespace whose keys have no state yet also answers, so an operator
     * would quietly rebuild state it already had.
     */
    @Override
    public Map<String, byte[]> loadAll(String namespace, Collection<String> keys) {
        Objects.requireNonNull(namespace, "namespace");
        Map<String, byte[]> loaded = new LinkedHashMap<>();
        List<Document> batch = new ArrayList<>();
        int idBytes = 0;
        for (String key : keys) {
            batch.add(id(namespace, key));
            idBytes += ID_OVERHEAD_BYTES
                    + WORST_CASE_BYTES_PER_CHAR * (namespace.length() + key.length());
            if (batch.size() >= MAX_KEYS_PER_READ || idBytes >= MAX_ID_BYTES_PER_READ) {
                readInto(loaded, batch);
                batch.clear();
                idBytes = 0;
            }
        }
        readInto(loaded, batch);
        return loaded;
    }

    @Override
    public void save(String namespace, String key, byte[] state) {
        Objects.requireNonNull(state, "state");
        Document document = new Document("_id", id(namespace, key)).append(STATE, new Binary(state));
        StoreIo.run(() -> collection.replaceOne(byId(namespace, key), document,
                new ReplaceOptions().upsert(true)));
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

    /**
     * Reads one batch of ids into {@code loaded}. An empty batch sends nothing: a caller on the event
     * path arrives with one routinely, and a round trip that asks for no keys is paid in the common case
     * rather than the odd one.
     */
    private void readInto(Map<String, byte[]> loaded, List<Document> ids) {
        if (ids.isEmpty()) {
            return;
        }
        Document byIds = new Document("_id", new Document("$in", List.copyOf(ids)));
        StoreIo.run(() -> {
            for (Document document : collection.find(byIds)) {
                Binary state = document.get(STATE, Binary.class);
                if (state != null) {
                    loaded.put(document.get("_id", Document.class).getString(KEY), state.getData());
                }
            }
        });
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
