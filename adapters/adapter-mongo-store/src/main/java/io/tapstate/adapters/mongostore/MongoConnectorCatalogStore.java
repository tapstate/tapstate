package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.catalog.CatalogEntryReader;
import io.tapstate.core.catalog.CatalogEntryWriter;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectorCatalogStore;
import io.tapstate.spi.store.IoError;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The MongoDB derived connector-catalog store: stores each registered connector's normalized catalog
 * row as one structured document keyed by the connector id. The row is serialized through the catalog
 * product's writer to the same neutral tree the bundled catalog uses, so the persisted shape matches
 * the bundled one and is reconstructed on read by the same reader — a single serialization contract
 * for both the offline snapshot and the online store.
 *
 * <p>The tree is a fixed shape of scalars, lists and nested maps, mapped into a bson document as-is;
 * on read it is reconstructed straight from the driver's {@code Document}, which is itself a map, so no
 * driver type escapes this module (rule R3). A stored row whose shape cannot be reconstructed is
 * surfaced as a coded {@code io.document-unreadable} diagnostic rather than a bare crash.
 */
public final class MongoConnectorCatalogStore implements ConnectorCatalogStore {

    private final MongoCollection<Document> collection;

    public MongoConnectorCatalogStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    @Override
    public void upsert(ConnectorCatalogEntry entry) {
        Objects.requireNonNull(entry, "entry");
        // Upsert by the connector id (the document _id): the stored form is a full replacement, so a
        // re-register of the same connector overwrites in place rather than accumulating documents.
        StoreIo.run(() -> collection.replaceOne(
                new Document("_id", entry.id()), toDocument(entry), new ReplaceOptions().upsert(true)));
    }

    @Override
    public Optional<ConnectorCatalogEntry> get(String connectorId) {
        Objects.requireNonNull(connectorId, "connectorId");
        Document document = StoreIo.call(() -> collection.find(new Document("_id", connectorId)).first());
        return document == null ? Optional.empty() : Optional.of(toEntry(document));
    }

    @Override
    public List<ConnectorCatalogEntry> list() {
        // A reconstruction failure (io.document-unreadable) passes through the driver-failure
        // translation untouched, and the explicitly-closed cursor is released even on that path — a
        // for-each over the iterable would not close it on the exception path.
        return StoreIo.call(() -> {
            List<ConnectorCatalogEntry> rows = new ArrayList<>();
            try (MongoCursor<Document> cursor = collection.find().iterator()) {
                while (cursor.hasNext()) {
                    rows.add(toEntry(cursor.next()));
                }
            }
            return rows;
        });
    }

    /** Maps a catalog row to its stored document: the connector id as {@code _id}, the row's serde tree as fields. */
    static Document toDocument(ConnectorCatalogEntry entry) {
        Document document = (Document) toBson(CatalogEntryWriter.toTree(entry));
        document.put("_id", entry.id());
        return document;
    }

    /** Reconstructs a catalog row from its stored document, or fails coded when the shape is unreadable. */
    static ConnectorCatalogEntry toEntry(Document document) {
        try {
            return CatalogEntryReader.fromTree(document);
        } catch (IllegalArgumentException e) {
            // A stored row whose shape cannot be reconstructed is store corruption, surfaced as a coded
            // io diagnostic rather than a bare crash while reconstructing.
            throw new TapstateException(
                    IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", String.valueOf(document.getString("_id")), "field", "entry"), e);
        }
    }

    /** Converts a neutral serde tree (maps / lists / scalars) into its bson document / list form. */
    private static Object toBson(Object value) {
        if (value instanceof Map<?, ?> map) {
            Document document = new Document();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                document.put(String.valueOf(entry.getKey()), toBson(entry.getValue()));
            }
            return document;
        }
        if (value instanceof List<?> list) {
            List<Object> out = new ArrayList<>(list.size());
            for (Object element : list) {
                out.add(toBson(element));
            }
            return out;
        }
        return value;
    }
}
