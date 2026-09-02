package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CatalogStore;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.IoError;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * The MongoDB connection catalog: stores each registered connection as one structured document keyed
 * by the connection's id, holding the connector id and the settings map as fields.
 *
 * <p>Unlike the artifact truth layer, a connection config has no canonical serialization contract to
 * reuse, so it is stored as a structured document rather than a canonical text body. The settings map
 * is kept as a nested sub-document — which also lets the catalog be queried by a settings field or the
 * connector id later. On read the sub-document is normalized back to plain Java maps and lists, so no
 * driver {@code Document} type escapes this module.
 */
public final class MongoCatalogStore implements CatalogStore {

    private final MongoCollection<Document> collection;

    public MongoCatalogStore(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    @Override
    public void save(ConnectionConfig connection) {
        Objects.requireNonNull(connection, "connection");
        // Upsert by the connection id (the document _id): the stored form is a full replacement, so a
        // re-save of the same id overwrites in place rather than accumulating documents.
        StoreIo.run(() -> collection.replaceOne(
                new Document("_id", connection.id()), toDocument(connection), new ReplaceOptions().upsert(true)));
    }

    @Override
    public Optional<ConnectionConfig> get(String id) {
        Objects.requireNonNull(id, "id");
        Document document = StoreIo.call(() -> collection.find(new Document("_id", id)).first());
        return document == null ? Optional.empty() : Optional.of(toConfig(document));
    }

    @Override
    public List<ConnectionConfig> list() {
        // A reconstruction failure (io.document-unreadable) passes through the driver-failure
        // translation untouched, and the explicitly-closed cursor is released even on that path — a
        // for-each over the iterable would not close it on the exception path.
        return StoreIo.call(() -> {
            List<ConnectionConfig> connections = new ArrayList<>();
            try (MongoCursor<Document> cursor = collection.find().iterator()) {
                while (cursor.hasNext()) {
                    connections.add(toConfig(cursor.next()));
                }
            }
            return connections;
        });
    }

    /** Maps a connection to its stored document: the id as {@code _id}, connector id and settings as fields. */
    static Document toDocument(ConnectionConfig connection) {
        return new Document("_id", connection.id())
                .append("connectorId", connection.connectorId())
                .append("settings", new Document(connection.settings()));
    }

    /** Reconstructs a connection from its stored document, normalizing the settings to plain Java types. */
    static ConnectionConfig toConfig(Document document) {
        String id = document.getString("_id");
        String connectorId = document.getString("connectorId");
        if (connectorId == null) {
            // A stored connection missing its connector id is store corruption, surfaced as a coded io
            // diagnostic rather than a bare null-argument crash while reconstructing.
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", String.valueOf(id), "field", "connectorId"), null);
        }
        Object settings = document.get("settings");
        if (settings != null && !(settings instanceof Document)) {
            // A settings field that is present but is not a sub-document is corruption — surfaced as
            // unreadable rather than silently coerced to an empty-settings config.
            throw new TapstateException(IoError.DOCUMENT_UNREADABLE,
                    Map.of("id", String.valueOf(id), "field", "settings"), null);
        }
        Map<String, Object> normalized = settings instanceof Document sub ? normalizeMap(sub) : Map.of();
        return new ConnectionConfig(id, connectorId, normalized);
    }

    /** Copies a bson document into a plain map, normalizing every value away from driver types. */
    private static Map<String, Object> normalizeMap(Document document) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : document.entrySet()) {
            out.put(entry.getKey(), normalizeValue(entry.getValue()));
        }
        return out;
    }

    /** Normalizes a stored value away from driver types: nested documents, lists, and bson leaf types. */
    private static Object normalizeValue(Object value) {
        if (value instanceof Document document) {
            return normalizeMap(document);
        }
        if (value instanceof List<?> list) {
            List<Object> out = new ArrayList<>(list.size());
            for (Object element : list) {
                out.add(normalizeValue(element));
            }
            return out;
        }
        // A driver-native leaf type (from a foreign or corrupt document) must not escape as an
        // org.bson type; represent it as the plain Java value the rest of the platform expects.
        if (value instanceof Decimal128 decimal) {
            return decimal.bigDecimalValue();
        }
        if (value instanceof ObjectId objectId) {
            return objectId.toHexString();
        }
        if (value instanceof Binary binary) {
            return binary.getData();
        }
        return value;
    }
}
