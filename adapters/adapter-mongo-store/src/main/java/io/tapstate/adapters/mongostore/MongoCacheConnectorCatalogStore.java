package io.tapstate.adapters.mongostore;

import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.catalog.CatalogEntryReader;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectorCatalogStore;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Derived catalog rows retain the shared catalog codec and one original CACHE scope. */
final class MongoCacheConnectorCatalogStore implements ConnectorCatalogStore {
    private final MongoCacheMetadataAccess access;
    MongoCacheConnectorCatalogStore(MongoCacheMetadataAccess access) { this.access = access; }
    @Override public void upsert(ConnectorCatalogEntry entry) {
        var normalized = MongoConnectorCatalogStore.toEntry(MongoConnectorCatalogStore.toDocument(Objects.requireNonNull(entry, "entry")));
        Document captured = access.stamp(MongoConnectorCatalogStore.toDocument(normalized));
        access.write(session -> {
            access.collection.replaceOne(session, new Document("_id", entry.id()), captured, new ReplaceOptions().upsert(true)); return null;
        });
    }
    @Override public Optional<ConnectorCatalogEntry> get(String connectorId) {
        Objects.requireNonNull(connectorId, "connectorId");
        return access.read(session -> {
            Document row = access.collection.find(session, new Document("_id", connectorId)).first();
            return row == null || !access.inScope(row, connectorId) ? Optional.empty() : Optional.of(decode(row, connectorId));
        });
    }
    @Override public List<ConnectorCatalogEntry> list() {
        return access.read(session -> {
            var entries = new ArrayList<ConnectorCatalogEntry>();
            try (var cursor = access.collection.find(session).iterator()) {
                while (cursor.hasNext()) {
                    Document row = cursor.next();
                    if (!(row.get("_id") instanceof String id)) throw MongoCacheMetadataAccess.unreadable("connector-catalog", "id");
                    if (access.inScope(row, id)) entries.add(decode(row, id));
                }
            }
            return List.copyOf(entries);
        });
    }
    private ConnectorCatalogEntry decode(Document row, String id) {
        try {
            Document payload = access.payload(row); var entry = MongoConnectorCatalogStore.toEntry(payload);
            var tree = new java.util.LinkedHashMap<String, Object>(payload); tree.remove("_id");
            if (!id.equals(entry.id()) || !CatalogEntryReader.isCanonicalPersistenceTree(tree)) throw MongoCacheMetadataAccess.unreadable(id, "entry");
            return entry;
        } catch (TapstateException | NullPointerException | ClassCastException | IllegalArgumentException invalid) {
            throw MongoCacheMetadataAccess.unreadable(id, "entry");
        }
    }
}
