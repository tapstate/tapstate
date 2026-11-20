package io.tapstate.adapters.mongostore;

import com.mongodb.client.model.Projections;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.core.catalog.CatalogEntryReader;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.BundledConnectorIcon;
import io.tapstate.spi.store.ConnectorCatalogStore;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.RegistrationSource;
import org.bson.BsonBinarySubType;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.mongodb.client.model.Filters.exists;

/** Derived catalog rows retain the shared catalog codec and one original CACHE scope. */
final class MongoCacheConnectorCatalogStore implements ConnectorCatalogStore {
    private static final String BUNDLED = "bundledArtifact";
    private static final String ICON = "icon";
    private static final String HASH = "contentHash";
    private static final String VERSION = "pdkApiVersion";
    private static final String SOURCE = "source";
    private static final Bson CATALOG = Projections.exclude(BUNDLED);
    private static final Bson REGISTRATION = Projections.include("cacheScope",
            BUNDLED + "." + HASH, BUNDLED + "." + VERSION, BUNDLED + "." + SOURCE);
    private static final Bson REGISTRATION_AND_ICON = Projections.include("cacheScope", BUNDLED);

    private final MongoCacheMetadataAccess access;
    MongoCacheConnectorCatalogStore(MongoCacheMetadataAccess access) { this.access = access; }

    @Override public void upsert(ConnectorCatalogEntry entry) {
        replace(entry.id(), normalized(entry));
    }

    @Override public void upsertBundled(ConnectorCatalogEntry entry, ConnectorRegistration registration,
                                       Optional<BundledConnectorIcon> icon) {
        String id = entry.id();
        if (!id.equals(registration.connectorId()) || registration.source() != RegistrationSource.SEED
                || !validHash(registration.contentHash())) {
            throw new IllegalArgumentException("Catalog and bundled registration must describe the same seed");
        }
        Document bundled = new Document(HASH, registration.contentHash())
                .append(VERSION, registration.pdkApiVersion()).append(SOURCE, registration.source().name());
        icon.ifPresent(value -> {
            byte[] bytes = value.bytes();
            if (value.mediaType().isBlank() || bytes.length > BundledConnectorIcon.MAX_BYTES) {
                throw new IllegalArgumentException("Invalid bundled connector icon");
            }
            bundled.append(ICON, new Document("mediaType", value.mediaType()).append("bytes", new Binary(bytes)));
        });
        replace(id, normalized(entry).append(BUNDLED, bundled));
    }

    private static Document normalized(ConnectorCatalogEntry entry) {
        var normalized = MongoConnectorCatalogStore.toEntry(MongoConnectorCatalogStore.toDocument(entry));
        return MongoConnectorCatalogStore.toDocument(normalized);
    }

    private void replace(String id, Document row) {
        Document captured = access.stamp(row);
        access.write(session -> {
            access.collection.replaceOne(session, new Document("_id", id), captured, new ReplaceOptions().upsert(true));
            return null;
        });
    }

    @Override public Optional<ConnectorCatalogEntry> get(String connectorId) {
        return find(connectorId, CATALOG).map(row -> decode(row, connectorId));
    }

    @Override public List<ConnectorCatalogEntry> list() {
        return access.read(session -> {
            var entries = new ArrayList<ConnectorCatalogEntry>();
            try (var cursor = access.collection.find(session).projection(CATALOG).iterator()) {
                while (cursor.hasNext()) {
                    Document row = cursor.next();
                    String id = id(row);
                    if (access.inScope(row, id)) entries.add(decode(row, id));
                }
            }
            return List.copyOf(entries);
        });
    }

    @Override public List<ConnectorRegistration> bundledRegistrations() {
        return access.read(session -> {
            var registrations = new ArrayList<ConnectorRegistration>();
            try (var cursor = access.collection.find(session, exists(BUNDLED)).projection(REGISTRATION).iterator()) {
                while (cursor.hasNext()) {
                    Document row = cursor.next();
                    String id = id(row);
                    if (access.inScope(row, id)) registrations.add(registration(row.get(BUNDLED), id));
                }
            }
            return List.copyOf(registrations);
        });
    }

    @Override public Optional<ConnectorRegistration> bundledRegistration(String connectorId) {
        return find(connectorId, REGISTRATION)
                .filter(row -> row.containsKey(BUNDLED))
                .map(row -> registration(row.get(BUNDLED), connectorId));
    }

    @Override public Optional<BundledConnectorIcon> bundledIcon(String connectorId, String contentHash) {
        return find(connectorId, REGISTRATION_AND_ICON)
                .filter(row -> row.containsKey(BUNDLED))
                .filter(row -> registration(row.get(BUNDLED), connectorId).contentHash().equals(contentHash))
                .flatMap(row -> icon(row.get(BUNDLED, Document.class), connectorId));
    }

    private Optional<Document> find(String id, Bson projection) {
        Objects.requireNonNull(id, "connectorId");
        return access.read(session -> {
            Document row = access.collection.find(session, new Document("_id", id)).projection(projection).first();
            return row == null || !access.inScope(row, id) ? Optional.empty() : Optional.of(row);
        });
    }

    private static String id(Document row) {
        if (row.get("_id") instanceof String id) return id;
        throw MongoCacheMetadataAccess.unreadable("connector-catalog", "id");
    }

    private static boolean validHash(String hash) {
        return hash.matches("[0-9a-f]{64}");
    }

    private static ConnectorRegistration registration(Object value, String id) {
        if (value instanceof Document bundled && bundled.get(HASH) instanceof String hash && validHash(hash)
                && RegistrationSource.SEED.name().equals(bundled.get(SOURCE))
                && bundled.containsKey(VERSION)
                && (bundled.get(VERSION) == null || bundled.get(VERSION) instanceof String)) {
            return new ConnectorRegistration(id, hash, bundled.getString(VERSION), RegistrationSource.SEED);
        }
        throw MongoCacheMetadataAccess.unreadable(id, BUNDLED);
    }

    private static Optional<BundledConnectorIcon> icon(Document bundled, String id) {
        if (!bundled.containsKey(ICON)) return Optional.empty();
        if (bundled.get(ICON) instanceof Document icon
                && icon.get("mediaType") instanceof String mediaType && !mediaType.isBlank()
                && icon.get("bytes") instanceof Binary bytes && bytes.getType() == BsonBinarySubType.BINARY.getValue()
                && bytes.length() <= BundledConnectorIcon.MAX_BYTES) {
            return Optional.of(new BundledConnectorIcon(mediaType, bytes.getData()));
        }
        throw MongoCacheMetadataAccess.unreadable(id, BUNDLED + "." + ICON);
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
