package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.store.OperatorPlacement;
import org.bson.Document;
import org.bson.types.Binary;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** Opaque operator bytes, split by resource type and fenced by the original Pipeline execution. */
final class MongoCacheOperatorStateStore implements KeyedStateStore {
    private static final int BATCH_KEYS = 256;
    private static final int BATCH_BYTES = 4 * 1024 * 1024;
    private final MongoCachePipelineAccess access;
    private final OperatorPlacement placement;
    private final SystemCollections table;
    private final Document address;
    private final String mapName;
    private final long addressBytes;

    MongoCacheOperatorStateStore(MongoCachePipelineAccess access, OperatorPlacement placement) {
        this.access = Objects.requireNonNull(access, "access"); this.placement = Objects.requireNonNull(placement, "placement");
        if (!access.scope.equals(placement.scope()) || !access.database.getName().equals(placement.database())) throw MongoCachePipelineAccess.fenced();
        table = table(placement.resource()); mapName = placement.mapName();
        address = new Document("scope", MongoPipelinePreparationFacts.scope(placement.scope()))
                .append("database", placement.database()).append("resource", placement.resource().name())
                .append("stepId", OperatorStateText.encode(placement.stepId())).append("pathId", OperatorStateText.encode(placement.pathId())).append("namespace", OperatorStateText.encode(placement.namespace()));
        addressBytes = 1024L + 3L * address.toJson().length();
    }

    static SystemCollections table(OperatorPlacement.Resource resource) {
        return switch (resource) {
            case NEST_STATE -> SystemCollections.CACHE_NEST_STATE;
            case JOIN_FACTS -> SystemCollections.CACHE_JOIN_FACTS;
            case JOIN_DIMENSIONS -> SystemCollections.CACHE_JOIN_DIMENSIONS;
            case JOIN_INDEXES -> SystemCollections.CACHE_JOIN_INDEXES;
            case JOIN_WRITERS -> SystemCollections.CACHE_JOIN_WRITERS;
        };
    }

    @Override public Optional<byte[]> load(String namespace, String key) {
        requireNamespace(namespace); Document id = id(key);
        return transaction(false, session -> Optional.ofNullable(
                table.on(access.database).find(session, new Document("_id", id)).first()).map(row -> decode(row, id)));
    }

    @Override public Map<String, byte[]> loadAll(String namespace, Collection<String> keys) {
        requireNamespace(namespace); var captured = List.copyOf(keys);
        if (captured.isEmpty()) return Map.of();
        return transaction(false, session -> {
            Map<String, byte[]> out = new LinkedHashMap<>(); var batch = new LinkedHashMap<Document, String>(); long size = 0;
            for (String key : captured) {
                Document id = id(key); long bytes = addressBytes + 3L * key.length();
                if (!batch.isEmpty() && (batch.size() == BATCH_KEYS || size + bytes > BATCH_BYTES)) {
                    readBatch(session, batch, out); batch.clear(); size = 0;
                }
                batch.put(id, key); size += bytes;
            }
            readBatch(session, batch, out); return out;
        });
    }

    private void readBatch(ClientSession session, Map<Document, String> ids, Map<String, byte[]> out) {
        if (ids.isEmpty()) return;
        for (Document row : table.on(access.database).find(session, new Document("_id", new Document("$in", List.copyOf(ids.keySet()))))) {
            Object stored = row.get("_id");
            if (!(stored instanceof Document id) || !ids.containsKey(id)) throw access.unreadable("operator-identity");
            out.put(ids.get(id), decode(row, id));
        }
    }

    @Override public void save(String namespace, String key, byte[] state) {
        requireNamespace(namespace); Document id = id(key); byte[] captured = Objects.requireNonNull(state, "state").clone();
        transaction(true, session -> {
            table.on(access.database).replaceOne(session, new Document("_id", id), row(id, captured), new ReplaceOptions().upsert(true)); return null;
        });
    }

    @Override public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] state) {
        requireNamespace(namespace); Document id = id(key); byte[] captured = Objects.requireNonNull(state, "state").clone();
        return transaction(true, session -> {
            Document before = table.on(access.database).find(session, new Document("_id", id)).first();
            if (before != null) return Optional.of(decode(before, id));
            table.on(access.database).insertOne(session, row(id, captured)); return Optional.empty();
        });
    }

    @Override public void delete(String namespace, String key) {
        requireNamespace(namespace); Document id = id(key);
        transaction(true, session -> { table.on(access.database).deleteOne(session, new Document("_id", id)); return null; });
    }

    /** Physical reclamation requires the separate exact retirement path, never an active MapStore. */
    @Override public void dropNamespace(String namespace) { requireNamespace(namespace); throw MongoCachePipelineAccess.claimFenced(); }

    @Override public long count(String namespace) {
        requireNamespace(namespace);
        return transaction(false, session -> table.on(access.database).countDocuments(session, new Document("_id.placement", address)));
    }

    private <T> T transaction(boolean mutation, java.util.function.Function<ClientSession, T> action) {
        return access.metadataTransaction(table, mutation, session -> {
            Document inventory = MongoOperatorInventory.require(session, access.database, placement.scope());
            if (!MongoOperatorInventory.knowsPlacement(inventory, placement)) {
                if (table.on(access.database).find(session, new Document("_id.placement", address)).projection(new Document("_id", 1)).first() != null)
                    throw access.unreadable("operator-inventory");
                if (mutation) MongoOperatorInventory.rememberPlacement(session, access.database, inventory, placement);
            }
            if (placement.resource() == OperatorPlacement.Resource.NEST_STATE) {
                new MongoCacheNestShapeStore(access, placement.database()).requirePath(session, placement);
            }
            return action.apply(session);
        });
    }

    private void requireNamespace(String namespace) { if (!mapName.equals(namespace)) throw MongoCachePipelineAccess.fenced(); }
    private Document id(String key) { return new Document("placement", address).append("key", OperatorStateText.encode(Objects.requireNonNull(key, "key"))); }
    private static Document row(Document id, byte[] value) { return new Document("_id", id).append("formatVersion", 1).append("state", new Binary(value)); }
    private byte[] decode(Document row, Document id) {
        if (!id.equals(row.get("_id")) || !Integer.valueOf(1).equals(row.get("formatVersion"))
                || !(row.get("state") instanceof Binary binary) || binary.getType() != 0) throw access.unreadable("operator-state");
        return binary.getData().clone();
    }
}
