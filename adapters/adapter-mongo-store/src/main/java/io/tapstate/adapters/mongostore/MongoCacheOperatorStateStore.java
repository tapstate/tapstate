package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.ReplaceOptions;
import io.tapstate.spi.store.NestReleaseStore;
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
final class MongoCacheOperatorStateStore implements NestReleaseStore {
    private static final int BATCH_KEYS = 256;
    private static final int BATCH_BYTES = 4 * 1024 * 1024;
    private final MongoCachePipelineAccess access;
    private final com.mongodb.client.MongoDatabase target;
    private final OperatorPlacement placement;
    private final SystemCollections table;
    private final Document address;
    private final String mapName;
    private final long addressBytes;

    MongoCacheOperatorStateStore(MongoCachePipelineAccess access, OperatorPlacement placement) {
        this.access = Objects.requireNonNull(access, "access"); this.placement = Objects.requireNonNull(placement, "placement");
        if (!access.scope.equals(placement.scope())) throw MongoCachePipelineAccess.fenced();
        target = access.operatorDatabase(placement.database(), placement.resource());
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
                table.on(target).find(session, new Document("_id", id)).first()).map(row -> decode(row, id)));
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
        for (Document row : table.on(target).find(session, new Document("_id", new Document("$in", List.copyOf(ids.keySet()))))) {
            Object stored = row.get("_id");
            if (!(stored instanceof Document id) || !ids.containsKey(id)) throw access.unreadable("operator-identity");
            out.put(ids.get(id), decode(row, id));
        }
    }

    @Override public void save(String namespace, String key, byte[] state) {
        saveWithRelease(namespace, key, state, null);
    }

    @Override public void saveWithRelease(String namespace, String key, byte[] state, byte[] routingKey) {
        requireNamespace(namespace); Document id = id(key); byte[] captured = Objects.requireNonNull(state, "state").clone();
        byte[] fixedKey = routingKey == null ? null : routingKey.clone();
        if (fixedKey != null && table != SystemCollections.CACHE_NEST_STATE) throw MongoCachePipelineAccess.fenced();
        transaction(true, session -> {
            var value = row(id, captured);
            if (fixedKey != null) value.append("pendingRelease", true).append("releaseKey", new Binary(fixedKey));
            table.on(target).replaceOne(session, new Document("_id", id), value, new ReplaceOptions().upsert(true)); return null;
        });
    }

    @Override public List<PendingKey> pendingReleases(String namespace, String afterKey) {
        requireNamespace(namespace);
        if (table != SystemCollections.CACHE_NEST_STATE) throw MongoCachePipelineAccess.fenced();
        return transaction(false, session -> {
            access.verifyOriginalClaim(session);
            var filter = new Document("_id.placement", address).append("pendingRelease", true);
            if (afterKey != null) filter.append("_id.key", new Document("$gt", OperatorStateText.encode(afterKey)));
            var found = new java.util.ArrayList<PendingKey>(); long bytes = 0;
            try (var cursor = table.on(target).find(session, filter)
                    .hintString(SystemCollections.CACHE_NEST_STATE.indexes().get(1).indexName())
                    .sort(new Document("_id.key", 1)).projection(new Document("_id", 1).append("releaseKey", 1).append("formatVersion", 1))
                    .limit(MAX_PAGE_KEYS).batchSize(1).iterator()) {
                while (cursor.hasNext()) {
                    var value = cursor.next();
                    if (!(value.get("_id") instanceof Document id) || !id.keySet().equals(java.util.Set.of("placement","key"))
                            || !Integer.valueOf(2).equals(value.get("formatVersion")) || !address.equals(id.get("placement"))
                            || !(id.get("key") instanceof String encoded) || !(value.get("releaseKey") instanceof Binary key)
                            || key.getType() != 0) throw access.unreadable("operator-release-key");
                    long held = addressBytes + 2L * encoded.length() + key.getData().length;
                    if (!found.isEmpty() && bytes + held > BATCH_BYTES) break;
                    String decoded;
                    try { decoded = OperatorStateText.decode(encoded); }
                    catch (IllegalArgumentException invalid) { throw access.unreadable("operator-release-key"); }
                    found.add(new PendingKey(decoded, key.getData())); bytes += held;
                }
            }
            return List.copyOf(found);
        });
    }

    @Override public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] state) {
        requireNamespace(namespace); Document id = id(key); byte[] captured = Objects.requireNonNull(state, "state").clone();
        return transaction(true, session -> {
            Document before = table.on(target).find(session, new Document("_id", id)).first();
            if (before != null) return Optional.of(decode(before, id));
            table.on(target).insertOne(session, row(id, captured)); return Optional.empty();
        });
    }

    @Override public void delete(String namespace, String key) {
        requireNamespace(namespace); Document id = id(key);
        transaction(true, session -> { table.on(target).deleteOne(session, new Document("_id", id)); return null; });
    }

    /** Physical reclamation requires the separate exact retirement path, never an active MapStore. */
    @Override public void dropNamespace(String namespace) { requireNamespace(namespace); throw MongoCachePipelineAccess.claimFenced(); }

    @Override public long count(String namespace) {
        requireNamespace(namespace);
        return transaction(false, session -> table.on(target).countDocuments(session, new Document("_id.placement", address)));
    }

    private <T> T transaction(boolean mutation, java.util.function.Function<ClientSession, T> action) {
        return access.metadataTransaction(table, target, mutation, session -> {
            Document inventory = MongoOperatorInventory.require(session, access.database, placement.scope());
            if (!MongoOperatorInventory.knowsPlacement(inventory, placement)) {
                if (table.on(target).find(session, new Document("_id.placement", address)).projection(new Document("_id", 1)).first() != null)
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
    private Document row(Document id, byte[] value) { return new Document("_id", id).append("formatVersion", table == SystemCollections.CACHE_NEST_STATE ? 2 : 1).append("state", new Binary(value)); }
    private byte[] decode(Document row, Document id) {
        if (!id.equals(row.get("_id")) || !Integer.valueOf(table == SystemCollections.CACHE_NEST_STATE ? 2 : 1).equals(row.get("formatVersion"))
                || !(row.get("state") instanceof Binary binary) || binary.getType() != 0) throw access.unreadable("operator-state");
        if (row.containsKey("pendingRelease") || row.containsKey("releaseKey")) {
            if (table != SystemCollections.CACHE_NEST_STATE || !Boolean.TRUE.equals(row.get("pendingRelease"))
                    || !(row.get("releaseKey") instanceof Binary key) || key.getType() != 0)
                throw access.unreadable("operator-release-key");
        }
        return binary.getData().clone();
    }
}
