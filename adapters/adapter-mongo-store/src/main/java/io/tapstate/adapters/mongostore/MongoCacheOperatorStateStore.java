package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.ReplaceOptions;
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
final class MongoCacheOperatorStateStore implements io.tapstate.spi.store.NestStateStore {
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
        saveNest(namespace, key, state, null);
    }

    @Override public void saveNest(String namespace, String key, byte[] state, byte[] pendingKey) {
        requireNamespace(namespace); Document id = id(key); byte[] captured = Objects.requireNonNull(state, "state").clone();
        byte[] pending = pendingKey == null ? null : pendingKey.clone();
        if (pending != null && placement.resource() != OperatorPlacement.Resource.NEST_STATE) throw MongoCachePipelineAccess.fenced();
        transaction(true, session -> {
            table.on(target).replaceOne(session, new Document("_id", id), row(id, captured, pending), new ReplaceOptions().upsert(true)); return null;
        });
    }

    @Override public List<Pending> pending(String namespace, String after, int limit) {
        requireNamespace(namespace);
        if (placement.resource() != OperatorPlacement.Resource.NEST_STATE) throw MongoCachePipelineAccess.fenced();
        if (limit < 1 || limit > BATCH_KEYS) throw new IllegalArgumentException("invalid pending page limit");
        // A page is authorized by the exact original claim inside the same transaction as the read.
        return transaction(false, session -> {
            access.proveClaim(session);
            var filter = new Document("_id.placement", address).append("pending", true);
            if (after != null) filter.append("_id.key", new Document("$gt", after));
            var found = new java.util.ArrayList<Pending>();
            for (Document value : table.on(target).find(session, filter)
                    .projection(new Document("_id", 1).append("formatVersion", 1).append("pendingKey", 1))
                    .hintString("pending_handoffs").sort(new Document("_id.key", 1)).limit(limit)) {
                if (!(value.get("_id") instanceof Document stored) || !address.equals(stored.get("placement"))
                        || !(stored.get("key") instanceof String cursor)
                        || !Integer.valueOf(2).equals(value.get("formatVersion"))
                        || !(value.get("pendingKey") instanceof Binary key) || key.getType() != 0)
                    throw access.unreadable("nest-pending");
                found.add(new Pending(cursor, key.getData()));
            }
            return List.copyOf(found);
        });
    }

    @Override public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] state) {
        requireNamespace(namespace); Document id = id(key); byte[] captured = Objects.requireNonNull(state, "state").clone();
        return transaction(true, session -> {
            Document before = table.on(target).find(session, new Document("_id", id)).first();
            if (before != null) return Optional.of(decode(before, id));
            table.on(target).insertOne(session, row(id, captured, null)); return Optional.empty();
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
    private Document row(Document id, byte[] value, byte[] pendingKey) {
        var row = new Document("_id", id).append("formatVersion", format()).append("state", new Binary(value));
        if (placement.resource() == OperatorPlacement.Resource.NEST_STATE) {
            row.append("pending", pendingKey != null);
            if (pendingKey != null) row.append("pendingKey", new Binary(pendingKey));
        }
        return row;
    }
    private int format() { return placement.resource() == OperatorPlacement.Resource.NEST_STATE ? 2 : 1; }
    private byte[] decode(Document row, Document id) {
        if (!id.equals(row.get("_id")) || !Integer.valueOf(format()).equals(row.get("formatVersion"))
                || !(row.get("state") instanceof Binary binary) || binary.getType() != 0) throw access.unreadable("operator-state");
        if (placement.resource() == OperatorPlacement.Resource.NEST_STATE
                && (!(row.get("pending") instanceof Boolean pending)
                || (pending ? !(row.get("pendingKey") instanceof Binary key) || key.getType() != 0
                            : row.containsKey("pendingKey")))) throw access.unreadable("nest-pending");
        return binary.getData().clone();
    }
}
