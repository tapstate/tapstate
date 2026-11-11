package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.WriteModel;
import java.util.ArrayList;
import io.tapstate.spi.store.LeasedStateStore;
import io.tapstate.spi.store.NestReleaseStore;
import io.tapstate.spi.store.OperatorPlacement;
import io.tapstate.spi.store.OrderedStateStore;
import org.bson.Document;
import org.bson.types.Binary;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Opaque operator bytes, split by resource type and fenced by the original Pipeline execution.
 *
 * <p>By default the store validates once per lease. A lease is proved by one transaction that runs the
 * full validation (admission, state and target structure, inventory and placement, nest shape, prepared
 * claim) plus the liveness of the original claim, and that registers the placement before the first
 * write. Within a lease, every write is one transaction holding one compare-and-set on the live claim, a
 * check that the inventory has not gone back behind the revision the lease proved, and the payload; reads
 * leave transactions. A lease ends when the claim's own lease, as the store counted it at the proof, or the
 * configured maximum has run out, when the member's claim reading is lost ({@link #dropLease()}), and after
 * any failed operation, so the next operation proves again and a lost claim fails closed for reads and
 * writes alike. A structure change made underneath a running lease is seen at the next proof.
 *
 * <p>Reads within a lease are not fenced: a member that has lost its claim may still read until its next
 * proof, and can never write.
 */
final class MongoCacheOperatorStateStore implements NestReleaseStore, LeasedStateStore, OrderedStateStore {
    private static final String STATE_FIELD = "state";
    private static final String FORMAT_VERSION_FIELD = "formatVersion";
    private static final int BATCH_KEYS = 256;
    private static final int BATCH_BYTES = 4 * 1024 * 1024;
    private final MongoCachePipelineAccess access;
    private final com.mongodb.client.MongoDatabase target;
    private final OperatorPlacement placement;
    private final SystemCollections table;
    private final Document address;
    private final String mapName;
    private final long addressBytes;
    private final Validation validation;
    private final AtomicReference<Lease> lease = new AtomicReference<>();
    private final Object proving = new Object();
    private final MongoCachePipelineAccess.LeasedClock committed = new MongoCachePipelineAccess.LeasedClock();

    /**
     * Selects how often this store validates. {@code transaction} restores per-transaction validation for one
     * release, as the rollback switch; any other value, and no value, selects the lease.
     */
    static final String VALIDATION_PROPERTY = "tapstate.state.validation";
    /** The longest one lease stands before the next operation proves again, in milliseconds. */
    static final String LEASE_PROPERTY = "tapstate.state.lease.max-millis";
    static final long DEFAULT_LEASE_MILLIS = 10_000L;
    /** Lease proofs and lost leases across every store in this process, for measurement only. */
    static final AtomicLong LEASE_OPENS = new AtomicLong();
    static final AtomicLong LEASE_LOSSES = new AtomicLong();

    enum Validation {
        TRANSACTION, LEASE;

        static Validation selected() {
            return "transaction".equals(System.getProperty(VALIDATION_PROPERTY)) ? TRANSACTION : LEASE;
        }
    }

    /**
     * One proved lease: until when it stands on this process's monotonic clock, whether it registered the
     * placement and proved the claim for writes, and the inventory it was proved under.
     */
    private record Lease(long deadlineNanos, boolean writes, String preparationId, long inventoryRevision) {}

    MongoCacheOperatorStateStore(MongoCachePipelineAccess access, OperatorPlacement placement) {
        this.access = Objects.requireNonNull(access, "access"); this.placement = Objects.requireNonNull(placement, "placement");
        if (!access.scope.equals(placement.scope())) throw MongoCachePipelineAccess.fenced();
        target = access.operatorDatabase(placement.database(), placement.resource());
        table = table(placement.resource()); mapName = placement.mapName();
        address = new Document("scope", MongoPipelinePreparationFacts.scope(placement.scope()))
                .append("database", placement.database()).append("resource", placement.resource().name())
                .append("stepId", OperatorStateText.encode(placement.stepId())).append("pathId", OperatorStateText.encode(placement.pathId())).append("namespace", OperatorStateText.encode(placement.namespace()));
        addressBytes = 1024L + 3L * address.toJson().length();
        validation = Validation.selected();
    }

    static SystemCollections table(OperatorPlacement.Resource resource) {
        return switch (resource) {
            case NEST_STATE -> SystemCollections.CACHE_NEST_STATE;
            case JOIN_FACTS -> SystemCollections.CACHE_JOIN_FACTS;
            case JOIN_DIMENSIONS -> SystemCollections.CACHE_JOIN_DIMENSIONS;
            case JOIN_INDEXES -> SystemCollections.CACHE_JOIN_INDEXES;
            case JOIN_WRITERS -> SystemCollections.CACHE_JOIN_WRITERS;
            case AGGREGATE_STATE -> SystemCollections.CACHE_AGGREGATE_STATE;
        };
    }

    @Override public Optional<byte[]> load(String namespace, String key) {
        requireNamespace(namespace); Document id = id(key);
        return read(session -> Optional.ofNullable(
                rows().find(session, new Document("_id", id)).first()).map(row -> decode(row, id)));
    }

    @Override public Map<String, byte[]> loadAll(String namespace, Collection<String> keys) {
        requireNamespace(namespace); var captured = List.copyOf(keys);
        if (captured.isEmpty()) return Map.of();
        return read(session -> {
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
        for (Document row : rows().find(session, new Document("_id", new Document("$in", List.copyOf(ids.keySet()))))) {
            Object stored = row.get("_id");
            if (!(stored instanceof Document id) || !ids.containsKey(id)) throw access.unreadable("operator-identity");
            out.put(ids.get(id), decode(row, id));
        }
    }

    @Override public void save(String namespace, String key, byte[] state) {
        saveWithRelease(namespace, key, state, null);
    }

    @Override public void saveAll(String namespace, Map<String, byte[]> states) {
        requireNamespace(namespace);
        List<WriteModel<Document>> batch = new ArrayList<>();
        long bytes = 0;
        for (var entry : states.entrySet()) {
            Document id = id(entry.getKey());
            byte[] captured = Objects.requireNonNull(entry.getValue(), STATE_FIELD).clone();
            long weight = 2 * (addressBytes + 3L * entry.getKey().length()) + captured.length;
            if (!batch.isEmpty() && (batch.size() == BATCH_KEYS || bytes + weight > BATCH_BYTES)) {
                writeBatch(batch);
                batch.clear();
                bytes = 0;
            }
            batch.add(new ReplaceOneModel<>(new Document("_id", id), row(id, captured), new ReplaceOptions().upsert(true)));
            bytes += weight;
        }
        writeBatch(batch);
    }

    private void writeBatch(List<WriteModel<Document>> batch) {
        if (batch.isEmpty()) return;
        write(session -> {
            table.on(target).bulkWrite(session, batch, new BulkWriteOptions().ordered(true));
            return null;
        });
    }

    @Override public void saveWithRelease(String namespace, String key, byte[] state, byte[] routingKey) {
        requireNamespace(namespace); Document id = id(key);
        byte[] captured = Objects.requireNonNull(state, STATE_FIELD).clone();
        byte[] fixedKey = routingKey == null ? null : routingKey.clone();
        if (fixedKey != null && table != SystemCollections.CACHE_NEST_STATE) throw MongoCachePipelineAccess.fenced();
        write(session -> {
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
                    .sort(new Document("_id.key", 1)).projection(new Document("_id", 1).append("releaseKey", 1).append(FORMAT_VERSION_FIELD, 1))
                    .limit(MAX_PAGE_KEYS).batchSize(1).iterator()) {
                while (cursor.hasNext()) {
                    var value = cursor.next();
                    if (!(value.get("_id") instanceof Document id) || !id.keySet().equals(java.util.Set.of("placement","key"))
                            || !Integer.valueOf(2).equals(value.get(FORMAT_VERSION_FIELD)) || !address.equals(id.get("placement"))
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
        requireNamespace(namespace); Document id = id(key);
        byte[] captured = Objects.requireNonNull(state, STATE_FIELD).clone();
        return write(session -> {
            Document before = table.on(target).find(session, new Document("_id", id)).first();
            if (before != null) return Optional.of(decode(before, id));
            table.on(target).insertOne(session, row(id, captured)); return Optional.empty();
        });
    }

    @Override public void delete(String namespace, String key) {
        requireNamespace(namespace); Document id = id(key);
        write(session -> { table.on(target).deleteOne(session, new Document("_id", id)); return null; });
    }

    /** Physical reclamation requires the separate exact retirement path, never an active MapStore. */
    @Override public void dropNamespace(String namespace) { requireNamespace(namespace); throw MongoCachePipelineAccess.claimFenced(); }

    @Override public long count(String namespace) {
        requireNamespace(namespace);
        return transaction(false, session -> table.on(target).countDocuments(session, new Document("_id.placement", address)));
    }

    @Override public void dropLease() { lease.set(ended()); }

    /**
     * A fresh, already expired marker that every way of ending a lease installs, never {@code null}: a proof in
     * flight publishes only over the exact lease it started from, so any end at any point of that proof wins,
     * and no two ends can ever look like the same lease.
     */
    private static Lease ended() { return new Lease(System.nanoTime(), false, null, 0L); }

    /**
     * Proves a lease now, as an explicit open: for a store holding a claim this registers its placement and
     * proves the claim for writes, so none of that bookkeeping is left to the first write. Per-transaction
     * mode validates every call instead, and opening is then a no-op.
     */
    void openLease() {
        if (validation == Validation.LEASE) prove(access.originalClaim() != null);
    }

    private <T> T read(Function<ClientSession, T> action) {
        if (validation == Validation.TRANSACTION) return transaction(false, action);
        return leased(false, proved -> access.leasedRead(committed, action));
    }

    private <T> T write(Function<ClientSession, T> action) {
        if (validation == Validation.TRANSACTION) return transaction(true, action);
        return leased(true, proved -> access.leasedWrite(committed, session -> {
            requireProvedInventory(session, proved);
            return action.apply(session);
        }));
    }

    /** Reads under a lease see only majority-committed state, because they are not inside a snapshot transaction. */
    private com.mongodb.client.MongoCollection<Document> rows() {
        var rows = table.on(target);
        return validation == Validation.TRANSACTION ? rows : rows.withReadConcern(com.mongodb.ReadConcern.MAJORITY);
    }

    /**
     * Runs one operation under a standing lease, proving one first when none stands, it has run out, or a
     * write meets a lease proved only for reads. Whatever the operation throws ends the lease.
     */
    private <T> T leased(boolean writes, Function<Lease, T> operation) {
        Lease current = lease.get();
        if (current == null || System.nanoTime() - current.deadlineNanos() >= 0 || (writes && !current.writes())) current = prove(writes);
        try {
            return operation.apply(current);
        } catch (RuntimeException failure) {
            if (lease.compareAndSet(current, ended())) LEASE_LOSSES.incrementAndGet();
            throw failure;
        }
    }

    /**
     * The lease proof: today's full validation in one transaction, and for writes the original claim's
     * liveness, which also registers the placement and bounds the lease by what remains of the claim. Reads
     * prove identity and structure only, as they always did: a member that no longer holds the claim may
     * still read the state it wrote, and only its writes are refused.
     */
    private Lease prove(boolean writes) {
        synchronized (proving) {
            Lease standing = lease.get();
            if (standing != null && System.nanoTime() - standing.deadlineNanos() < 0 && (standing.writes() || !writes)) return standing;
            long startedAt = System.nanoTime();
            long maximum = TimeUnit.MILLISECONDS.toNanos(Math.max(1L, Long.getLong(LEASE_PROPERTY, DEFAULT_LEASE_MILLIS)));
            boolean claimed = access.originalClaim() != null;
            Lease before = lease.get();
            Lease proved;
            try {
                proved = validated(writes, (session, inventory) -> {
                    long held = claimed && writes ? Math.min(maximum, TimeUnit.MILLISECONDS.toNanos(access.proveLease(session))) : maximum;
                    return new Lease(startedAt + held, writes, inventory.getString("preparationId"), inventory.getLong("revision"));
                });
            } catch (RuntimeException failure) {
                // A failed proof also ends a standing read lease, which an upgrade to a write would otherwise leave behind.
                lease.set(ended());
                throw failure;
            }
            LEASE_OPENS.incrementAndGet();
            // Published only over the exact lease this proof started from. A proof that ran out while validating,
            // or whose starting lease a drop replaced meanwhile, serves only the operation it was proved for and
            // is never visible to another one, which proves again.
            if (System.nanoTime() - proved.deadlineNanos() < 0) lease.compareAndSet(before, proved);
            return proved;
        }
    }

    /**
     * The inventory a write may still rely on: the same preparation, not retired, not behind the revision the
     * lease proved, and still listing this placement. Inventories only grow while a preparation lives, so an
     * older legal copy restored underneath the lease is the one way to fall behind; a sibling registering
     * its own placement moves the revision forward and is no reason to refuse.
     */
    private void requireProvedInventory(ClientSession session, Lease proved) {
        var filter = new Document("_id", MongoPipelinePreparationFacts.scope(placement.scope()))
                .append("preparationId", proved.preparationId()).append("revision", new Document("$gte", proved.inventoryRevision()))
                .append("placements", MongoOperatorInventory.location(placement)).append("retirement", new Document("$exists", false));
        if (SystemCollections.CACHE_OPERATOR_CLEANUP.on(access.database).find(session, filter).projection(new Document("_id", 1)).first() == null)
            throw access.unreadable("operator-inventory");
    }

    /** A fully validated call; what it finds wrong also ends the lease, so the next leased call proves again. */
    private <T> T transaction(boolean mutation, Function<ClientSession, T> action) {
        try {
            return validated(mutation, (session, inventory) -> action.apply(session));
        } catch (RuntimeException failure) {
            lease.set(ended());
            throw failure;
        }
    }

    /** One fully validated transaction; the action also sees the inventory root as this transaction left it. */
    private <T> T validated(boolean mutation, java.util.function.BiFunction<ClientSession, Document, T> action) {
        return access.metadataTransaction(table, target, mutation, session -> {
            Document inventory = MongoOperatorInventory.require(session, access.database, placement.scope());
            if (!MongoOperatorInventory.knowsPlacement(inventory, placement)) {
                if (table.on(target).find(session, new Document("_id.placement", address)).projection(new Document("_id", 1)).first() != null)
                    throw access.unreadable("operator-inventory");
                if (mutation) {
                    MongoOperatorInventory.rememberPlacement(session, access.database, inventory, placement);
                    inventory = MongoOperatorInventory.require(session, access.database, placement.scope());
                }
            }
            if (placement.resource() == OperatorPlacement.Resource.NEST_STATE) {
                new MongoCacheNestShapeStore(access, placement.database()).requirePath(session, placement);
            }
            return action.apply(session, inventory);
        });
    }

    /** The keyed operations serve every resource but the ordered one, whose keys are stored in another form. */
    private void requireNamespace(String namespace) {
        if (!mapName.equals(namespace) || table == SystemCollections.CACHE_AGGREGATE_STATE) throw MongoCachePipelineAccess.fenced();
    }

    // ---- ordered entries: whole commits and the first key of a range, for the aggregate resource only ----

    /** What one commit may carry: the batch bound of every other write, with room left for ids and framing. */
    private static final OrderedStateStore.Limit ORDERED_COMMIT = new OrderedStateStore.Limit(BATCH_KEYS, BATCH_BYTES / 2);

    @Override public OrderedStateStore.Limit commitLimit() { return ORDERED_COMMIT; }

    @Override public List<byte[]> loadAll(List<byte[]> keys) {
        requireOrdered();
        List<Document> ids = new ArrayList<>(keys.size());
        for (byte[] key : keys) ids.add(orderedId(key));
        if (ids.isEmpty()) return List.of();
        return read(session -> {
            Map<Document, byte[]> found = new java.util.HashMap<>();
            for (int from = 0; from < ids.size(); from += BATCH_KEYS) {
                List<Document> batch = ids.subList(from, Math.min(ids.size(), from + BATCH_KEYS));
                for (Document row : rows().find(session, new Document("_id", new Document("$in", batch)))) {
                    if (!(row.get("_id") instanceof Document id)) throw access.unreadable("operator-identity");
                    found.put(id, orderedState(row));
                }
            }
            List<byte[]> states = new ArrayList<>(ids.size());
            for (Document id : ids) states.add(found.get(id));
            return java.util.Collections.unmodifiableList(states);
        });
    }

    @Override public void commit(List<OrderedStateStore.Entry> saves, List<byte[]> deletes) {
        requireOrdered();
        List<WriteModel<Document>> batch = new ArrayList<>(saves.size() + deletes.size());
        java.util.Set<String> named = new java.util.HashSet<>();
        long bytes = 0;
        for (OrderedStateStore.Entry save : saves) {
            Document id = orderedId(save.key());
            if (!named.add(id.getString("key"))) throw new IllegalArgumentException("a key appears twice in one commit");
            bytes += save.key().length + save.state().length;
            batch.add(new ReplaceOneModel<>(new Document("_id", id), new Document("_id", id).append(FORMAT_VERSION_FIELD, 1)
                    .append(STATE_FIELD, new Binary(save.state().clone())), new ReplaceOptions().upsert(true)));
        }
        for (byte[] delete : deletes) {
            Document id = orderedId(delete);
            if (!named.add(id.getString("key"))) throw new IllegalArgumentException("a key appears twice in one commit");
            bytes += delete.length;
            batch.add(new com.mongodb.client.model.DeleteOneModel<>(new Document("_id", id)));
        }
        if (batch.size() > ORDERED_COMMIT.operations() || bytes > ORDERED_COMMIT.bytes())
            throw new IllegalArgumentException("a commit of " + batch.size() + " operations and " + bytes + " bytes is beyond " + ORDERED_COMMIT);
        if (batch.isEmpty()) return;
        // One transaction: the compare-and-set on the live claim, the inventory check, and every save and delete.
        write(session -> { table.on(target).bulkWrite(session, batch, new BulkWriteOptions().ordered(true)); return null; });
    }

    @Override public Optional<OrderedStateStore.Entry> first(byte[] from, byte[] to, OrderedStateStore.End end) {
        requireOrdered();
        Document range = new Document("$gte", orderedId(from)).append("$lt", orderedId(to));
        return read(session -> {
            Document row = rows().find(session, new Document("_id", range))
                    .sort(new Document("_id", end == OrderedStateStore.End.LOWEST ? 1 : -1)).limit(1).first();
            return row == null ? Optional.empty() : Optional.of(orderedEntry(row));
        });
    }

    @Override public List<OrderedStateStore.Entry> scan(byte[] from, byte[] to, byte[] after, int limit) {
        requireOrdered();
        if (limit < 1 || limit > MAX_PAGE_KEYS) throw new IllegalArgumentException("a page holds 1 to " + MAX_PAGE_KEYS + " entries");
        Document range = (after == null ? new Document("$gte", orderedId(from)) : new Document("$gt", orderedId(after))).append("$lt", orderedId(to));
        return read(session -> {
            List<OrderedStateStore.Entry> page = new ArrayList<>();
            for (Document row : rows().find(session, new Document("_id", range)).sort(new Document("_id", 1)).limit(limit)) page.add(orderedEntry(row));
            return List.copyOf(page);
        });
    }

    private void requireOrdered() { if (table != SystemCollections.CACHE_AGGREGATE_STATE) throw MongoCachePipelineAccess.fenced(); }

    /**
     * An id whose key is the lowercase hexadecimal of the caller's bytes. Ids of one placement then compare
     * as their keys do, byte by byte, so a range of keys is a range of the id index and needs no other.
     */
    private Document orderedId(byte[] key) {
        return new Document("placement", address).append("key", java.util.HexFormat.of().formatHex(Objects.requireNonNull(key, "key")));
    }

    private byte[] orderedState(Document row) {
        if (!Integer.valueOf(1).equals(row.get(FORMAT_VERSION_FIELD)) || !(row.get(STATE_FIELD) instanceof Binary binary)
                || binary.getType() != 0) throw access.unreadable("operator-state");
        return binary.getData().clone();
    }

    private OrderedStateStore.Entry orderedEntry(Document row) {
        if (!(row.get("_id") instanceof Document id) || !id.keySet().equals(java.util.Set.of("placement", "key"))
                || !address.equals(id.get("placement")) || !(id.get("key") instanceof String hex)) throw access.unreadable("operator-identity");
        byte[] key;
        try { key = java.util.HexFormat.of().parseHex(hex); }
        catch (IllegalArgumentException invalid) { throw access.unreadable("operator-identity"); }
        if (!java.util.HexFormat.of().formatHex(key).equals(hex)) throw access.unreadable("operator-identity");
        return new OrderedStateStore.Entry(key, orderedState(row));
    }
    private Document id(String key) { return new Document("placement", address).append("key", OperatorStateText.encode(Objects.requireNonNull(key, "key"))); }
    private Document row(Document id, byte[] value) { return new Document("_id", id).append(FORMAT_VERSION_FIELD, table == SystemCollections.CACHE_NEST_STATE ? 2 : 1).append(STATE_FIELD, new Binary(value)); }
    private byte[] decode(Document row, Document id) {
        if (!id.equals(row.get("_id")) || !Integer.valueOf(table == SystemCollections.CACHE_NEST_STATE ? 2 : 1).equals(row.get(FORMAT_VERSION_FIELD))
                || !(row.get(STATE_FIELD) instanceof Binary binary) || binary.getType() != 0) throw access.unreadable("operator-state");
        if (row.containsKey("pendingRelease") || row.containsKey("releaseKey")) {
            if (table != SystemCollections.CACHE_NEST_STATE || !Boolean.TRUE.equals(row.get("pendingRelease"))
                    || !(row.get("releaseKey") instanceof Binary key) || key.getType() != 0)
                throw access.unreadable("operator-release-key");
        }
        return binary.getData().clone();
    }
}
