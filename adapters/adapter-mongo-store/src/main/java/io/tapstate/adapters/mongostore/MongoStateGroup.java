package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import io.tapstate.spi.store.OperatorPlacement;
import io.tapstate.spi.store.StateGroup;
import org.bson.Document;
import org.bson.types.Binary;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One vertex instance's commit group over the namespaces it writes, every one on the same pipeline execution
 * and the same database.
 *
 * <p><b>The proof</b> is one transaction: the full validation of every namespace's placement (registering the
 * ones not yet in the inventory), the commit fence placement registered beside them, the claim's liveness and
 * compare-and-set, and the fence rows: every row of the pipeline carrying another tenure rewritten to this
 * group's tenure, and this instance's own row created when it has none. The group holds the proof as a lease,
 * bounded by what remains of the claim's lease and by the configured maximum, and hands the same lease to each
 * namespace's store so its reads stand on it.
 *
 * <p><b>A commit</b> is one transaction of four statements before the payload: the compare-and-set on this
 * instance's own fence row, a read of the claim with its full live filter, a check that the inventory still
 * lists every placement the group writes at no earlier revision than the proof saw, and one bulk write per
 * table. Either fence statement matching nothing refuses the whole group with
 * {@code io.workload-claim-fenced}; any failure ends the lease, so the next commit proves again.
 */
final class MongoStateGroup implements StateGroup {
    /** Group proofs and group commits in this process, for measurement only. */
    static final AtomicLong PROOFS = new AtomicLong();
    static final AtomicLong COMMITS = new AtomicLong();
    /**
     * Runs inside each commit transaction after its payload, before it commits. Nothing but a test sets it: it
     * is how a case holds a commit open across another transaction, which no outside observer can arrange.
     */
    static volatile Runnable afterPayload = () -> {};

    private final String vertexInstance;
    private final Map<String, MongoCacheOperatorStateStore> stores;
    private final MongoCacheOperatorStateStore first;
    private final MongoCachePipelineAccess access;
    private final OperatorPlacement fencePlacement;
    private final Document tenure;
    private final List<Document> locations = new ArrayList<>();
    private final List<MongoCachePipelineAccess.LeasedClock> clocks = new ArrayList<>();
    private Proof proof;

    private record Proof(long deadlineNanos, String preparationId, long inventoryRevision) {}

    MongoStateGroup(String vertexInstance, Map<String, MongoCacheOperatorStateStore> stores) {
        this.vertexInstance = Objects.requireNonNull(vertexInstance, "vertexInstance");
        this.stores = Map.copyOf(stores);
        this.first = stores.values().iterator().next();
        this.access = first.access;
        if (access.originalClaim() == null) throw MongoCachePipelineAccess.fenced();
        for (MongoCacheOperatorStateStore store : stores.values()) {
            if (!access.sameTenure(store.access) || !first.target.getName().equals(store.target.getName())) throw MongoCachePipelineAccess.fenced();
            Document location = MongoOperatorInventory.location(store.placement);
            if (!locations.contains(location)) locations.add(location);
            if (!clocks.contains(store.committed)) clocks.add(store.committed);
        }
        fencePlacement = MongoCommitFence.placement(access.scope, access.database);
        locations.add(MongoOperatorInventory.location(fencePlacement));
        tenure = MongoCommitFence.tenure(access.originalClaim(), access.scope);
    }

    /** Proves the group now, as the class note describes; also the re-proof of a lapsed lease. */
    void prove() {
        long startedAt = System.nanoTime();
        long maximum = MongoCacheOperatorStateStore.maximumLeaseNanos();
        Proof proved;
        try {
            proved = access.metadataTransaction(first.table, first.target, true, session -> {
                Document inventory = MongoOperatorInventory.require(session, access.database, access.scope);
                for (MongoCacheOperatorStateStore store : stores.values()) {
                    store.access.requireActive();
                    new MongoCacheMetadataStructure(store.target, access.rateRetention).verifyFacet(store.table);
                    inventory = store.admitPlacement(session, inventory, true);
                }
                new MongoCacheMetadataStructure(access.database, access.rateRetention).verifyFacet(SystemCollections.CACHE_NEST_STATE);
                if (!MongoOperatorInventory.knowsPlacement(inventory, fencePlacement)) {
                    MongoOperatorInventory.rememberPlacement(session, access.database, inventory, fencePlacement);
                    inventory = MongoOperatorInventory.require(session, access.database, access.scope);
                }
                long held = Math.min(maximum, TimeUnit.MILLISECONDS.toNanos(access.proveLease(session)));
                MongoCommitFence.adopt(session, access.database, access.scope, vertexInstance, tenure);
                return new Proof(startedAt + held, inventory.getString("preparationId"), inventory.getLong("revision"));
            });
        } catch (RuntimeException failure) {
            lose();
            throw failure;
        }
        proof = proved;
        for (MongoCacheOperatorStateStore store : stores.values()) store.adoptLease(proved.deadlineNanos(), proved.preparationId(), proved.inventoryRevision());
        PROOFS.incrementAndGet();
        MongoCacheOperatorStateStore.LEASE_OPENS.incrementAndGet();
    }

    @Override public void commit(Map<String, Changes> changes) {
        Objects.requireNonNull(changes, "changes");
        for (String namespace : changes.keySet()) if (!stores.containsKey(namespace)) throw MongoCachePipelineAccess.fenced();
        if (changes.values().stream().allMatch(Changes::isEmpty)) return;
        Map<String, Map.Entry<MongoCollection<Document>, List<WriteModel<Document>>>> byTable = new LinkedHashMap<>();
        changes.forEach((namespace, part) -> {
            if (part.isEmpty()) return;
            MongoCacheOperatorStateStore store = stores.get(namespace);
            MongoCollection<Document> collection = store.table.on(store.target);
            var models = byTable.computeIfAbsent(collection.getNamespace().getFullName(), name -> Map.entry(collection, new ArrayList<>())).getValue();
            part.writes().forEach((key, value) -> {
                Document id = store.id(key);
                Document row = store.row(id, value);
                byte[] release = part.releaseKeys().get(key);
                if (release != null) {
                    if (store.table != SystemCollections.CACHE_NEST_STATE) throw MongoCachePipelineAccess.fenced();
                    row.append("pendingRelease", true).append("releaseKey", new Binary(release));
                }
                models.add(new ReplaceOneModel<>(new Document("_id", id), row, new ReplaceOptions().upsert(true)));
            });
            for (String key : part.removals()) models.add(new DeleteOneModel<>(new Document("_id", store.id(key))));
        });
        Proof current = proof;
        if (current == null || System.nanoTime() - current.deadlineNanos() >= 0) {
            prove();
            current = proof;
        }
        Proof proved = current;
        try {
            access.groupWrite(clocks, session -> {
                for (MongoCacheOperatorStateStore store : stores.values()) store.access.requireActive();
                if (!MongoCommitFence.fence(session, access.database, access.scope, vertexInstance, tenure) || !access.claimLive(session))
                    throw MongoCachePipelineAccess.claimFenced();
                requireInventory(session, proved);
                for (var table : byTable.values()) table.getKey().bulkWrite(session, table.getValue(), new BulkWriteOptions().ordered(true));
                afterPayload.run();
                return null;
            });
        } catch (RuntimeException failure) {
            lose();
            throw failure;
        }
        COMMITS.incrementAndGet();
    }

    /**
     * The inventory the group may still rely on: the same preparation, not retired, not behind the revision
     * the proof saw, and listing every placement the group writes, its fence placement included.
     */
    private void requireInventory(ClientSession session, Proof proved) {
        var filter = new Document("_id", MongoPipelinePreparationFacts.scope(access.scope))
                .append("preparationId", proved.preparationId()).append("revision", new Document("$gte", proved.inventoryRevision()))
                .append("placements", new Document("$all", locations)).append("retirement", new Document("$exists", false));
        if (SystemCollections.CACHE_OPERATOR_CLEANUP.on(access.database).find(session, filter).projection(new Document("_id", 1)).first() == null)
            throw access.unreadable("operator-inventory");
    }

    private void lose() {
        proof = null;
        for (MongoCacheOperatorStateStore store : stores.values()) store.loseLease();
    }
}
