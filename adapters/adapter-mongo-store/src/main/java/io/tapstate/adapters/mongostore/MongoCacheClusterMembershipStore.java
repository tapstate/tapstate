package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndReplaceOptions;
import com.mongodb.client.model.ReturnDocument;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.ClusterMembershipStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimType;
import org.bson.Document;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Known genesis and committed topology are distinct from missing membership history. */
final class MongoCacheClusterMembershipStore implements ClusterMembershipStore {
    // Leave time for the driver to abort a timed-out read before the HTTP caller gives up.
    private static final Duration READ_BUDGET = Duration.ofSeconds(5);
    private static final String GENESIS = "GENESIS";
    private static final String COMMITTED = "COMMITTED";
    private static final String NODE_WRITES = "fencedMembershipWrites";
    private final MongoDatabase database;
    private final CacheBusinessScope scope;
    private final WorkloadClaimFence node;
    private final MongoCacheMetadataAccess access;

    MongoCacheClusterMembershipStore(MongoClient client, MongoDatabase database, CacheBusinessScope scope, WorkloadClaimFence node, Runnable active) {
        this.database = Objects.requireNonNull(database, "database"); this.scope = Objects.requireNonNull(scope, "scope"); this.node = node;
        if (node != null && (node.key().type() != WorkloadClaimType.NODE_SESSION || !scope.clusterId().equals(node.key().clusterId())
                || !node.key().resourceId().equals(node.owner().nodeId()))) throw fenced();
        this.access = new MongoCacheMetadataAccess(client, database, scope, SystemCollections.CACHE_CLUSTER_MEMBERSHIP, active);
    }

    /** Only the original whole-business preparation transaction may allocate this pending fact. */
    static void prepareInitial(ClientSession session, MongoDatabase database, CacheBusinessScope scope, String operationId) {
        if (!session.hasActiveTransaction()) throw new IllegalStateException("membership preparation requires its owner's transaction");
        Document root = SystemCollections.CACHE_IDENTITY.on(database).find(session, new Document("_id", "cluster")).first();
        if (root == null || !scope.clusterId().equals(root.get("clusterId")) || !Long.valueOf(scope.bindingVersion()).equals(root.get("bindingVersion"))
                || !scope.cacheIncarnation().equals(root.get("cacheIncarnation")) || !MongoConfigurationDomain.LAYOUT.equals(root.get("layoutId"))
                || !MongoCacheBusinessPreparation.CREATING.equals(root.get("businessPhase")) || operationId == null || operationId.isBlank()
                || !operationId.equals(root.get("businessPreparationId")) || root.containsKey("claimAdmissionFence") || root.containsKey("businessRetirementId")
                || SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).find(session, new Document("_id", "business")).first() != null
                || SystemCollections.CACHE_CLUSTER_MEMBERSHIP.on(database).countDocuments(session) != 0) throw unreadable(scope);
        SystemCollections.CACHE_CLUSTER_MEMBERSHIP.on(database).insertOne(session, new Document("_id", scope.clusterId())
                .append("cacheScope", header(scope)).append("phase", GENESIS).append("revision", 0L));
    }

    @Override public Optional<ClusterMembership> read(String clusterId) {
        requireCluster(clusterId);
        return access.read(READ_BUDGET, session -> Optional.ofNullable(read(session)));
    }

    @Override public ClusterMembership createIfAbsent(String clusterId, Set<String> activeNodeIds) {
        requireCluster(clusterId); List<String> captured = nodes(activeNodeIds);
        return access.write(session -> {
            proveNode(session); ClusterMembership current = read(session);
            if (current != null) return current;
            Document result = access.collection.findOneAndReplace(session, new Document("_id", scope.clusterId())
                            .append("cacheScope", header(scope)).append("phase", GENESIS).append("revision", 0L),
                    committed(1L, captured), new FindOneAndReplaceOptions().returnDocument(ReturnDocument.AFTER));
            if (result == null) throw new IllegalStateException("membership genesis changed within its transaction snapshot");
            return decode(result);
        });
    }

    @Override public Optional<ClusterMembership> compareAndSet(String clusterId, long expectedRevision, Set<String> activeNodeIds) {
        requireCluster(clusterId); List<String> captured = nodes(activeNodeIds);
        if (expectedRevision < 1) throw new IllegalArgumentException("expected membership revision must be positive");
        return access.write(session -> {
            proveNode(session); ClusterMembership current = read(session);
            if (current == null || current.revision() != expectedRevision) return Optional.empty();
            if (current.revision() >= Long.MAX_VALUE - 1) throw fenced();
            Document result = access.collection.findOneAndReplace(session, new Document("_id", scope.clusterId())
                            .append("cacheScope", header(scope)).append("phase", COMMITTED).append("revision", expectedRevision),
                    committed(expectedRevision + 1, captured), new FindOneAndReplaceOptions().returnDocument(ReturnDocument.AFTER));
            if (result == null) throw new IllegalStateException("membership revision changed within its transaction snapshot");
            return Optional.of(decode(result));
        });
    }

    private ClusterMembership read(ClientSession session) {
        Document row = access.collection.find(session, new Document("_id", scope.clusterId())).first();
        if (row == null) throw unreadable(scope);
        return decode(row);
    }
    private ClusterMembership decode(Document row) {
        try {
            if (!scope.clusterId().equals(row.get("_id")) || !access.inScope(row, scope.clusterId()) || !(row.get("revision") instanceof Long revision)) throw unreadable(scope);
            if (GENESIS.equals(row.get("phase"))) {
                if (revision != 0 || !row.keySet().equals(Set.of("_id", "cacheScope", "phase", "revision"))) throw unreadable(scope);
                return null;
            }
            if (!COMMITTED.equals(row.get("phase")) || revision < 1 || revision == Long.MAX_VALUE
                    || !row.keySet().equals(Set.of("_id", "cacheScope", "phase", "revision", "activeNodeIds"))
                    || !(row.get("activeNodeIds") instanceof List<?> raw)) throw unreadable(scope);
            var ids = new ArrayList<String>();
            for (Object value : raw) { if (!(value instanceof String id) || id.isBlank()) throw unreadable(scope); ids.add(id); }
            var membership = new ClusterMembership(scope.clusterId(), revision, Set.copyOf(ids));
            if (!nodes(membership.activeNodeIds()).equals(ids)) throw unreadable(scope);
            return membership;
        } catch (TapstateException | IllegalArgumentException | ClassCastException | NullPointerException invalid) { throw unreadable(scope); }
    }
    private Document committed(long revision, List<String> ids) { return new Document("_id", scope.clusterId()).append("cacheScope", header(scope))
            .append("phase", COMMITTED).append("revision", revision).append("activeNodeIds", ids); }
    private void proveNode(ClientSession session) {
        if (node == null) throw MongoCacheBusinessIdentity.businessFenced();
        MongoCacheWorkloadClaimStore.requireCollection(database);
        var claims = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database);
        Document row = claims.find(session, new Document("_id", MongoWorkloadClaimStore.id(node.key()))).first();
        if (row == null) throw fenced();
        var stored = new MongoWorkloadClaimStore(claims).inStrictSession(session).decodeStored(row);
        if (!node.equals(WorkloadClaimFence.from(stored))) throw fenced();
        Document filter = WorkloadClaimDocuments.live(node);
        if (row.containsKey(NODE_WRITES)) {
            if (!(row.get(NODE_WRITES) instanceof Long count) || count < 0 || count >= Long.MAX_VALUE - 1) throw fenced();
            filter.append(NODE_WRITES, count);
        } else filter.append(NODE_WRITES, new Document("$exists", false));
        if (claims.updateOne(session, filter, new Document("$inc", new Document(NODE_WRITES, 1L))).getMatchedCount() != 1) throw fenced();
    }
    private static List<String> nodes(Set<String> ids) {
        var captured = Set.copyOf(Objects.requireNonNull(ids, "activeNodeIds"));
        if (captured.isEmpty() || captured.stream().anyMatch(String::isBlank)) throw new IllegalArgumentException("membership nodes must not be empty or blank");
        return captured.stream().sorted().toList();
    }
    private void requireCluster(String clusterId) { if (!scope.clusterId().equals(Objects.requireNonNull(clusterId, "clusterId"))) throw MongoCacheBusinessIdentity.businessFenced(); }
    private static Document header(CacheBusinessScope scope) { return new Document("clusterId", scope.clusterId()).append("bindingVersion", scope.bindingVersion()).append("cacheIncarnation", scope.cacheIncarnation()); }
    private static TapstateException unreadable(CacheBusinessScope scope) { return MongoCacheMetadataAccess.unreadable(scope.clusterId(), "cluster-membership"); }
    private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null); }
}
