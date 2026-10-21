package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.ClusterMembershipStore;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimType;
import org.bson.Document;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/** Majority Mongo implementation of the monotonic committed-membership registry. */
public final class MongoClusterMembershipStore implements ClusterMembershipStore {

    private final MongoCollection<Document> collection;
    private final MongoClusterProfileStore profiles;

    public MongoClusterMembershipStore(MongoCollection<Document> collection) {
        this(collection, null);
    }

    public MongoClusterMembershipStore(MongoCollection<Document> collection, MongoClusterProfileStore profiles) {
        this.profiles = profiles;
        this.collection = Objects.requireNonNull(collection, "collection")
                .withReadPreference(ReadPreference.primary())
                .withReadConcern(ReadConcern.MAJORITY)
                .withWriteConcern(WriteConcern.MAJORITY.withJournal(true));
    }

    @Override
    public Optional<ClusterMembership> read(String clusterId) {
        Document found = StoreIo.call(() -> collection.find(new Document("_id", clusterId)).first());
        return Optional.ofNullable(found).map(MongoClusterMembershipStore::read);
    }

    @Override
    public ClusterMembership createIfAbsent(String clusterId, Set<String> activeNodeIds) {
        requireLegacy();
        validate(clusterId, activeNodeIds);
        Document stored = StoreIo.call(() -> collection.findOneAndUpdate(
                new Document("_id", clusterId),
                Updates.combine(
                        Updates.setOnInsert("revision", 1L),
                        Updates.setOnInsert("activeNodeIds", ordered(activeNodeIds))),
                new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER)));
        return read(stored);
    }

    @Override
    public Optional<ClusterMembership> compareAndSet(
            String clusterId, long expectedRevision, Set<String> activeNodeIds) {
        requireLegacy();
        validate(clusterId, activeNodeIds);
        if (expectedRevision < 1) {
            throw new IllegalArgumentException("expectedRevision must be positive");
        }
        Document stored = StoreIo.call(() -> collection.findOneAndUpdate(
                new Document("_id", clusterId).append("revision", expectedRevision),
                Updates.combine(
                        Updates.set("activeNodeIds", ordered(activeNodeIds)),
                        Updates.inc("revision", 1L)),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER)));
        return Optional.ofNullable(stored).map(MongoClusterMembershipStore::read);
    }

    @Override
    public Optional<ClusterMembership> initializeProfile(
            WorkloadClaim nodeSession, long expectedRevision, Set<String> activeNodeIds) {
        return writeProfile(nodeSession, expectedRevision, activeNodeIds, true);
    }

    @Override
    public Optional<ClusterMembership> compareAndSetProfile(
            WorkloadClaim nodeSession, long expectedRevision, Set<String> activeNodeIds) {
        return writeProfile(nodeSession, expectedRevision, activeNodeIds, false);
    }

    private Optional<ClusterMembership> writeProfile(
            WorkloadClaim nodeSession, long expectedRevision, Set<String> activeNodeIds, boolean bootstrap) {
        Objects.requireNonNull(nodeSession, "nodeSession");
        String clusterId = nodeSession.key().clusterId();
        validate(clusterId, activeNodeIds);
        if (profiles == null || nodeSession.key().type() != WorkloadClaimType.NODE_SESSION
                || nodeSession.profileGeneration() < 1 || expectedRevision < 0) {
            throw new IllegalArgumentException("profile membership requires an admitted node-session authority");
        }
        return profiles.transaction(session -> {
            if (profiles.guard(session, clusterId, nodeSession.owner(), nodeSession.profileGeneration()) == null
                    || !profiles.guardJoinedNodes(session, clusterId, nodeSession.profileGeneration(), activeNodeIds)) {
                return Optional.empty();
            }
            Document previous = collection.find(session, new Document("_id", clusterId)).first();
            long revision = previous == null ? 0 : ((Number) previous.get("revision")).longValue();
            long generation = previous == null || previous.get("profileGeneration") == null
                    ? 0 : ((Number) previous.get("profileGeneration")).longValue();
            if (revision != expectedRevision
                    || (bootstrap ? generation >= nodeSession.profileGeneration()
                            : generation != nodeSession.profileGeneration())) {
                return Optional.empty();
            }
            if (!bootstrap && !activeNodeIds.containsAll(read(previous).activeNodeIds())) {
                return Optional.empty();
            }
            Document fields = new Document("revision", Math.addExact(revision, 1L))
                    .append("profileGeneration", nodeSession.profileGeneration())
                    .append("activeNodeIds", ordered(activeNodeIds));
            Document filter = new Document("_id", clusterId);
            if (previous != null) {
                filter.append("revision", revision);
            }
            Document next = collection.findOneAndUpdate(session, filter, new Document("$set", fields),
                    new FindOneAndUpdateOptions().upsert(previous == null).returnDocument(ReturnDocument.AFTER));
            return Optional.ofNullable(next).map(MongoClusterMembershipStore::read);
        });
    }

    private void requireLegacy() {
        if (profiles != null) {
            throw new IllegalStateException("profile membership must carry its exact node-session authority");
        }
    }

    private static ClusterMembership read(Document document) {
        List<String> nodes = document.getList("activeNodeIds", String.class);
        Number revision = document.get("revision", Number.class);
        if (nodes == null || revision == null) {
            throw new IllegalStateException("cluster membership document is incomplete");
        }
        Number profile = document.get("profileGeneration", Number.class);
        return new ClusterMembership(document.getString("_id"), revision.longValue(), new LinkedHashSet<>(nodes),
                profile == null ? 0 : profile.longValue());
    }

    private static List<String> ordered(Set<String> activeNodeIds) {
        List<String> ordered = new ArrayList<>(activeNodeIds);
        ordered.sort(String::compareTo);
        return ordered;
    }

    private static void validate(String clusterId, Set<String> activeNodeIds) {
        Objects.requireNonNull(clusterId, "clusterId");
        Objects.requireNonNull(activeNodeIds, "activeNodeIds");
        if (clusterId.isBlank() || activeNodeIds.isEmpty() || activeNodeIds.stream().anyMatch(String::isBlank)) {
            throw new IllegalArgumentException("cluster membership fields must not be blank or empty");
        }
    }
}
