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

    public MongoClusterMembershipStore(MongoCollection<Document> collection) {
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

    private static ClusterMembership read(Document document) {
        List<String> nodes = document.getList("activeNodeIds", String.class);
        Number revision = document.get("revision", Number.class);
        if (nodes == null || revision == null) {
            throw new IllegalStateException("cluster membership document is incomplete");
        }
        return new ClusterMembership(document.getString("_id"), revision.longValue(), new LinkedHashSet<>(nodes));
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
