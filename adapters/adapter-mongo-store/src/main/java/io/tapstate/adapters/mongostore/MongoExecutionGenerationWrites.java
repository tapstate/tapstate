package io.tapstate.adapters.mongostore;

import com.mongodb.MongoException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import org.bson.Document;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** The one generation algorithm, usable alone or in a caller-owned short transaction. */
final class MongoExecutionGenerationWrites {
    private final MongoCollection<Document> collection;

    MongoExecutionGenerationWrites(MongoCollection<Document> collection) {
        this.collection = Objects.requireNonNull(collection, "collection");
    }

    Optional<Document> advanceUnderClaim(ClientSession session, WorkloadClaimFence expected, long topologyRevision) {
        Objects.requireNonNull(expected, "expected");
        if (topologyRevision < 0) { throw new IllegalArgumentException("topologyRevision must not be negative"); }
        Document next = new Document("$set", new Document("executionGeneration", nextExecutionGeneration()));
        return Optional.ofNullable(apply(session, liveExpected(expected, topologyRevision), List.of(next), false));
    }

    Optional<Document> advanceStandalone(ClientSession session, String clusterId, String pipelineId) {
        WorkloadClaimKey key = new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION, pipelineId);
        Document fields = new Document("clusterId", key.clusterId()).append("resourceType", key.type().name())
                .append("resourceId", key.resourceId()).append("executionGeneration", nextExecutionGeneration());
        Document eligible = new Document("$and", List.of(new Document("_id", id(key)),
                new Document("$or", List.of(new Document("ownerNodeId", new Document("$exists", false)),
                        new Document("$and", List.of(new Document("leaseUntil", new Document("$type", "date")),
                                new Document("$expr", new Document("$lte", List.of("$leaseUntil", "$$NOW")))))))));
        List<Document> update = List.of(new Document("$set", fields));
        Document advanced = apply(session, eligible, update, false);
        if (advanced != null) { return Optional.of(advanced); }
        Document absent = new Document("_id", id(key)).append("ownerNodeId", new Document("$exists", false));
        try {
            advanced = apply(session, absent, update, true);
        } catch (MongoException raced) {
            if (raced.getCode() != 11000 || session != null) { throw raced; }
            // A transaction must abort after a write error; only the standalone operation retries here.
            advanced = apply(null, eligible, update, false);
        }
        return Optional.ofNullable(advanced);
    }

    static Document liveExpected(WorkloadClaimFence expected, long topologyRevision) {
        return new Document("$and", List.of(new Document("_id", id(expected.key()))
                .append("ownerNodeId", expected.owner().nodeId()).append("ownerBootId", expected.owner().bootId())
                .append("claimGeneration", expected.claimGeneration()).append("executionGeneration", expected.executionGeneration()),
                new Document("topologyRevision", topologyRevision),
                new Document("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW")))));
    }

    static Document id(WorkloadClaimKey key) {
        return new Document("clusterId", key.clusterId()).append("resourceType", key.type().name())
                .append("resourceId", key.resourceId());
    }

    private static Document nextExecutionGeneration() {
        return new Document("$add", List.of(new Document("$ifNull", List.of("$executionGeneration", 0L)), 1L));
    }

    private Document apply(ClientSession session, Document filter, List<Document> update, boolean upsert) {
        FindOneAndUpdateOptions options = new FindOneAndUpdateOptions().upsert(upsert).returnDocument(ReturnDocument.AFTER);
        return session == null ? collection.findOneAndUpdate(filter, update, options)
                : collection.findOneAndUpdate(session, filter, update, options);
    }
}
