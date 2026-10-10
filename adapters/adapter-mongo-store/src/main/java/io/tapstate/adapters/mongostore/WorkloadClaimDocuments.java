package io.tapstate.adapters.mongostore;

import io.tapstate.spi.store.WorkloadClaimFence;
import org.bson.Document;

import java.util.List;

/** The one Mongo shape and live-claim filter shared by durable store-side workload fences. */
final class WorkloadClaimDocuments {

    private WorkloadClaimDocuments() {
    }

    /** The exact live claim named by {@code fence}, with lease time judged by the Mongo server. */
    static Document live(WorkloadClaimFence fence) {
        Document id = new Document("clusterId", fence.key().clusterId())
                .append("resourceType", fence.key().type().name())
                .append("resourceId", fence.key().resourceId());
        Document filter = new Document("_id", id)
                .append("ownerNodeId", fence.owner().nodeId())
                .append("ownerBootId", fence.owner().bootId())
                .append("claimGeneration", fence.claimGeneration())
                .append("executionGeneration", fence.executionGeneration())
                .append("topologyRevision", fence.topologyRevision())
                .append("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW")));
        if (fence.profileGeneration() > 0) {
            filter.append("profileGeneration", fence.profileGeneration());
        } else {
            filter.append("$or", List.of(new Document("profileGeneration", 0L),
                    new Document("profileGeneration", new Document("$exists", false))));
        }
        return filter;
    }

    /** The canonical persisted form of one workload claim fence. */
    static Document stored(WorkloadClaimFence fence) {
        Document stored = new Document("clusterId", fence.key().clusterId())
                .append("resourceType", fence.key().type().name())
                .append("resourceId", fence.key().resourceId())
                .append("ownerNodeId", fence.owner().nodeId())
                .append("ownerBootId", fence.owner().bootId())
                .append("claimGeneration", fence.claimGeneration())
                .append("executionGeneration", fence.executionGeneration())
                .append("topologyRevision", fence.topologyRevision());
        if (fence.profileGeneration() > 0) {
            stored.append("profileGeneration", fence.profileGeneration());
        }
        return stored;
    }
}
