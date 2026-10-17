package io.tapstate.adapters.mongostore;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.bson.Document;

import java.util.List;
import java.util.Map;

/** The one Mongo shape and live-claim filter shared by durable store-side workload fences. */
final class WorkloadClaimDocuments {

    private WorkloadClaimDocuments() {
    }

    /** The exact live claim named by {@code fence}, with lease time judged by the Mongo server. */
    static Document live(WorkloadClaimFence fence) {
        Document id = new Document("clusterId", fence.key().clusterId())
                .append("resourceType", fence.key().type().name())
                .append("resourceId", fence.key().resourceId());
        return new Document("_id", id)
                .append("ownerNodeId", fence.owner().nodeId())
                .append("ownerBootId", fence.owner().bootId())
                .append("claimGeneration", fence.claimGeneration())
                .append("executionGeneration", fence.executionGeneration())
                .append("topologyRevision", fence.topologyRevision())
                .append("$expr", new Document("$gt", List.of("$leaseUntil", "$$NOW")));
    }

    static WorkloadClaimFence readCapture(Document stored) { return read(stored, WorkloadClaimType.CAPTURE); }
    static WorkloadClaimFence readPipeline(Document stored) { return read(stored, WorkloadClaimType.PIPELINE_ACTUATION); }
    private static WorkloadClaimFence read(Document stored, WorkloadClaimType type) {
        try {
            if (!(stored.get("claimGeneration") instanceof Long claim) || !(stored.get("executionGeneration") instanceof Long execution)
                    || !(stored.get("topologyRevision") instanceof Long topology)) throw unreadable();
            var fence = new WorkloadClaimFence(new WorkloadClaimKey(stored.getString("clusterId"), type,
                    stored.getString("resourceId")), new WorkloadOwner(stored.getString("ownerNodeId"), stored.getString("ownerBootId")),
                    claim, execution, topology);
            if (!stored.equals(stored(fence))) throw unreadable();
            return fence;
        } catch (IllegalArgumentException | NullPointerException | ClassCastException invalid) { throw unreadable(); }
    }

    private static TapstateException unreadable() {
        return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "workload", "field", "claim-fence"), null);
    }

    /** The canonical persisted form of one workload claim fence. */
    static Document stored(WorkloadClaimFence fence) {
        return new Document("clusterId", fence.key().clusterId())
                .append("resourceType", fence.key().type().name())
                .append("resourceId", fence.key().resourceId())
                .append("ownerNodeId", fence.owner().nodeId())
                .append("ownerBootId", fence.owner().bootId())
                .append("claimGeneration", fence.claimGeneration())
                .append("executionGeneration", fence.executionGeneration())
                .append("topologyRevision", fence.topologyRevision());
    }
}
