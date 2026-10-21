package io.tapstate.adapters.mongostore;

import io.tapstate.spi.store.PendingPipelineResume;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import org.bson.Document;

import java.util.Date;
import java.util.Optional;

/** The bounded resume request embedded in the existing state checkpoint. */
final class PendingPipelineResumeDocuments {
    static final String FIELD = "pendingPipelineResume";
    static final String CAPACITY_EPOCH = "pendingResumeStateEpoch";

    private PendingPipelineResumeDocuments() {}

    static Document document(PendingPipelineResume resume) {
        WorkloadClaim claim = resume.originalClaim();
        Document original = WorkloadClaimDocuments.stored(WorkloadClaimFence.from(claim))
                .append("leaseUntil", Date.from(claim.leaseUntil()))
                .append("contextExecutionGeneration", claim.contextExecutionGeneration())
                .append("executionClaimGeneration", claim.executionClaimGeneration())
                .append("executionNodeIds", claim.executionNodeIds().stream().sorted().toList())
                .append("failureClaimGeneration", claim.failureClaimGeneration())
                .append("failureAfterMemberLoss", claim.failureAfterMemberLoss())
                .append("executionProfileVersion", 1L)
                .append("executionProfile", ClusterRecoveryDocuments.profile(claim.executionProfile()).append("schemaVersion", 1L))
                .append("executionTopologyRevision", claim.executionTopologyRevision())
                .append("executionIncarnation", claim.executionIncarnation()).append("executionRevision", claim.executionRevision())
                .append("executionMembers", MongoClusterCapacityStore.executionMemberDocuments(claim.executionMembers()));
        return new Document("stateEpoch", resume.stateEpoch()).append("intentFingerprint", resume.intentFingerprint())
                .append("originalClaim", original).append("originalNativeJobId", resume.originalNativeJobId())
                .append("originalRuntimeExecutionId", resume.originalRuntimeExecutionId()).append("reservationId", resume.reservationId());
    }

    static PendingPipelineResume resume(Document document) {
        try {
            return new PendingPipelineResume(integer(document, "stateEpoch"),
                    document.getString("intentFingerprint"), MongoWorkloadClaimStore.readDocument(document.get("originalClaim", Document.class)),
                    document.getString("originalNativeJobId"), document.getString("originalRuntimeExecutionId"),
                    document.getString("reservationId"));
        } catch (RuntimeException malformed) {
            throw ClusterRecoveryDocuments.unreadable(document, FIELD, malformed);
        }
    }

    static Optional<PendingPipelineResume> current(Document state) {
        if (state == null || state.get(FIELD) == null) {
            return Optional.empty();
        }
        if (!(state.get(FIELD) instanceof Document document)) {
            throw ClusterRecoveryDocuments.unreadable(state, FIELD, null);
        }
        PendingPipelineResume resume = resume(document);
        if (!resume.originalClaim().key().resourceId().equals(state.getString("_id"))) {
            throw ClusterRecoveryDocuments.unreadable(state, FIELD, null);
        }
        return "RUNNING".equals(state.getString("stateJson"))
                && ClusterRecoveryDocuments.number(state, "epoch") == resume.stateEpoch() ? Optional.of(resume) : Optional.empty();
    }

    static Document filter(Document state) {
        return new Document("_id", state.get("_id")).append("epoch", state.get("epoch"))
                .append("stateJson", "RUNNING").append(FIELD, state.get(FIELD));
    }

    static long integer(Document document, String field) {
        Object value = document.get(field);
        if (!(value instanceof Long) && !(value instanceof Integer)) {
            throw ClusterRecoveryDocuments.unreadable(document, field, null);
        }
        return ((Number) value).longValue();
    }
}
