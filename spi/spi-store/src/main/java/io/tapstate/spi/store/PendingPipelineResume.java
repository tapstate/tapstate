package io.tapstate.spi.store;

import java.util.Objects;

/** One accepted explicit resume, retaining the execution whose effects must retire before allocation. */
public record PendingPipelineResume(long stateEpoch, String intentFingerprint, WorkloadClaim originalClaim,
        String originalNativeJobId, String originalRuntimeExecutionId, String reservationId) {
    public PendingPipelineResume {
        Objects.requireNonNull(originalClaim, "originalClaim");
        if (stateEpoch < 1 || intentFingerprint == null || intentFingerprint.isBlank()
                || originalNativeJobId == null || originalNativeJobId.isBlank()
                || originalRuntimeExecutionId == null || originalRuntimeExecutionId.isBlank()
                || (reservationId != null && reservationId.isBlank())
                || originalClaim.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || originalClaim.profileGeneration() < 1 || originalClaim.executionGeneration() < 1
                || originalClaim.contextExecutionGeneration() != originalClaim.executionGeneration()
                || originalClaim.executionClaimGeneration() < 1 || originalClaim.executionProfile() == null
                || originalClaim.executionProfile().generation() != originalClaim.profileGeneration()
                || originalClaim.executionTopologyRevision() == null
                || originalClaim.executionIncarnation() == null || originalClaim.executionIncarnation().isBlank()
                || originalClaim.executionRevision() == null || originalClaim.executionRevision().isBlank()
                || originalClaim.executionNodeIds().isEmpty()
                || !originalClaim.executionMembers().keySet().equals(originalClaim.executionNodeIds())) {
            throw new IllegalArgumentException("pending resume requires a complete current execution identity");
        }
    }

    public PendingPipelineResume(long stateEpoch, String intentFingerprint, WorkloadClaim originalClaim,
            String originalNativeJobId, String originalRuntimeExecutionId) {
        this(stateEpoch, intentFingerprint, originalClaim, originalNativeJobId, originalRuntimeExecutionId, null);
    }

    /** Request identity excludes renewed leases, acquisition topology and the later reservation link. */
    public boolean sameRequestAs(PendingPipelineResume other) {
        return other != null && stateEpoch == other.stateEpoch && intentFingerprint.equals(other.intentFingerprint)
                && WorkloadClaimFence.from(originalClaim).sameAuthorityAs(WorkloadClaimFence.from(other.originalClaim))
                && sameExecutionContext(originalClaim, other.originalClaim)
                && originalNativeJobId.equals(other.originalNativeJobId)
                && originalRuntimeExecutionId.equals(other.originalRuntimeExecutionId);
    }

    /** The context remains immutable across authority handover. */
    public static boolean sameExecutionContext(WorkloadClaim one, WorkloadClaim other) {
        return one != null && other != null && one.key().equals(other.key())
                && one.executionGeneration() == other.executionGeneration()
                && one.contextExecutionGeneration() == other.contextExecutionGeneration()
                && one.executionClaimGeneration() == other.executionClaimGeneration()
                && one.executionNodeIds().equals(other.executionNodeIds())
                && Objects.equals(one.executionProfile(), other.executionProfile())
                && Objects.equals(one.executionTopologyRevision(), other.executionTopologyRevision())
                && Objects.equals(one.executionIncarnation(), other.executionIncarnation())
                && Objects.equals(one.executionRevision(), other.executionRevision())
                && one.executionMembers().equals(other.executionMembers());
    }

    public PendingPipelineResume withReservation(String id) {
        Objects.requireNonNull(id, "id");
        return new PendingPipelineResume(stateEpoch, intentFingerprint, originalClaim,
                originalNativeJobId, originalRuntimeExecutionId, id);
    }
}
