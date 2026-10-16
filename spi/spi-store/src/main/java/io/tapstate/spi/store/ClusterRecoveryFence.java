package io.tapstate.spi.store;

import java.util.Objects;

/** All optimistic expectations for one atomic queue command; a read never grants write authority. */
public record ClusterRecoveryFence(
        ClusterRecoveryKey key, long itemRevision, String intentFingerprint,
        ClusterExecutionProfile targetProfile, long executionGeneration,
        WorkloadClaimFence recoveryClaim) {
    public ClusterRecoveryFence {
        key = Objects.requireNonNull(key, "key");
        intentFingerprint = ClusterRecoveryKey.required(intentFingerprint, "intentFingerprint");
        targetProfile = Objects.requireNonNull(targetProfile, "targetProfile");
        recoveryClaim = Objects.requireNonNull(recoveryClaim, "recoveryClaim");
        if (itemRevision < 0 || executionGeneration < 1
                || !key.clusterId().equals(targetProfile.clusterId())
                || !key.clusterId().equals(recoveryClaim.key().clusterId())
                || recoveryClaim.key().type() != WorkloadClaimType.CLUSTER_RECOVERY
                || !key.clusterId().equals(recoveryClaim.key().resourceId())) {
            throw new IllegalArgumentException("recovery fence identities or generations are invalid");
        }
    }

    /** Revision zero is the insert expectation, not permission to replace an existing item. */
    public static ClusterRecoveryFence enqueue(ClusterRecoveryEvent event, WorkloadClaimFence claim) {
        return new ClusterRecoveryFence(event.key(), 0, event.intentFingerprint(),
                event.targetProfile(), event.originalExecutionGeneration(), claim);
    }
}
