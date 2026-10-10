package io.tapstate.spi.store;

import java.util.Map;
import java.util.Objects;

/** Durable evidence used to enqueue or deduplicate a recovery, never an in-memory job enumeration. */
public record ClusterRecoveryEvent(
        ClusterRecoveryKey key, ClusterRecoveryCause cause,
        long originalExecutionGeneration, String originalExecutionRevision, Long sourceTopologyRevision,
        ClusterExecutionProfile sourceProfile, boolean legacySourceProfile, ClusterExecutionProfile targetProfile,
        long targetTopologyRevision, String intentFingerprint,
        Map<String, ClusterRecoveryPosition> resumePositions) {
    public ClusterRecoveryEvent {
        key = Objects.requireNonNull(key, "key");
        cause = Objects.requireNonNull(cause, "cause");
        if (originalExecutionRevision != null) {
            originalExecutionRevision = ClusterRecoveryKey.required(originalExecutionRevision, "originalExecutionRevision");
        }
        targetProfile = Objects.requireNonNull(targetProfile, "targetProfile");
        intentFingerprint = ClusterRecoveryKey.required(intentFingerprint, "intentFingerprint");
        resumePositions = Map.copyOf(Objects.requireNonNull(resumePositions, "resumePositions"));
        if (originalExecutionGeneration < 1 || targetTopologyRevision < 1
                || !key.clusterId().equals(targetProfile.clusterId())
                || (legacySourceProfile != (sourceProfile == null))
                || (!legacySourceProfile && originalExecutionRevision == null)
                || (legacySourceProfile != (sourceTopologyRevision == null))
                || (sourceTopologyRevision != null && sourceTopologyRevision < 1)
                || (sourceProfile != null && (!key.clusterId().equals(sourceProfile.clusterId())
                        || targetProfile.generation() < sourceProfile.generation()))
                || (legacySourceProfile && cause != ClusterRecoveryCause.FULL_CLUSTER_RESTART)) {
            throw new IllegalArgumentException("recovery event generations or cluster identities are invalid");
        }
        if (cause == ClusterRecoveryCause.FULL_CLUSTER_RESTART
                && sourceProfile != null && targetProfile.generation() <= sourceProfile.generation()) {
            throw new IllegalArgumentException("full restart requires a newer profile generation");
        }
        for (Map.Entry<String, ClusterRecoveryPosition> entry : resumePositions.entrySet()) {
            if (!entry.getKey().equals(entry.getValue().sourceId())) {
                throw new IllegalArgumentException("resume position keys must be source identities");
            }
        }
    }

    public ClusterRecoveryEvent(ClusterRecoveryKey key, ClusterRecoveryCause cause, long originalExecutionGeneration,
            String originalExecutionRevision, Long sourceTopologyRevision, ClusterExecutionProfile sourceProfile, ClusterExecutionProfile targetProfile,
            long targetTopologyRevision, String intentFingerprint, Map<String, ClusterRecoveryPosition> resumePositions) {
        this(key, cause, originalExecutionGeneration, originalExecutionRevision, sourceTopologyRevision, sourceProfile, false, targetProfile,
                targetTopologyRevision, intentFingerprint, resumePositions);
    }

    /** Event identity uses topology for member loss and the new profile generation for a full restart. */
    public UniqueKey uniqueKey() {
        long revision = cause == ClusterRecoveryCause.MEMBER_LOSS
                ? targetTopologyRevision : targetProfile.generation();
        return new UniqueKey(key, originalExecutionGeneration, revision, cause);
    }

    public record UniqueKey(ClusterRecoveryKey key, long originalExecutionGeneration,
            long recoveryRevision, ClusterRecoveryCause cause) {}
}
