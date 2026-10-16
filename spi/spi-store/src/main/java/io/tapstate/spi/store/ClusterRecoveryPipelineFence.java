package io.tapstate.spi.store;

import java.util.Objects;

/** Exact pipeline authority for reporting a fact about its allocated recovery successor. */
public record ClusterRecoveryPipelineFence(ClusterRecoveryKey key, long itemRevision,
        String intentFingerprint, ClusterExecutionProfile targetProfile, WorkloadClaimFence pipelineClaim) {
    public ClusterRecoveryPipelineFence {
        key = Objects.requireNonNull(key, "key");
        intentFingerprint = ClusterRecoveryKey.required(intentFingerprint, "intentFingerprint");
        targetProfile = Objects.requireNonNull(targetProfile, "targetProfile");
        pipelineClaim = Objects.requireNonNull(pipelineClaim, "pipelineClaim");
        if (itemRevision < 1 || pipelineClaim.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || pipelineClaim.executionGeneration() < 1 || pipelineClaim.profileGeneration() != targetProfile.generation()
                || !key.clusterId().equals(targetProfile.clusterId())
                || !key.clusterId().equals(pipelineClaim.key().clusterId())
                || !key.pipelineId().equals(pipelineClaim.key().resourceId())) {
            throw new IllegalArgumentException("pipeline failure report requires the exact allocated recovery execution");
        }
    }
}
