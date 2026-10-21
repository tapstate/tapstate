package io.tapstate.app;

import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ArtifactIdentity;
import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ClusterRecoveryCause;
import io.tapstate.spi.store.WorkloadClaim;
import java.util.Optional;

/** Discovers candidates from durable intent and execution provenance; the queue revalidates all writes. */
final class PipelineRecoveryDiscovery {
    private PipelineRecoveryDiscovery() { }

    static Optional<ClusterRecoveryCause> cause(DesiredState desired, PipelineState actual,
            ArtifactIdentity artifact, WorkloadClaim execution, ClusterExecutionProfile currentProfile,
            boolean hasLiveJob, boolean originalCohortChanged) {
        if (desired.targetState() != PipelineState.RUNNING || execution.executionGeneration() < 1
                || hasLiveJob || !artifact.incarnation().equals(execution.executionIncarnation())
                || (actual != PipelineState.RUNNING && actual != PipelineState.FAILED)) {
            return Optional.empty();
        }
        boolean recorded = execution.contextExecutionGeneration() == execution.executionGeneration()
                && execution.failureClaimGeneration() > 0;
        if (recorded && !execution.failureAfterMemberLoss()) {
            return Optional.empty();
        }
        // A FAILED checkpoint without a durable topology verdict is not a cold-start restart request.
        if (actual == PipelineState.FAILED && (!recorded || !execution.failureAfterMemberLoss())) {
            return Optional.empty();
        }
        ClusterExecutionProfile original = execution.executionProfile();
        if (original == null || original.generation() < currentProfile.generation()) {
            return Optional.of(ClusterRecoveryCause.FULL_CLUSTER_RESTART);
        }
        if (!original.equals(currentProfile) || execution.executionTopologyRevision() == null
                || execution.executionRevision() == null
                || execution.contextExecutionGeneration() != execution.executionGeneration()) {
            return Optional.empty();
        }
        return originalCohortChanged || (recorded && execution.failureAfterMemberLoss())
                ? Optional.of(ClusterRecoveryCause.MEMBER_LOSS) : Optional.empty();
    }
}
