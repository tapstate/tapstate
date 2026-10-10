package io.tapstate.app;

import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ArtifactIdentity;
import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ClusterRecoveryCause;
import io.tapstate.spi.store.ExecutionProfile;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class PipelineRecoveryDiscoveryTest {
    private static final ClusterExecutionProfile CURRENT = profile(3);
    private static final ArtifactIdentity ARTIFACT = new ArtifactIdentity("p", "inc-p", "a".repeat(64));
    private static final DesiredState RUNNING = new DesiredState("p", PipelineState.RUNNING, ARTIFACT.contentHash());

    @Test void aFullRestartReadsTheOriginalExecutionProfileRatherThanItsNewController() {
        assertThat(PipelineRecoveryDiscovery.cause(RUNNING, PipelineState.RUNNING, ARTIFACT,
                execution(profile(2), 0, false), CURRENT, false, false))
                .contains(ClusterRecoveryCause.FULL_CLUSTER_RESTART);
    }

    @Test void aCompletedPipelineIsNotReplayedEvenWhenItsDesiredIntentStillSaysRunning() {
        assertThat(PipelineRecoveryDiscovery.cause(RUNNING, PipelineState.COMPLETED, ARTIFACT,
                execution(profile(2), 0, false), CURRENT, false, false)).isEmpty();
    }

    @Test void anOrdinaryFailureStaysStickyAcrossAFullClusterRestart() {
        assertThat(PipelineRecoveryDiscovery.cause(RUNNING, PipelineState.FAILED, ARTIFACT,
                execution(profile(2), 4, false), CURRENT, false, true)).isEmpty();
    }

    @Test void anUnknownFailedCheckpointIsNotReclassifiedAsTopologyRecovery() {
        assertThat(PipelineRecoveryDiscovery.cause(RUNNING, PipelineState.FAILED, ARTIFACT,
                execution(profile(2), 0, false), CURRENT, false, true)).isEmpty();
    }

    @Test void aRecordedTopologyFailureCanResumeAfterTheClusterHasReformed() {
        assertThat(PipelineRecoveryDiscovery.cause(RUNNING, PipelineState.FAILED, ARTIFACT,
                execution(profile(2), 4, true), CURRENT, false, false))
                .contains(ClusterRecoveryCause.FULL_CLUSTER_RESTART);
    }

    @Test void aChangedExactCohortIsAMemberLossWhileAnIntactCohortIsNot() {
        WorkloadClaim claim = execution(CURRENT, 0, false);
        assertThat(PipelineRecoveryDiscovery.cause(RUNNING, PipelineState.RUNNING, ARTIFACT,
                claim, CURRENT, false, true)).contains(ClusterRecoveryCause.MEMBER_LOSS);
        assertThat(PipelineRecoveryDiscovery.cause(RUNNING, PipelineState.RUNNING, ARTIFACT,
                claim, CURRENT, false, false)).isEmpty();
    }

    @Test void deleteAndRecreateDoesNotInheritThePreviousIncarnationsRecovery() {
        ArtifactIdentity recreated = new ArtifactIdentity("p", "new-inc-p", ARTIFACT.contentHash());
        assertThat(PipelineRecoveryDiscovery.cause(RUNNING, PipelineState.RUNNING, recreated,
                execution(profile(2), 0, false), CURRENT, false, true)).isEmpty();
    }

    @Test void aLiveJobOrPausedIntentDoesNotCreateAnAutomaticRebuild() {
        WorkloadClaim claim = execution(profile(2), 0, false);
        assertThat(PipelineRecoveryDiscovery.cause(RUNNING, PipelineState.RUNNING, ARTIFACT,
                claim, CURRENT, true, true)).isEmpty();
        assertThat(PipelineRecoveryDiscovery.cause(new DesiredState("p", PipelineState.PAUSED, ARTIFACT.contentHash()),
                PipelineState.RUNNING, ARTIFACT, claim, CURRENT, false, true)).isEmpty();
    }

    private static WorkloadClaim execution(ClusterExecutionProfile original, long failure, boolean topology) {
        return new WorkloadClaim(new WorkloadClaimKey("c", WorkloadClaimType.PIPELINE_ACTUATION, "p"),
                new WorkloadOwner("new-holder", "new-boot"), 5, 8, 9, Instant.parse("2026-10-10T06:00:00Z"),
                8, 4, Set.of("a", "b", "c"), failure, topology, CURRENT.generation(), original,
                7L, ARTIFACT.incarnation(), ARTIFACT.contentHash());
    }

    private static ClusterExecutionProfile profile(long generation) {
        return new ClusterExecutionProfile("c", generation, new ExecutionProfile(1, Map.of("runtime", "same-build")));
    }
}
