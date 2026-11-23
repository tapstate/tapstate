package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.OperatorRunManifest;
import io.tapstate.spi.store.*;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class NativeStoppedTerminalRecheckTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster", 1, "cache", "p", 2);
    private static final WorkloadClaimFence FENCE = new WorkloadClaimFence(
            new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "p"), new WorkloadOwner("node", "boot"), 4, 9, 3);
    private static final CheckpointDoc CHECKPOINT = new CheckpointDoc("p", "STOPPED", 7, Instant.EPOCH);
    private static final PipelineIntentSnapshot PROCESSED = new PipelineIntentSnapshot(SCOPE, 12,
            new DesiredState("p", PipelineState.STOPPED, "revision", false));

    private static final class Fixture {
        final MongoConfigurationDomain config = mock(MongoConfigurationDomain.class);
        final PipelineIntentStore intents = mock(PipelineIntentStore.class);
        final MongoCacheDomain cache = mock(MongoCacheDomain.class);
        final Engine engine = mock(Engine.class);
        final HazelcastExecutionPlans plans = mock(HazelcastExecutionPlans.class);
        final HazelcastOperatorReadiness operators = mock(HazelcastOperatorReadiness.class);
        final Runnable active = () -> {};
        final NativeStoppedTerminalRecheck recheck = new NativeStoppedTerminalRecheck(config, cache, engine, plans, operators, active);
        Fixture() {
            when(config.intents()).thenReturn(intents);
            when(intents.loadForActuation("p")).thenReturn(Optional.of(PROCESSED));
            when(cache.stoppedCheckpointUnchanged(eq(SCOPE), eq(CHECKPOINT), eq(FENCE), anyBoolean(), same(active))).thenReturn(true);
            when(engine.captureRun("p")).thenReturn(Optional.empty());
            when(plans.terminalAt(SCOPE, FENCE)).thenReturn(true);
        }
        boolean check(StoppedTerminalRecheck.Tenure tenure) { return recheck.unchanged(PROCESSED, CHECKPOINT, FENCE, tenure); }
        void job(ExecutionFence original, com.hazelcast.jet.core.JobStatus status) {
            var run = mock(Engine.Run.class);
            when(run.status()).thenReturn(status);
            when(run.operatorManifest()).thenReturn(new OperatorRunManifest("p", original, List.of()));
            when(engine.captureRun("p")).thenReturn(Optional.of(run));
        }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void exactHeldOrLocallyConfirmedEndedTenureUsesOnlyReadProofs(boolean ended) {
        var f = new Fixture();
        assertThat(f.check(ended ? StoppedTerminalRecheck.Tenure.ENDED : StoppedTerminalRecheck.Tenure.HELD)).isTrue();
        verify(f.cache, times(2)).stoppedCheckpointUnchanged(SCOPE, CHECKPOINT, FENCE, ended, f.active);
        verify(f.intents, times(2)).loadForActuation("p");
        verify(f.operators).requireTerminal(SCOPE, FENCE, f.active);
        verifyNoMoreInteractions(f.cache, f.intents, f.operators);
    }

    @Test void localCompletionReceiptsCannotAuthorizeRemoteTenuresOrAChangedTopology() {
        var member = mock(com.hazelcast.core.HazelcastInstance.class, org.mockito.Mockito.RETURNS_DEEP_STUBS);
        var local = mock(com.hazelcast.cluster.Member.class);
        var remote = mock(com.hazelcast.cluster.Member.class);
        var membership = mock(ClusterMembershipGate.class);
        var claim = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "p"),
                new WorkloadOwner("node", "boot"), 1, 1, 1);
        when(local.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE)).thenReturn("node");
        when(local.localMember()).thenReturn(true);
        when(member.getCluster().getMembers()).thenReturn(java.util.Set.of(local));
        when(membership.committed()).thenReturn(new io.tapstate.spi.store.ClusterMembership("cluster", 1, java.util.Set.of("node")));
        assertThat(NativeStoppedTerminalRecheck.localEvidence(member, membership, claim)).isTrue();
        when(member.getCluster().getMembers()).thenReturn(java.util.Set.of(local, remote));
        assertThat(NativeStoppedTerminalRecheck.localEvidence(member, membership, claim)).isFalse();
        when(member.getCluster().getMembers()).thenReturn(java.util.Set.of(local));
        when(membership.committed()).thenReturn(new io.tapstate.spi.store.ClusterMembership("cluster", 2, java.util.Set.of("node")));
        assertThat(NativeStoppedTerminalRecheck.localEvidence(member, membership, claim)).isFalse();
    }

    @Test void aNewRunOrSameTargetNewStopRevisionCannotReuseTheProcessedStop() {
        for (var state : List.of(PipelineState.RUNNING, PipelineState.STOPPED)) {
            var f = new Fixture();
            when(f.intents.loadForActuation("p")).thenReturn(Optional.of(new PipelineIntentSnapshot(SCOPE, 13,
                    new DesiredState("p", state, "revision", false))));
            assertThat(f.check(StoppedTerminalRecheck.Tenure.HELD)).isFalse();
            verifyNoInteractions(f.cache, f.engine, f.plans, f.operators);
        }
    }

    @Test void aChangeDuringClusterReadsDoesNotBecomeAnOwnerFreeHint() {
        var f = new Fixture();
        when(f.intents.loadForActuation("p")).thenReturn(Optional.of(PROCESSED), Optional.empty());
        assertThat(f.check(StoppedTerminalRecheck.Tenure.ENDED)).isFalse();
    }

    @Test void anExplicitlyRetiredOldGenerationDoesNotHideAnActualLiveJob() {
        var f = new Fixture();
        var prior = new ExecutionFence(new PipelineIntentScope("cluster", 1, "cache", "p", 1),
                new WorkloadClaimFence(FENCE.key(), new WorkloadOwner("old", "boot-old"), 3, 8, 2));
        f.job(prior, com.hazelcast.jet.core.JobStatus.FAILED);
        assertThat(f.check(StoppedTerminalRecheck.Tenure.ENDED)).isTrue();
        f.job(prior, com.hazelcast.jet.core.JobStatus.RUNNING);
        assertThat(f.check(StoppedTerminalRecheck.Tenure.ENDED)).isFalse();
        f.job(prior, com.hazelcast.jet.core.JobStatus.COMPLETING);
        assertThat(f.check(StoppedTerminalRecheck.Tenure.ENDED)).isFalse();
    }

    @Test void aLivePlanLostEngineOrChangedLeaseKeepsTheOriginalStopPath() {
        var f = new Fixture(); when(f.plans.terminalAt(SCOPE, FENCE)).thenReturn(false);
        assertThat(f.check(StoppedTerminalRecheck.Tenure.HELD)).isFalse();
        f = new Fixture(); when(f.engine.isLost()).thenReturn(true);
        assertThat(f.check(StoppedTerminalRecheck.Tenure.HELD)).isFalse();
        f = new Fixture(); when(f.cache.stoppedCheckpointUnchanged(SCOPE, CHECKPOINT, FENCE, false, f.active)).thenReturn(false);
        assertThat(f.check(StoppedTerminalRecheck.Tenure.HELD)).isFalse();
    }

    @Test void aNewerOwnerCannotBeCoveredByAnOlderRetiredPlan() {
        var f = new Fixture();
        f.job(new ExecutionFence(SCOPE, new WorkloadClaimFence(FENCE.key(), new WorkloadOwner("new", "boot-new"), 5, 10, 4)),
                com.hazelcast.jet.core.JobStatus.FAILED);
        assertThatThrownBy(() -> f.check(StoppedTerminalRecheck.Tenure.ENDED)).isInstanceOf(io.tapstate.core.common.TapstateException.class);
    }
}
