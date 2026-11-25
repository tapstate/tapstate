package io.tapstate.app;

import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.spi.store.*;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class LocalCapturePreparationLeaseTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster", 1, "cache", "p", 1);
    private static final WorkloadClaimFence CLAIM = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "p"),
            new WorkloadOwner("node", "boot"), 3, 2, 7);
    private static final ExecutionFence RUN = new ExecutionFence(SCOPE, CLAIM);
    private static final class Fixture {
        final Member local = mock(Member.class);
        final Cluster cluster = mock(Cluster.class);
        final HazelcastInstance member = mock(HazelcastInstance.class);
        final ClusterMembershipGate membership = mock(ClusterMembershipGate.class);
        final PipelineActuationOwnership ownership = mock(PipelineActuationOwnership.class);
        boolean cloud;
        ClusterProperties.Profile profile = ClusterProperties.Profile.SINGLE;
        HazelcastProperties.DiscoveryMode discovery = HazelcastProperties.DiscoveryMode.NONE;
        StorageBinding.Mode mode = StorageBinding.Mode.OP;
        Fixture() {
            when(local.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE)).thenReturn("node");
            when(local.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE)).thenReturn("boot");
            when(local.localMember()).thenReturn(true);
            when(member.getCluster()).thenReturn(cluster);
            when(cluster.getMembers()).thenReturn(Set.of(local)); when(cluster.getLocalMember()).thenReturn(local);
            when(membership.committed()).thenReturn(new ClusterMembership("cluster", 7, Set.of("node")));
            when(ownership.hasLocalWriterLease(RUN)).thenReturn(true);
        }
        boolean allowed() { return NativeBusinessContext.localCapturePreparationLease(cloud, profile, discovery, mode, member, membership, ownership, RUN); }
    }
    @Test void onlyTheExactOriginalLocalSingleOpExecutionCanUseTheHint() {
        var f = new Fixture(); assertThat(f.allowed()).isTrue(); verify(f.ownership).hasLocalWriterLease(RUN);
    }
    @ParameterizedTest
    @ValueSource(strings = {"cloud", "cloud-binding", "ha", "process-failure", "tcp", "kubernetes", "missing-membership", "changed-topology", "wrong-cluster", "other-committed-member", "second-member", "remote", "other-boot", "other-node", "lost-or-expired"})
    void unsupportedDeploymentOrChangedIdentityCannotUseTheLocalPreparationHint(String scenario) {
        var f = new Fixture();
        switch (scenario) {
            case "cloud" -> f.cloud = true;
            case "cloud-binding" -> f.mode = StorageBinding.Mode.CLOUD;
            case "ha" -> f.profile = ClusterProperties.Profile.PRODUCTION_HA;
            case "process-failure" -> f.profile = ClusterProperties.Profile.PROCESS_FAILURE_ONLY;
            case "tcp" -> f.discovery = HazelcastProperties.DiscoveryMode.TCP_IP;
            case "kubernetes" -> f.discovery = HazelcastProperties.DiscoveryMode.KUBERNETES;
            case "missing-membership" -> when(f.membership.committed()).thenReturn(null);
            case "changed-topology" -> when(f.membership.committed()).thenReturn(new ClusterMembership("cluster", 8, Set.of("node")));
            case "wrong-cluster" -> when(f.membership.committed()).thenReturn(new ClusterMembership("other", 7, Set.of("node")));
            case "other-committed-member" -> when(f.membership.committed()).thenReturn(new ClusterMembership("cluster", 7, Set.of("node", "other")));
            case "second-member" -> when(f.cluster.getMembers()).thenReturn(Set.of(f.local, mock(Member.class)));
            case "remote" -> when(f.local.localMember()).thenReturn(false);
            case "other-boot" -> when(f.local.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE)).thenReturn("replacement-boot");
            case "other-node" -> when(f.local.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE)).thenReturn("replacement-node");
            case "lost-or-expired" -> when(f.ownership.hasLocalWriterLease(RUN)).thenReturn(false);
            default -> throw new AssertionError(scenario);
        }
        assertThat(f.allowed()).isFalse();
        verify(f.ownership, never()).proveStartClaim(any(), any());
    }
}
