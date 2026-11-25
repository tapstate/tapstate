package io.tapstate.app;

import com.hazelcast.cluster.Member;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ClusterIdentity;
import io.tapstate.spi.store.ClusterIdentityStore;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.ClusterMembershipStore;
import org.junit.jupiter.api.Test;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/** Identity and claim admission over explicit supplied domains; this does not prove production domain assembly. */
class SingleNodeAdmissionTest {
    @Test void defaultSingleIdentityReusesTheExistingClusterAndCompetesAcrossExplicitNodeSettings() {
        var ids = identities(); var memberships = memberships(); var properties = new ClusterProperties();
        properties.setNodeId("ignored-single-setting-a"); var first = ClusterMemberPreflight.singleIdentity(properties, ids, memberships);
        properties.setNodeId("ignored-single-setting-b"); var second = ClusterMemberPreflight.singleIdentity(properties, ids, memberships);
        assertThat(first.clusterId()).isEqualTo("cluster-a"); assertThat(first.nodeId()).isEqualTo("cluster-a"); assertThat(first.controlUrl()).isNull();
        assertThat(second).isEqualTo(first);
        var raw = new InMemoryWorkloadClaimStore();
        var admitted = ClusterMemberPreflight.reserveExisting(first, properties, ids, raw, "boot-a");
        assertThatThrownBy(() -> ClusterMemberPreflight.reserveExisting(second, properties, ids, raw, "boot-b"))
                .satisfies(error -> code(error, BootError.NODE_ID_IN_USE));
        assertThat(raw.release(admitted.nodeSession())).isTrue();
        var replacement = ClusterMemberPreflight.reserveExisting(second, properties, ids, raw, "boot-b");
        assertThat(replacement.nodeSession().claimGeneration()).isGreaterThan(admitted.nodeSession().claimGeneration());
        assertThat(raw.release(admitted.nodeSession())).isFalse();
        verify(ids, never()).createIfAbsent(any()); verify(memberships, never()).createIfAbsent(any(), any());
    }

    @Test void missingOrChangedConfigurationIdentityCannotBeCreatedDuringAdmission() {
        var ids = identities(); var memberships = memberships(); var properties = new ClusterProperties();
        when(ids.find()).thenReturn(Optional.empty());
        assertThatThrownBy(() -> ClusterMemberPreflight.singleIdentity(properties, ids, memberships)).satisfies(error -> code(error, BootError.CLUSTER_ID_REQUIRED));
        when(ids.find()).thenReturn(Optional.of(new ClusterIdentity("cluster-a")), Optional.empty());
        var captured = ClusterMemberPreflight.singleIdentity(properties, ids, memberships);
        assertThatThrownBy(() -> ClusterMemberPreflight.reserveExisting(captured, properties, ids, new InMemoryWorkloadClaimStore(), "boot-a"))
                .satisfies(error -> code(error, BootError.CLUSTER_ID_REQUIRED));
        verify(ids, never()).createIfAbsent(any());
    }

    @Test void anExistingClusterOrMultiMemberDeploymentCannotBeReplacedByLocalSingleFacts() {
        var ids = identities(); var memberships = memberships(); var properties = new ClusterProperties(); properties.setId("other-cluster");
        assertThatThrownBy(() -> ClusterMemberPreflight.singleIdentity(properties, ids, memberships)).satisfies(error -> code(error, BootError.CLUSTER_ID_MISMATCH));
        properties.setId(null); when(memberships.read("cluster-a")).thenReturn(Optional.of(new ClusterMembership("cluster-a", 7, Set.of("a", "b", "c"))));
        assertThatThrownBy(() -> ClusterMemberPreflight.singleIdentity(properties, ids, memberships)).satisfies(error -> code(error, BootError.CLUSTER_PROFILE_INVALID));
        verify(memberships, never()).createIfAbsent(any(), any()); verify(memberships, never()).compareAndSet(any(), anyLong(), any());
    }

    @Test void aSingleNodePublishesNoNetworkControlAddressAndKeepsAllDiscoveryDisabled() {
        var properties = new ClusterProperties(); var known = ClusterMemberPreflight.singleIdentity(properties, identities(), memberships());
        var admitted = ClusterMemberPreflight.reserveExisting(known, properties, identities(), new InMemoryWorkloadClaimStore(), "boot-a");
        var config = HazelcastConfiguration.identify(HazelcastConfiguration.memberConfig(new HazelcastProperties()), admitted);
        var join = config.getNetworkConfig().getJoin();
        assertThat(join.getAutoDetectionConfig().isEnabled()).isFalse(); assertThat(join.getMulticastConfig().isEnabled()).isFalse();
        assertThat(join.getTcpIpConfig().isEnabled()).isFalse(); assertThat(join.getKubernetesConfig().isEnabled()).isFalse();
        assertThat(config.getMemberAttributeConfig().getAttribute(ClusterMembershipGate.CONTROL_URL_ATTRIBUTE)).isNull();
        assertThat(config.getMemberAttributeConfig().getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE)).isEqualTo("cluster-a");
        assertThat(config.getMemberAttributeConfig().getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE)).isEqualTo("boot-a");
    }

    @Test void onlyTheActualReservedSingletonAndLiveNodeProofCanQualify() {
        var properties = new ClusterProperties(); var original = ClusterMemberPreflight.reserveExisting(
                ClusterMemberPreflight.singleIdentity(properties, identities(), memberships()), properties, identities(), new InMemoryWorkloadClaimStore(), "boot-a");
        var gate = new ClusterMembershipGate(properties); gate.bindSingle(original.nodeSession());
        Member valid = member("cluster-a", "boot-a"); var active = new AtomicBoolean(); gate.observeNodeSession(active::get);
        assertThat(gate.businessEligible()).isFalse(); assertThat(gate.apply(Set.of(valid))).isFalse();
        active.set(true); assertThat(gate.apply(Set.of(valid))).isTrue(); assertThat(gate.businessEligible()).isTrue();
        assertThat(gate.apply(Set.of())).isFalse(); assertThat(gate.businessEligible()).isFalse();
        assertThat(gate.apply(Set.of(member("cluster-a", "boot-b")))).isFalse();
        assertThat(gate.apply(Set.of(valid, member("cluster-a", "boot-b")))).isFalse();
        assertThat(gate.apply(Set.of(valid))).isTrue(); gate.canCommit(Set.of("cluster-a"));
        assertThat(gate.businessEligible()).as("stable IDs alone do not prove the actual boot").isFalse();
        assertThat(gate.apply(Set.of(valid))).isTrue(); active.set(false); assertThat(gate.businessEligible()).isFalse();
        active.set(true); gate.failClosed(); assertThat(gate.apply(Set.of(valid))).isFalse();
    }

    private static ClusterIdentityStore identities() { var ids = mock(ClusterIdentityStore.class); when(ids.find()).thenReturn(Optional.of(new ClusterIdentity("cluster-a"))); return ids; }
    private static ClusterMembershipStore memberships() { var store = mock(ClusterMembershipStore.class); when(store.read("cluster-a")).thenReturn(Optional.empty()); return store; }
    private static Member member(String node, String boot) { var member = mock(Member.class); when(member.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE)).thenReturn(node); when(member.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE)).thenReturn(boot); return member; }
    private static void code(Throwable error, BootError expected) { assertThat(error).isInstanceOfSatisfying(TapstateException.class, coded -> assertThat(coded.code()).isEqualTo(expected)); }
}
