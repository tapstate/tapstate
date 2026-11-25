package io.tapstate.app;

import com.hazelcast.cluster.InitialMembershipEvent;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleService;
import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

class SingleMemberGuardTest {
    private final Member local = member();
    private final Member peer = member();

    private static Member member() {
        var result = mock(Member.class);
        when(result.getUuid()).thenReturn(UUID.randomUUID());
        return result;
    }

    @Test void singleStartupRefusesAnActualPeerAndLeavesThatPeerRunning() {
        var properties = new HazelcastProperties();
        properties.setMemberPort(0);
        properties.setClusterName("single-guard-" + UUID.randomUUID());
        var peerConfig = HazelcastConfiguration.memberConfig(properties);
        peerConfig.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        var cluster = new ClusterProperties();
        var beans = new org.springframework.beans.factory.support.StaticListableBeanFactory();
        var existing = com.hazelcast.core.Hazelcast.newHazelcastInstance(peerConfig);
        try {
            int port = existing.getCluster().getLocalMember().getAddress().getPort();
            assertThatThrownBy(() -> new HazelcastConfiguration().hazelcastMember(properties, cluster, new ControlEndpointProperties(),
                    null, null, null, null, io.tapstate.runtime.engine.nest.NestSettings.defaults(), null, null, null,
                    beans.getBeanProvider(io.tapstate.spi.store.ClusterIdentityStore.class),
                    beans.getBeanProvider(io.tapstate.spi.store.WorkloadClaimStore.class),
                    new ClusterMembershipGate(cluster), BootId.fresh(), beans.getBeanProvider(io.tapstate.spi.store.StorePort.class), config -> {
                        // Exercise a mismatch between the declared profile and the actual embedded member.
                        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1:" + port);
                        return com.hazelcast.core.Hazelcast.newHazelcastInstance(config);
                    })).isInstanceOf(TapstateException.class).hasMessageContaining("observed 2 Hazelcast members");
            org.assertj.core.api.Assertions.assertThat(existing.getLifecycleService().isRunning()).isTrue();
        } finally { existing.shutdown(); }
    }

    @Test void refusesAnInitialPeerEvenIfItLeavesBeforeStartupFinishes() {
        var guard = new SingleMemberGuard("test-cluster", null);
        var initial = mock(InitialMembershipEvent.class);
        when(initial.getMembers()).thenReturn(Set.of(local, peer));
        guard.init(initial);
        guard.observe(Set.of(local));
        assertThatThrownBy(() -> guard.started(mock(HazelcastInstance.class))).isInstanceOf(TapstateException.class)
                .hasMessageContaining("test-cluster").hasMessageContaining(peer.getUuid().toString())
                .hasMessageContaining("discovery NONE");
    }

    @Test void aPeerArrivingAfterStartupStopsOnlyTheLocalMemberOnce() {
        var exit = mock(SingleMemberGuard.ProcessExit.class);
        var guard = new SingleMemberGuard("test-cluster", exit);
        var instance = mock(HazelcastInstance.class);
        var lifecycle = mock(LifecycleService.class);
        when(instance.getLifecycleService()).thenReturn(lifecycle);
        guard.observe(Set.of(local));
        guard.started(instance);
        var join = mock(MembershipEvent.class);
        when(join.getMembers()).thenReturn(Set.of(local, peer));
        guard.memberAdded(join);
        guard.memberAdded(join);
        verify(exit, timeout(3000)).exit();
        verify(lifecycle, times(1)).terminate();
        assertThatThrownBy(guard::requireSingle).isInstanceOf(TapstateException.class);
    }
}
