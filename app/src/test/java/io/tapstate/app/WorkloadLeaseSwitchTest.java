package io.tapstate.app;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkloadLeaseSwitchTest {
    @Test void singleOpUsesTheNodeByDefaultAndHonorsAnExplicitSwitchEvenWithAnExplicitProfile() {
        var defaults = new ClusterProperties();
        var discovery = HazelcastProperties.DiscoveryMode.NONE;
        assertThat(defaults.workloadLeases(false, true, discovery)).isEqualTo(ClusterProperties.WorkloadLeases.NODE_BACKED);
        for (boolean enabled : new boolean[]{true, false}) {
            var source = new MapConfigurationPropertySource(Map.of("tapstate.cluster.workload-leases-enabled", enabled,
                    "tapstate.cluster.profile", "SINGLE"));
            var properties = new Binder(source).bind("tapstate.cluster", Bindable.of(ClusterProperties.class)).get();
            assertThat(properties.workloadLeases(false, true, discovery)).isEqualTo(enabled
                    ? ClusterProperties.WorkloadLeases.INDEPENDENT : ClusterProperties.WorkloadLeases.NODE_BACKED);
        }
    }

    @Test void cloudAndMultiMemberDefaultToIndependentLeases() {
        var properties = new ClusterProperties();
        var discovery = HazelcastProperties.DiscoveryMode.NONE;
        assertThat(properties.workloadLeases(true, true, discovery)).isEqualTo(ClusterProperties.WorkloadLeases.INDEPENDENT);
        assertThat(properties.workloadLeases(false, false, discovery)).isEqualTo(ClusterProperties.WorkloadLeases.INDEPENDENT);
        for (var profile : java.util.List.of(ClusterProperties.Profile.PRODUCTION_HA, ClusterProperties.Profile.PROCESS_FAILURE_ONLY)) {
            properties.setProfile(profile);
            assertThat(properties.workloadLeases(false, true, discovery)).isEqualTo(ClusterProperties.WorkloadLeases.INDEPENDENT);
        }
    }

    @Test void otherDeploymentModesRefuseTheSingleMemberSetting() {
        var properties = new ClusterProperties();
        properties.setWorkloadLeasesEnabled(false);
        assertThatThrownBy(() -> properties.workloadLeases(true, true, HazelcastProperties.DiscoveryMode.NONE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> properties.workloadLeases(false, false, HazelcastProperties.DiscoveryMode.NONE))
                .isInstanceOf(IllegalArgumentException.class);
        for (var discovery : HazelcastProperties.DiscoveryMode.values()) {
            if (discovery != HazelcastProperties.DiscoveryMode.NONE)
                assertThatThrownBy(() -> properties.workloadLeases(false, true, discovery)).isInstanceOf(IllegalArgumentException.class);
        }
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        assertThatThrownBy(() -> properties.workloadLeases(false, true, HazelcastProperties.DiscoveryMode.NONE))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
