package io.tapstate.app;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkloadLeaseSwitchTest {
    @Test void singleOpBindsTheSameSettingUsedByPipelineCaptureAndNest() {
        var defaults = new ClusterProperties();
        assertThat(defaults.isWorkloadLeasesEnabled()).isTrue();
        assertThat(defaults.nodeBackedPipeline()).isFalse();
        var source = new MapConfigurationPropertySource(Map.of("tapstate.cluster.workload-leases-enabled", "false"));
        var properties = new Binder(source).bind("tapstate.cluster", Bindable.of(ClusterProperties.class)).get();
        properties.validateWorkloadLeases(false, true, HazelcastProperties.DiscoveryMode.NONE);
        assertThat(properties.isWorkloadLeasesEnabled()).isFalse();
        assertThat(properties.nodeBackedPipeline()).isTrue();
    }

    @Test void otherDeploymentModesRefuseTheSingleMemberSetting() {
        var properties = new ClusterProperties();
        properties.setWorkloadLeasesEnabled(false);
        assertThatThrownBy(() -> properties.validateWorkloadLeases(true, true, HazelcastProperties.DiscoveryMode.NONE))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> properties.validateWorkloadLeases(false, false, HazelcastProperties.DiscoveryMode.NONE))
                .isInstanceOf(IllegalArgumentException.class);
        for (var discovery : HazelcastProperties.DiscoveryMode.values()) {
            if (discovery != HazelcastProperties.DiscoveryMode.NONE)
                assertThatThrownBy(() -> properties.validateWorkloadLeases(false, true, discovery)).isInstanceOf(IllegalArgumentException.class);
        }
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        assertThatThrownBy(() -> properties.validateWorkloadLeases(false, true, HazelcastProperties.DiscoveryMode.NONE))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
