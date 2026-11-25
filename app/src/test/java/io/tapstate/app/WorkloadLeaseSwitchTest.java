package io.tapstate.app;

import org.junit.jupiter.api.Test;

import static io.tapstate.app.ClusterProperties.WorkloadLeases.INDEPENDENT;
import static io.tapstate.app.ClusterProperties.WorkloadLeases.NODE_BACKED;
import static org.assertj.core.api.Assertions.assertThat;

class WorkloadLeaseSwitchTest {
    @Test void opResolvesWorkloadLeasesFromTheConfiguredProfile() {
        var properties = new ClusterProperties();
        assertThat(properties.workloadLeases(false, true)).isEqualTo(NODE_BACKED);
        for (var profile : ClusterProperties.Profile.values()) {
            properties.setProfile(profile);
            assertThat(properties.workloadLeases(false, true)).isEqualTo(
                    profile == ClusterProperties.Profile.SINGLE ? NODE_BACKED : INDEPENDENT);
            assertThat(properties.workloadLeases(true, true)).isEqualTo(INDEPENDENT);
            assertThat(properties.workloadLeases(false, false)).isEqualTo(INDEPENDENT);
        }
    }
}
