package io.tapstate.control.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class DeploymentProfileTest {

    @Test
    void defaultsToOnPremAndParsesPackagedProfiles() {
        assertThat(DeploymentProfile.parse(null)).isEqualTo(DeploymentProfile.ON_PREM);
        assertThat(DeploymentProfile.parse("cloud")).isEqualTo(DeploymentProfile.CLOUD);
        assertThat(DeploymentProfile.parse("on-prem")).isEqualTo(DeploymentProfile.ON_PREM);
    }

    @Test
    void refusesTyposInsteadOfSilentlySelectingTheWrongDeploymentPolicy() {
        assertThatThrownBy(() -> DeploymentProfile.parse("clould"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("unsupported deployment profile: clould");
    }
}
