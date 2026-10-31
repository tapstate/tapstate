package io.tapstate.core.alerting;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static io.tapstate.core.alerting.AlertChannelPolicy.*;

class AlertChannelPolicyTest {
    static final Capability EMAIL = new Capability("email", 1, Family.OP_EXTERNAL);
    static final Capability SDK = new Capability("cloud-sdk", 1, Family.CLOUD_SDK);
    @Test void deploymentNeverFallsBackAndOnlyItsExternalAdaptersCanDeliver() {
        for (Capability capability : new Capability[]{EMAIL, SDK}) {
            assertThat(mayDeliver(Deployment.UNRESOLVED, capability)).isFalse();
            assertThatThrownBy(() -> requireDeliverable(Deployment.UNRESOLVED, capability)).isInstanceOf(TapstateException.class);
        }
        assertThat(mayDeliver(Deployment.ON_PREM, EMAIL)).isTrue();
        assertThat(mayDeliver(Deployment.CLOUD_MANAGED, EMAIL)).isFalse();
        assertThat(mayDeliver(Deployment.ON_PREM, SDK)).isFalse();
        assertThat(mayDeliver(Deployment.CLOUD_MANAGED, SDK)).isTrue();
    }
    @Test void externalConfigurationAndTestsAreOnlyAvailableForOnPremExternalAdapters() {
        assertThat(mayConfigureExternal(Deployment.ON_PREM, EMAIL)).isTrue();
        assertThat(maySendTest(Deployment.ON_PREM, EMAIL)).isTrue();
        assertThat(mayConfigureExternal(Deployment.CLOUD_MANAGED, EMAIL)).isFalse();
        assertThat(maySendTest(Deployment.CLOUD_MANAGED, SDK)).isFalse();
        Capability added = new Capability("test-new-channel", 1, Family.OP_EXTERNAL);
        assertThat(mayDeliver(Deployment.ON_PREM, added)).isTrue();
        assertThat(mayDeliver(Deployment.CLOUD_MANAGED, added)).isFalse();
    }
}
