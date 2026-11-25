package io.tapstate.app;

import io.tapstate.core.alerting.AlertChannelCatalog;
import io.tapstate.core.alerting.AlertChannelPolicy;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AlertChannelRegistryTest {
    @Test
    void onPremExtensionCannotClaimTheManagedEmailChannel() {
        var extension = new AlertChannelPolicy.Capability(AlertChannelPolicy.CLOUD_EMAIL.typeId(), 1,
                AlertChannelPolicy.Family.OP_EXTERNAL);
        var descriptor = new AlertChannelCatalog.Descriptor(extension, "Extension",
                AlertChannelCatalog.TransportFamily.HTTPS_JSON, 1, List.of());
        assertThatThrownBy(() -> new AlertChannelRegistry.Registration(descriptor, context -> null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid static HTTPS registration");
    }
}
