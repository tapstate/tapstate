package io.tapstate.app;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThatCode;

class CloudAlertSdkPublisherTest {
    @Test void theLocalSeamDoesNotRequireARemoteSdkService() {
        var round = new io.tapstate.core.alerting.DeliveryKey.NotificationRound(
                new io.tapstate.core.alerting.AlarmKey.Scope("org", "cluster"), "episode", "1", "system", "binding",
                io.tapstate.core.alerting.DeliveryKey.Kind.FAULT, 1);
        var request = io.tapstate.spi.alerting.NotificationChannel.Request.business(
                new io.tapstate.core.alerting.DeliveryKey(round, "cloud-sdk", "system", "cluster"), "delivery", "attempt", "binding-1",
                java.time.Instant.now().plusSeconds(1), "application/json", new byte[]{123, 125});
        assertThatCode(() -> new CloudAlertSdkPublisher().publishClusterAlert(request)).doesNotThrowAnyException();
    }
}
