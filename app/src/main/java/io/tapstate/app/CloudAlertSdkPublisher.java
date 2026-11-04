package io.tapstate.app;

import io.tapstate.spi.alerting.NotificationChannel;
import java.util.Objects;

/** Local Cloud SDK seam. Normal return is the current placeholder acceptance boundary. */
public final class CloudAlertSdkPublisher {
    public void publishClusterAlert(NotificationChannel.Request notification) {
        Objects.requireNonNull(notification, "notification");
    }
}
