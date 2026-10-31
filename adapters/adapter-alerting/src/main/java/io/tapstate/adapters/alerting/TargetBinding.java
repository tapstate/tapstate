package io.tapstate.adapters.alerting;

import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.spi.alerting.NotificationChannel;
import java.util.Objects;

/** A frozen semantic destination/configuration identity supplied by the host, never a current-config lookup. */
public record TargetBinding(AlarmKey.Scope scope, String receiverIncarnation, String targetId, String bindingId) {
    public TargetBinding {
        Objects.requireNonNull(scope, "scope");
        NotificationChannel.Destination.validateIdentity(receiverIncarnation, "receiverIncarnation");
        NotificationChannel.Destination.validateIdentity(targetId, "targetId");
        NotificationChannel.Destination.validateIdentity(bindingId, "bindingId");
    }

    public boolean matches(NotificationChannel.Request request) {
        return scope.equals(request.destination().scope()) && receiverIncarnation.equals(request.destination().receiverIncarnation())
                && targetId.equals(request.destination().targetId()) && bindingId.equals(request.destination().bindingId());
    }
}
