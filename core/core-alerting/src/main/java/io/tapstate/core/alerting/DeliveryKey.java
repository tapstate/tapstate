package io.tapstate.core.alerting;

import java.util.Objects;

/** The complete target key; attempt ids and mutable presentation never participate. */
public record DeliveryKey(NotificationRound notification, String channelType,
        String receiverIncarnation, String targetId) {
    public enum Kind { FAULT, REMINDER, RECOVERY, EVENT }
    public record NotificationRound(AlarmKey.Scope scope, String subjectId, String subjectIncarnation,
            String routeId, String routeIncarnation, Kind kind, long round) {
        public NotificationRound {
            Objects.requireNonNull(scope, "scope");
            Objects.requireNonNull(kind, "kind");
            subjectId = AlertValidation.text(subjectId, "notificationSubject", 256);
            subjectIncarnation = AlertValidation.text(subjectIncarnation, "subjectIncarnation", 256);
            routeId = AlertValidation.text(routeId, "routeId", 256);
            routeIncarnation = AlertValidation.text(routeIncarnation, "routeIncarnation", 256);
            AlertValidation.require(round > 0, "notificationRound", "must-be-positive");
        }
    }
    public DeliveryKey {
        Objects.requireNonNull(notification, "notification");
        channelType = AlertValidation.text(channelType, "channelType", 64);
        receiverIncarnation = AlertValidation.text(receiverIncarnation, "receiverIncarnation", 256);
        targetId = AlertValidation.text(targetId, "targetId", 256);
    }
}
