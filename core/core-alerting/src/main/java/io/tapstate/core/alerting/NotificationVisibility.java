package io.tapstate.core.alerting;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/** Personal visibility is separate from shared retention. Current authorization and conditional writes belong to the host. */
public final class NotificationVisibility {
    public record Receipt(AlarmKey.Scope scope, String notificationId, String principalId, Instant readAt,
            Instant visibleUntil, Instant markerUntil) {
        public Receipt {
            Objects.requireNonNull(scope, "scope");
            notificationId = AlertValidation.text(notificationId, "notificationId", 256);
            principalId = AlertValidation.text(principalId, "principalId", 256);
            Objects.requireNonNull(readAt, "readAt"); Objects.requireNonNull(visibleUntil, "visibleUntil"); Objects.requireNonNull(markerUntil, "markerUntil");
        }
    }
    private final AlarmKey.Scope scope;
    private final String notificationId;
    private final DeliveryRetention retention;
    public NotificationVisibility(AlarmKey.Scope scope, String notificationId, DeliveryRetention retention) {
        this.scope = Objects.requireNonNull(scope, "scope");
        this.notificationId = AlertValidation.text(notificationId, "notificationId", 256);
        this.retention = Objects.requireNonNull(retention, "retention");
        AlertValidation.require(retention.kind() == DeliveryRetention.Kind.IN_APP && retention.acceptedAt() != null,
                "notification", "not-published");
    }
    public Instant sharedVisibleUntil() { return retention.payloadUntil(); }
    public Optional<Receipt> markRead(String principalId, Instant now, Receipt existing) {
        validate(principalId, existing); Objects.requireNonNull(now, "now");
        if (!sharedVisible(now)) { return Optional.empty(); }
        if (existing != null) {
            AlertValidation.require(!existing.readAt().isAfter(now), "readAt", "future-receipt");
            return Optional.of(existing);
        }
        Instant personal = DeliveryRetention.min(sharedVisibleUntil(), DeliveryRetention.add(now,
                retention.policy().duration(AlertRetentionPolicy.Setting.READ_NOTIFICATION)));
        return Optional.of(new Receipt(scope, notificationId, principalId, now, personal, sharedVisibleUntil()));
    }
    public boolean visibleFor(String principalId, Instant now, Receipt receipt) {
        validate(principalId, receipt);
        return sharedVisible(now) && (receipt == null || (!now.isBefore(receipt.readAt()) && now.isBefore(receipt.visibleUntil())));
    }
    public boolean unreadFor(String principalId, Instant now, Receipt receipt) {
        return visibleFor(principalId, now, receipt) && receipt == null;
    }
    private boolean sharedVisible(Instant now) {
        return !now.isBefore(retention.acceptedAt()) && now.isBefore(sharedVisibleUntil());
    }
    private void validate(String principalId, Receipt receipt) {
        AlertValidation.text(principalId, "principalId", 256);
        if (receipt == null) { return; }
        Instant expected = DeliveryRetention.min(sharedVisibleUntil(), DeliveryRetention.add(receipt.readAt(),
                retention.policy().duration(AlertRetentionPolicy.Setting.READ_NOTIFICATION)));
        AlertValidation.require(receipt.scope().equals(scope) && receipt.notificationId().equals(notificationId)
                && receipt.principalId().equals(principalId) && !receipt.readAt().isBefore(retention.acceptedAt())
                && receipt.readAt().isBefore(sharedVisibleUntil()) && receipt.visibleUntil().equals(expected)
                && receipt.markerUntil().equals(sharedVisibleUntil()), "receipt", "foreign-or-invalid-notification-receipt");
    }
}
