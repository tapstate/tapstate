package io.tapstate.core.alerting;

import io.tapstate.core.alerting.DeliveryEvidence.Outcome;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** One immutable permit and its evidence. The host owns durable budgets, owner fencing, result-window checks and I/O cancellation. */
public final class DeliveryAttempt {
    public record Permit(AlarmKey.Scope scope, String deliveryId, String attemptId, int ordinal,
            long ownerGeneration, String ownerId, Instant grantedAt, Instant deadline) {
        public Permit {
            Objects.requireNonNull(scope, "scope");
            deliveryId = transportId(deliveryId, "deliveryId");
            attemptId = transportId(attemptId, "attemptId");
            ownerId = AlertValidation.text(ownerId, "ownerId", 256);
            AlertValidation.require(ordinal > 0 && ordinal <= 30 && ownerGeneration > 0, "permit", "invalid-ordinal-or-generation");
            AlertValidation.require(grantedAt != null && deadline != null && deadline.isAfter(grantedAt), "attemptDeadline", "not-after-grant");
        }
        /** Shared by the transport port so the exact granted identifiers remain representable. */
        public static String transportId(String value, String field) {
            AlertValidation.require(value != null && value.matches("[A-Za-z0-9._-]{1,256}"), field, "invalid-transport-id");
            return value;
        }
    }
    public record Update(DeliveryAttempt attempt, boolean ignored) { }
    private final Permit permit;
    private final Outcome outcome;
    private final Instant endedAt;
    private DeliveryAttempt(Permit permit, Outcome outcome, Instant endedAt) {
        this.permit = permit; this.outcome = outcome; this.endedAt = endedAt;
    }
    public Permit permit() { return permit; }
    public Outcome outcome() { return outcome; }
    public Instant endedAt() { return endedAt; }
    public boolean pending() { return endedAt == null; }

    /** This returns a candidate only; the host must atomically consume the next ordinal and persist this exact permit before sending. */
    public static Optional<DeliveryAttempt> grant(AlarmKey.Scope scope, String deliveryId, String attemptId, int ordinal,
            long ownerGeneration, String ownerId, ChannelRetryPolicy policy, Instant createdAt, Instant originalDeadline, Instant grantedAt) {
        Objects.requireNonNull(policy, "policy"); Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(originalDeadline, "originalDeadline"); Objects.requireNonNull(grantedAt, "grantedAt");
        AlertValidation.require(ordinal > 0 && ordinal <= policy.maximumTotalAttempts(), "ordinal", "outside-original-budget");
        AlertValidation.require(!grantedAt.isBefore(createdAt) && originalDeadline.isAfter(createdAt)
                && !originalDeadline.isAfter(DeliveryRetention.add(createdAt, policy.maximumAge())), "originalDeadline", "outside-original-age");
        return policy.attemptDeadline(grantedAt, originalDeadline).map(deadline -> new DeliveryAttempt(
                new Permit(scope, deliveryId, attemptId, ordinal, ownerGeneration, ownerId, grantedAt, deadline), Outcome.UNKNOWN, null));
    }
    /** Repeated takeovers use the original cutoff, regardless of the caller's recovery time. */
    public DeliveryAttempt settleDeadline(Instant now) {
        Objects.requireNonNull(now, "now");
        return pending() && !now.isBefore(permit.deadline()) ? new DeliveryAttempt(permit, Outcome.UNKNOWN, permit.deadline()) : this;
    }
    /** Called only after the host verifies the original permit and open result window in its conditional transaction. */
    public Update receipt(Permit originalPermit, Outcome result, Instant recordedAt) {
        Objects.requireNonNull(originalPermit, "originalPermit"); Objects.requireNonNull(result, "result"); Objects.requireNonNull(recordedAt, "recordedAt");
        if (!permit.equals(originalPermit)) { return new Update(this, true); }
        AlertValidation.require(result != Outcome.NOT_ATTEMPTED && !recordedAt.isBefore(permit.grantedAt()), "attemptResult", "invalid-permitted-result");
        DeliveryAttempt settled = settleDeadline(recordedAt);
        Instant fixedEnd = settled.endedAt != null ? settled.endedAt : recordedAt;
        Outcome merged = settled.pending() ? result : DeliveryEvidence.aggregate(List.of(settled.outcome, result));
        if (settled.endedAt != null && merged == settled.outcome) { return new Update(settled, false); }
        return new Update(new DeliveryAttempt(permit, merged, fixedEnd), false);
    }
    public DeliveryEvidence.Previous previousEvidence() {
        return new DeliveryEvidence.Previous(outcome, false, pending() ? permit.deadline() : null);
    }
}
