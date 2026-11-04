package io.tapstate.core.alerting;

import java.time.Instant;

/** Durable worker identity only; a value does not establish that a claim or renewal committed. */
public record AlertOwnership(long generation, String ownerId, Instant leaseUntil) {
    public AlertOwnership {
        AlertValidation.require(generation >= 0, "ownerGeneration", "negative");
        if (generation == 0) {
            AlertValidation.require(ownerId == null && leaseUntil == null, "owner", "unclaimed-has-no-lease");
        } else {
            ownerId = AlertValidation.text(ownerId, "ownerId", 256);
            AlertValidation.require(leaseUntil != null, "ownerLease", "required");
        }
    }
    public static AlertOwnership unclaimed() { return new AlertOwnership(0, null, null); }
    public boolean activeAt(Instant now) { return generation > 0 && now.isBefore(leaseUntil); }
    public boolean owns(DeliveryAttempt.Permit permit, Instant now) {
        return activeAt(now) && generation == permit.ownerGeneration() && ownerId.equals(permit.ownerId());
    }
}
