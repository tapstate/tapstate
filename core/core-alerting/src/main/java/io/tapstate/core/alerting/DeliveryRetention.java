package io.tapstate.core.alerting;

import static io.tapstate.core.alerting.AlertRetentionPolicy.Setting.*;
import io.tapstate.core.alerting.AlertChannelPolicy.Deployment;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/** Monotone cutoff candidates, not permission to prune. The host must still settle attempts and references atomically. */
public final class DeliveryRetention {
    public enum Kind { OP_EXTERNAL, CLOUD_SDK, CHANNEL_TEST }
    private final AlertRetentionPolicy policy;
    private final Kind kind;
    private final Instant createdAt, originalDeadline, payloadUntil, resultUntil, acceptedAt, stoppedAt;
    private DeliveryRetention(AlertRetentionPolicy policy, Kind kind, Instant createdAt, Instant originalDeadline,
            Instant payloadUntil, Instant resultUntil, Instant acceptedAt, Instant stoppedAt) {
        this.policy = policy; this.kind = kind; this.createdAt = createdAt; this.originalDeadline = originalDeadline;
        this.payloadUntil = payloadUntil; this.resultUntil = resultUntil; this.acceptedAt = acceptedAt; this.stoppedAt = stoppedAt;
    }
    public AlertRetentionPolicy policy() { return policy; }
    public Kind kind() { return kind; }
    public Instant createdAt() { return createdAt; }
    public Instant originalDeadline() { return originalDeadline; }
    public Instant payloadUntil() { return payloadUntil; }
    public Instant resultUntil() { return resultUntil; }
    public Instant acceptedAt() { return acceptedAt; }
    public Instant stoppedAt() { return stoppedAt; }

    public static DeliveryRetention begin(AlertRetentionPolicy policy, Kind kind, Instant createdAt, Instant deadline) {
        Objects.requireNonNull(policy, "policy"); Objects.requireNonNull(kind, "kind");
        Objects.requireNonNull(createdAt, "createdAt"); Objects.requireNonNull(deadline, "deadline");
        boolean cloud = policy.deployment() == Deployment.CLOUD_MANAGED;
        AlertValidation.require((cloud ? kind == Kind.CLOUD_SDK : kind == Kind.OP_EXTERNAL || kind == Kind.CHANNEL_TEST),
                "retention.kind", "not-permitted-for-deployment");
        Duration age = kind == Kind.CHANNEL_TEST ? Duration.ofMinutes(5) : Duration.ofDays(1);
        AlertValidation.require(deadline.isAfter(createdAt) && !deadline.isAfter(add(createdAt, age)), "deadline", "outside-original-age");
        Instant payload = kind == Kind.CHANNEL_TEST ? add(createdAt, policy.duration(TEST_PAYLOAD))
                : add(deadline, policy.duration(FAILED_PAYLOAD));
        Instant result = add(deadline, policy.duration(kind == Kind.CHANNEL_TEST ? TEST_FAILED_RESULT : FAILED_RESULT));
        return new DeliveryRetention(policy, kind, createdAt, deadline, payload, result, null, null);
    }

    /** acceptedRecordedAt is the first durable receipt time. Expired detail windows cannot be reopened. */
    public Optional<DeliveryRetention> accept(Instant acceptedRecordedAt, Instant evidenceProtectedUntil, Instant now) {
        Objects.requireNonNull(now, "now"); validateEvidenceFloor(evidenceProtectedUntil);
        AlertValidation.require(acceptedRecordedAt != null && !acceptedRecordedAt.isBefore(createdAt)
                && !acceptedRecordedAt.isAfter(now), "acceptedAt", "outside-record-lifetime");
        if (!hasResultWindow(now)) { return Optional.empty(); }
        if (acceptedAt != null) { return Optional.of(this); }
        Instant payload = kind == Kind.CHANNEL_TEST ? payloadUntil
                : add(acceptedRecordedAt, policy.acceptedPayload());
        Duration success = policy.duration(kind == Kind.CHANNEL_TEST ? TEST_ACCEPTED_RESULT : ACCEPTED_RESULT);
        Instant result = max(add(acceptedRecordedAt, success), evidenceFloor(evidenceProtectedUntil));
        return Optional.of(new DeliveryRetention(policy, kind, createdAt, originalDeadline,
                min(payloadUntil, payload), min(resultUntil, result), acceptedRecordedAt, stoppedAt));
    }

    /** Fixed only once after no new permit is possible and original attempts have settled. Exhaustion alone is insufficient. */
    public DeliveryRetention stop(Instant fixedStoppedAt, Instant evidenceProtectedUntil) {
        validateEvidenceFloor(evidenceProtectedUntil);
        AlertValidation.require(fixedStoppedAt != null && !fixedStoppedAt.isBefore(createdAt)
                && !fixedStoppedAt.isAfter(originalDeadline), "stoppedAt", "outside-original-lifetime");
        if (stoppedAt != null) { return this; }
        Instant payload = payloadUntil, result = resultUntil;
        if (acceptedAt == null) {
            if (kind != Kind.CHANNEL_TEST) { payload = min(payload, add(fixedStoppedAt, policy.duration(FAILED_PAYLOAD))); }
            result = min(result, max(add(fixedStoppedAt, policy.duration(kind == Kind.CHANNEL_TEST ? TEST_FAILED_RESULT : FAILED_RESULT)),
                    evidenceFloor(evidenceProtectedUntil)));
        }
        return new DeliveryRetention(policy, kind, createdAt, originalDeadline, payload, result, acceptedAt, fixedStoppedAt);
    }
    public boolean hasResultWindow(Instant now) { return !now.isBefore(createdAt) && now.isBefore(resultUntil); }
    private void validateEvidenceFloor(Instant protectedUntil) {
        AlertValidation.require(protectedUntil != null && !protectedUntil.isBefore(createdAt)
                && !protectedUntil.isAfter(kind == Kind.CHANNEL_TEST ? max(originalDeadline, add(createdAt, Duration.ofMinutes(5))) : originalDeadline),
                "evidenceProtectedUntil", "outside-original-permission-window");
    }
    private Instant evidenceFloor(Instant protectedUntil) {
        return kind == Kind.CHANNEL_TEST ? max(protectedUntil, add(createdAt, Duration.ofMinutes(5))) : protectedUntil;
    }
    static Instant add(Instant time, Duration duration) {
        try { return time.plus(duration); }
        catch (DateTimeException | ArithmeticException overflow) {
            AlertValidation.require(false, "retention.time", "overflow"); throw new AssertionError("unreachable");
        }
    }
    static Instant min(Instant a, Instant b) { return a.isBefore(b) ? a : b; }
    static Instant max(Instant a, Instant b) { return a.isAfter(b) ? a : b; }
}
