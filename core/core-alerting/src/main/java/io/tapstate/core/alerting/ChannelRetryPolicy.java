package io.tapstate.core.alerting;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Frozen policy values. The host atomically stores each chosen next time and consumed budget. */
public record ChannelRetryPolicy(int attemptsPerCycle, int maximumManualCycles,
        List<Duration> retryDelays, Duration maximumAge, Duration attemptTimeout) {
    public ChannelRetryPolicy {
        retryDelays = List.copyOf(retryDelays);
        AlertValidation.require(attemptsPerCycle >= 1 && attemptsPerCycle <= 10, "attemptsPerCycle", "outside-1-10");
        AlertValidation.require(maximumManualCycles >= 0 && maximumManualCycles <= 2, "maximumManualCycles", "outside-0-2");
        AlertValidation.require(retryDelays.size() == attemptsPerCycle - 1, "retryDelays", "must-match-attempt-count");
        retryDelays.forEach(delay -> AlertValidation.duration(delay, "retryDelay", true, Duration.ofDays(1)));
        maximumAge = AlertValidation.duration(maximumAge, "maximumAge", false, Duration.ofDays(1));
        attemptTimeout = AlertValidation.duration(attemptTimeout, "attemptTimeout", false, maximumAge);
    }
    public static ChannelRetryPolicy emailDefaults() {
        return new ChannelRetryPolicy(5, 2, List.of(Duration.ofMinutes(30), Duration.ofHours(1),
                Duration.ofHours(2), Duration.ofHours(4)), Duration.ofDays(1), Duration.ofSeconds(60));
    }
    public static ChannelRetryPolicy webhookDefaults() {
        return new ChannelRetryPolicy(5, 2, List.of(Duration.ofSeconds(30), Duration.ofMinutes(2),
                Duration.ofMinutes(10), Duration.ofMinutes(30)), Duration.ofDays(1), Duration.ofSeconds(30));
    }
    /** Used by explicit configuration tests and the local SDK placeholder, with no appended cycles. */
    public static ChannelRetryPolicy singleAttempt(Duration maximumAge, Duration timeout) {
        AlertValidation.duration(maximumAge, "singleAttemptAge", false, Duration.ofMinutes(5));
        return new ChannelRetryPolicy(1, 0, List.of(), maximumAge, timeout);
    }
    public int maximumTotalAttempts() { return attemptsPerCycle * (1 + maximumManualCycles); }

    public Optional<Instant> attemptDeadline(Instant grantedAt, Instant originalDeadline) {
        Objects.requireNonNull(grantedAt, "grantedAt");
        Objects.requireNonNull(originalDeadline, "originalDeadline");
        if (!grantedAt.isBefore(originalDeadline)) { return Optional.empty(); }
        Instant bounded = grantedAt.plus(attemptTimeout);
        return Optional.of(bounded.isBefore(originalDeadline) ? bounded : originalDeadline);
    }

    /** jitterBasisPoints is supplied once by the host and must be committed with the returned time. */
    public Optional<Instant> nextAttemptAt(int attemptsConsumedInCycle, Instant fixedEnd,
            int jitterBasisPoints, Instant originalDeadline, Instant retryAfter, Instant targetNotBefore) {
        AlertValidation.require(attemptsConsumedInCycle >= 1 && attemptsConsumedInCycle <= attemptsPerCycle,
                "attemptsConsumed", "outside-cycle-budget");
        AlertValidation.require(jitterBasisPoints >= 0 && jitterBasisPoints <= 2000, "jitter", "outside-0-20-percent");
        Objects.requireNonNull(fixedEnd, "fixedEnd");
        Objects.requireNonNull(originalDeadline, "originalDeadline");
        if (attemptsConsumedInCycle == attemptsPerCycle) { return Optional.empty(); }
        Duration base = retryDelays.get(attemptsConsumedInCycle - 1);
        Instant next = fixedEnd.plus(base).plus(base.multipliedBy(jitterBasisPoints).dividedBy(10000));
        if (retryAfter != null && retryAfter.isAfter(next)) { next = retryAfter; }
        if (targetNotBefore != null && targetNotBefore.isAfter(next)) { next = targetNotBefore; }
        return next.isBefore(originalDeadline) ? Optional.of(next) : Optional.empty();
    }
}
