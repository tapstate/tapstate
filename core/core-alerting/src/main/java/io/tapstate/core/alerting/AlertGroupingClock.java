package io.tapstate.core.alerting;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/** Three clocks for one stable group. Returned progress and its new intent must commit together. */
public record AlertGroupingClock(Instant firstCandidateAt, Instant lastPlannedAt, boolean changed,
        long round, Instant reminderEligibleAt) {
    public enum Reason { INITIAL, CHANGE, REPEAT }
    public record Policy(Duration initialWait, Duration changeInterval, Duration repeatInterval) {
        public Policy {
            initialWait = AlertValidation.duration(initialWait, "initialWait", true, Duration.ofHours(1));
            changeInterval = AlertValidation.duration(changeInterval, "changeInterval", false, Duration.ofDays(1));
            repeatInterval = AlertValidation.duration(repeatInterval, "repeatInterval", false, Duration.ofDays(1));
        }
    }
    public record Candidate(Reason reason, long round, Instant eligibleAt) { }
    public static AlertGroupingClock empty() { return new AlertGroupingClock(null, null, false, 0, null); }
    public AlertGroupingClock changed(Instant now) {
        Objects.requireNonNull(now, "now");
        return new AlertGroupingClock(firstCandidateAt == null ? now : firstCandidateAt,
                lastPlannedAt, true, round, reminderEligibleAt);
    }
    public Optional<Candidate> due(Policy policy, Instant now, boolean hasEligibleMembers, boolean previousUnfinished) {
        if (!hasEligibleMembers || firstCandidateAt == null) { return Optional.empty(); }
        Reason reason = lastPlannedAt == null ? Reason.INITIAL : changed ? Reason.CHANGE : Reason.REPEAT;
        if (reason == Reason.REPEAT && previousUnfinished) { return Optional.empty(); }
        Instant at = lastPlannedAt == null ? firstCandidateAt.plus(policy.initialWait())
                : lastPlannedAt.plus(changed ? policy.changeInterval() : policy.repeatInterval());
        if (reason == Reason.REPEAT && reminderEligibleAt != null && reminderEligibleAt.isAfter(at)) { at = reminderEligibleAt; }
        return now.isBefore(at) ? Optional.empty() : Optional.of(new Candidate(reason, Math.incrementExact(round), at));
    }
    /** Called only as part of successfully planning the exact returned candidate. */
    public AlertGroupingClock planned(Candidate candidate, Instant plannedAt) {
        if (candidate.round() != Math.incrementExact(round) || plannedAt.isBefore(candidate.eligibleAt())) {
            throw new IllegalStateException("group candidate does not match planning progress");
        }
        return new AlertGroupingClock(firstCandidateAt, plannedAt, false, candidate.round(), reminderEligibleAt);
    }
    /** originalFixedEnd belongs to the coalesced predecessor; the host applies this once by identity. */
    public AlertGroupingClock coalescedPredecessorEnded(Instant originalFixedEnd, Policy policy) {
        Instant lower = originalFixedEnd.plus(policy.repeatInterval());
        if (reminderEligibleAt != null && reminderEligibleAt.isAfter(lower)) { lower = reminderEligibleAt; }
        return new AlertGroupingClock(firstCandidateAt, lastPlannedAt, changed, round, lower);
    }
}
