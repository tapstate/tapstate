package io.tapstate.core.alerting;

import java.time.Instant;
import java.util.List;

/** Pure durable progress candidates. These methods never grant a request or perform delivery. */
public final class AlertIntentControl {
    private AlertIntentControl() { }
    public static AlertIntent.Record stop(AlertIntent.Record record, AlertIntent.StopReason reason, Instant now) {
        var s = record.state();
        Instant at = now.isAfter(record.frozen().deadline()) ? record.frozen().deadline() : now;
        if (s.permitsStopped()) at = s.permitsStoppedAt();
        var attempts = s.attempts().stream().map(a -> {
            var ended = a.value().settleDeadline(now);
            return ended.endedAt() == a.endedAt() ? a : new AlertIntent.Attempt(a.permit(), ended.outcome(), ended.endedAt(),
                    a.acceptedRecordedAt(), a.protocol(), a.receipt());
        }).toList();
        Instant last = attempts.stream().map(AlertIntent.Attempt::endedAt).filter(java.util.Objects::nonNull).max(Instant::compareTo).orElse(null);
        var retained = s.retained().value(record.frozen());
        if (attempts.stream().noneMatch(a -> a.endedAt() == null) && retained.stoppedAt() == null) {
            Instant fixed = last != null && last.isAfter(at) ? last : at;
            Instant floor = attempts.stream().map(a -> a.permit().deadline()).max(Instant::compareTo).orElse(record.frozen().createdAt());
            retained = retained.stop(fixed, floor);
        }
        return new AlertIntent.Record(record.frozen(), new AlertIntent.State(s.owner(), s.manualCycles(), true,
                s.permitsStopped() ? s.stopReason() : reason, null, s.attemptsConsumed(), attempts,
                s.detailsPrunedAt() == null ? DeliveryEvidence.aggregate(attempts.stream().map(AlertIntent.Attempt::outcome).toList()) : s.summary(),
                AlertIntent.Retained.capture(retained), s.payload(), s.candidate(), s.payloadPrunedAt(), s.candidatePrunedAt(), s.detailsPrunedAt(),
                at, last, s.cycleStarts(), s.acceptedReceipt()));
    }
    public static AlertIntent.Record claim(AlertIntent.Record record, String worker, Instant now, Instant until) {
        var s = record.state(); var old = s.owner();
        if (old.activeAt(now) && !old.ownerId().equals(worker)) return record;
        long generation = old.activeAt(now) ? old.generation() : Math.incrementExact(old.generation());
        if (old.activeAt(now) && old.leaseUntil().isAfter(until)) until = old.leaseUntil();
        var owner = new AlertOwnership(generation, worker, until);
        return new AlertIntent.Record(record.frozen(), new AlertIntent.State(owner, s.manualCycles(), s.permitsStopped(), s.stopReason(),
                s.nextAttemptAt(), s.attemptsConsumed(), s.attempts(), s.summary(), s.retained(), s.payload(), s.candidate(), s.payloadPrunedAt(),
                s.candidatePrunedAt(), s.detailsPrunedAt(), s.permitsStoppedAt(), s.lastAttemptEnd(), s.cycleStarts(), s.acceptedReceipt()));
    }
}
