package io.tapstate.core.alerting;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Pure candidates for the original durable permit and result. No method performs IO. */
public final class AlertDeliveryProgress {
    private AlertDeliveryProgress() { }

    public static AlertIntent.Record grant(AlertIntent.Record record, String worker, String attemptId, Instant now) {
        var s = record.state(); var f = record.frozen();
        require(s.owner().activeAt(now) && worker.equals(s.owner().ownerId()), "original-owner");
        require(!s.permitsStopped() && s.nextAttemptAt() != null && !now.isBefore(s.nextAttemptAt())
                && s.currentCycleConsumed() < f.retry().attemptsPerCycle()
                && s.attempts().stream().noneMatch(a -> a.endedAt() == null), "original-eligibility");
        var value = DeliveryAttempt.grant(f.identity().scope(), f.identity().deliveryId(), attemptId,
                s.attemptsConsumed() + 1, s.owner().generation(), worker, f.retry(), f.createdAt(), f.deadline(), now).orElseThrow();
        var attempts = new ArrayList<>(s.attempts()); attempts.add(AlertIntent.Attempt.pending(value));
        boolean exhausted = s.manualCycles()==f.retry().maximumManualCycles() && s.currentCycleConsumed()+1==f.retry().attemptsPerCycle();
        return new AlertIntent.Record(f, new AlertIntent.State(s.owner(), s.manualCycles(), exhausted,
                exhausted ? AlertIntent.StopReason.EXHAUSTED : null, null, attempts.size(), attempts,
                DeliveryEvidence.aggregate(attempts.stream().map(AlertIntent.Attempt::outcome).toList()), s.retained(),
                s.payload(), s.candidate(), s.payloadPrunedAt(), s.candidatePrunedAt(), s.detailsPrunedAt(),
                exhausted ? now : null, s.lastAttemptEnd(), s.cycleStarts(), s.acceptedReceipt()));
    }

    /** Merges only a matching original permit while its original result window is open. */
    public static AlertIntent.Record result(AlertIntent.Record record, DeliveryAttempt.Permit permit,
            DeliveryEvidence.Outcome outcome, AlertIntent.Protocol protocol, AlertIntent.Receipt receipt,
            Instant now, int jitterBasisPoints) {
        var s = record.state(); var f = record.frozen();
        if (!s.retained().value(f).hasResultWindow(now) || s.detailsPrunedAt() != null) return record;
        int index = -1;
        for (int i = 0; i < s.attempts().size(); i++) if (s.attempts().get(i).permit().equals(permit)) index = i;
        if (index < 0) return record;
        var old = s.attempts().get(index);
        var merged = old.value().receipt(permit, outcome, now).attempt();
        boolean accepted = merged.outcome() == DeliveryEvidence.Outcome.ACCEPTED;
        if (old.protocol()!=null) {
            Instant restriction=old.protocol().retryAfter();
            if(protocol!=null && (restriction!=null && (protocol.retryAfter()==null || restriction.isAfter(protocol.retryAfter()))
                    || old.protocol().action()==AlertIntent.Action.STOP)) {
                protocol=new AlertIntent.Protocol(protocol.phase(),old.protocol().action()==AlertIntent.Action.STOP?AlertIntent.Action.STOP:protocol.action(),
                        protocol.status(),protocol.implementation(),restriction!=null&&(protocol.retryAfter()==null||restriction.isAfter(protocol.retryAfter()))?restriction:protocol.retryAfter());
            }
        }
        var evidence = new AlertIntent.Attempt(permit, merged.outcome(), merged.endedAt(),
                old.acceptedRecordedAt() != null ? old.acceptedRecordedAt() : accepted ? now : null,
                old.outcome() == DeliveryEvidence.Outcome.ACCEPTED ? old.protocol() : protocol,
                old.receipt() != null ? old.receipt() : accepted ? receipt : null);
        var attempts = new ArrayList<>(s.attempts()); attempts.set(index, evidence);
        var summary = DeliveryEvidence.aggregate(attempts.stream().map(AlertIntent.Attempt::outcome).toList());
        boolean stopped = s.permitsStopped(); var reason = s.stopReason(); var stopAt = s.permitsStoppedAt();
        if (!stopped && summary == DeliveryEvidence.Outcome.ACCEPTED) {
            stopped = true; reason = now.isAfter(f.deadline()) ? AlertIntent.StopReason.EXPIRED : AlertIntent.StopReason.ACCEPTED;
            stopAt = now.isAfter(f.deadline()) ? f.deadline() : now;
        }
        Instant next = s.nextAttemptAt();
        if (stopped) next = null;
        else if (index == attempts.size() - 1 && index >= s.cycleStarts().getLast()) {
            next = protocol == null || protocol.action() == AlertIntent.Action.RETRY
                    ? f.retry().nextAttemptAt(s.currentCycleConsumed(), merged.endedAt(), jitterBasisPoints, f.deadline(),
                            protocol == null ? null : protocol.retryAfter(), null).orElse(null) : null;
            if(next!=null && s.nextAttemptAt()!=null && s.nextAttemptAt().isAfter(next))next=s.nextAttemptAt();
            if (next == null && s.currentCycleConsumed() < f.retry().attemptsPerCycle()
                    && (protocol == null || protocol.action() == AlertIntent.Action.RETRY)) {
                stopped = true; reason = AlertIntent.StopReason.AGE_LIMIT; stopAt = merged.endedAt();
            }
        }
        Instant restriction=attempts.stream().map(AlertIntent.Attempt::protocol).filter(Objects::nonNull)
                .map(AlertIntent.Protocol::retryAfter).filter(Objects::nonNull).max(Instant::compareTo).orElse(null);
        if(!stopped && next!=null && restriction!=null && restriction.isAfter(next)) {
            if(restriction.isBefore(f.deadline())) next=restriction;
            else {next=null;stopped=true;reason=AlertIntent.StopReason.AGE_LIMIT;stopAt=now.isAfter(f.deadline())?f.deadline():now;}
        }
        Instant last = attempts.stream().map(AlertIntent.Attempt::endedAt).filter(Objects::nonNull).max(Instant::compareTo).orElse(null);
        Instant floor = attempts.stream().map(a -> a.permit().deadline()).max(Instant::compareTo).orElse(f.createdAt());
        var retained = s.retained().value(f);
        Instant fixed = stopped && attempts.stream().noneMatch(a -> a.endedAt() == null)
                ? last != null && last.isAfter(stopAt) ? last : stopAt : null;
        if (fixed != null && fixed.isBefore(now)) retained = retained.stop(fixed, floor);
        if (accepted) retained = retained.accept(evidence.acceptedRecordedAt(), floor, now).orElseThrow();
        if (fixed != null) retained = retained.stop(fixed, floor);
        var acceptedReceipt = s.acceptedReceipt();
        if (acceptedReceipt == null && accepted && receipt != null)
            acceptedReceipt = new AlertIntent.Receipt(receipt.source(), receipt.remoteId(), null);
        return new AlertIntent.Record(f, new AlertIntent.State(s.owner(), s.manualCycles(), stopped, reason, next,
                s.attemptsConsumed(), attempts, summary, AlertIntent.Retained.capture(retained), s.payload(), s.candidate(),
                s.payloadPrunedAt(), s.candidatePrunedAt(), s.detailsPrunedAt(), stopAt, last, s.cycleStarts(), acceptedReceipt));
    }

    public static AlertIntent.Record manual(AlertIntent.Record record, Instant now) {
        var s = record.state(); var f = record.frozen();
        require(!s.permitsStopped() && s.nextAttemptAt() == null && s.currentCycleConsumed() > 0
                && s.manualCycles() < f.retry().maximumManualCycles() && now.isBefore(f.deadline())
                && s.attempts().stream().allMatch(a -> a.endedAt() != null), "manual-cycle");
        var cycles = new ArrayList<>(s.cycleStarts()); cycles.add(s.attemptsConsumed());
        var next = s.attempts().stream().map(AlertIntent.Attempt::protocol).filter(Objects::nonNull)
                .map(AlertIntent.Protocol::retryAfter).filter(Objects::nonNull).max(Instant::compareTo).filter(t -> t.isAfter(now)).orElse(now);
        require(next.isBefore(f.deadline()), "original-age");
        return new AlertIntent.Record(f, new AlertIntent.State(s.owner(), s.manualCycles()+1, false, null, next,
                s.attemptsConsumed(), s.attempts(), s.summary(), s.retained(), s.payload(), s.candidate(), s.payloadPrunedAt(),
                s.candidatePrunedAt(), s.detailsPrunedAt(), null, s.lastAttemptEnd(), cycles, s.acceptedReceipt()));
    }
    private static void require(boolean condition, String reason) {
        if (!condition) throw new io.tapstate.core.common.TapstateException(AlertError.STALE_ACTION, java.util.Map.of("action",reason),null);
    }
}
