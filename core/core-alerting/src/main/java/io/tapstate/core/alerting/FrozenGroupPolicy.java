package io.tapstate.core.alerting;

import io.tapstate.core.alerting.DeliveryEvidence.Outcome;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Membership-disposition candidates only. Stopping the original intent and preserving pending members requires one host transaction. */
public final class FrozenGroupPolicy {
    private FrozenGroupPolicy() { }
    public static final int MAX_MEMBERS = 100;
    public record Member(AlarmKey key, long episode) {
        public Member {
            Objects.requireNonNull(key, "key"); AlertValidation.require(episode > 0, "episode", "must-be-positive");
        }
    }
    public record Batch(DeliveryKey deliveryKey, long revision, List<Member> members) {
        public Batch {
            Objects.requireNonNull(deliveryKey, "deliveryKey"); members = List.copyOf(members);
            AlertValidation.require(revision > 0 && !members.isEmpty() && members.size() <= MAX_MEMBERS, "frozenGroup", "invalid-version-or-size");
            AlertValidation.require(deliveryKey.notification().kind() == DeliveryKey.Kind.FAULT || deliveryKey.notification().kind() == DeliveryKey.Kind.REMINDER,
                    "frozenGroup", "not-a-fault-batch");
            AlertValidation.require(members.stream().map(Member::key).distinct().count() == members.size(), "frozenGroup", "duplicate-alarm");
            AlertValidation.require(members.stream().allMatch(member -> member.key().scope().equals(deliveryKey.notification().scope())),
                    "frozenGroup", "foreign-scope");
        }
    }
    /** One conditional disposition identity, never a replacement delivery ID or evidence that its transaction committed. */
    public record DispositionKey(DeliveryKey originalDelivery, long originalRevision) { }
    public enum Action { KEEP_ORIGINAL, STOP_WAIT, STOP_NO_SUCCESSOR, STOP_COVERED, STOP_PENDING }
    public record Decision(DispositionKey key, Action action, List<Member> pendingMembers, Instant recheckAt, Outcome previousOutcome) {
        public Decision { pendingMembers = List.copyOf(pendingMembers); }
        public boolean stopOriginalPermits() { return action != Action.KEEP_ORIGINAL; }
    }
    /** eligibleOriginalMembers excludes new joiners; the host handles them in ordinary group progress and rechecks all current permissions. */
    public static Decision reconcile(Batch batch, Set<Member> eligibleOriginalMembers,
            List<DeliveryEvidence.Previous> originalAttempts, Instant now) {
        Objects.requireNonNull(batch, "batch"); Objects.requireNonNull(now, "now");
        Set<Member> eligible = Set.copyOf(eligibleOriginalMembers); List<DeliveryEvidence.Previous> attempts = List.copyOf(originalAttempts);
        AlertValidation.require(batch.members().containsAll(eligible), "eligibleMembers", "not-from-original-batch");
        AlertValidation.require(attempts.size() <= 30, "groupAttempts", "outside-attempt-budget");
        var key = new DispositionKey(batch.deliveryKey(), batch.revision());
        List<Member> survivors = batch.members().stream().filter(eligible::contains).toList();
        if (survivors.size() == batch.members().size()) { return new Decision(key, Action.KEEP_ORIGINAL, List.of(), null, Outcome.NOT_ATTEMPTED); }
        Instant recheck = null; var outcomes = new ArrayList<Outcome>();
        for (var attempt : attempts) {
            Instant deadline = attempt.inFlightDeadline();
            if (deadline != null && deadline.isAfter(now)) {
                if (recheck == null || deadline.isBefore(recheck)) { recheck = deadline; }
            }
            outcomes.add(attempt.outcome());
            if (deadline != null && !deadline.isAfter(now)) { outcomes.add(Outcome.UNKNOWN); }
        }
        Outcome previous = DeliveryEvidence.aggregate(outcomes);
        if (survivors.isEmpty()) { return new Decision(key, Action.STOP_NO_SUCCESSOR, List.of(), recheck, previous); }
        if (recheck != null) { return new Decision(key, Action.STOP_WAIT, survivors, recheck, previous); }
        if (previous == Outcome.ACCEPTED) { return new Decision(key, Action.STOP_COVERED, List.of(), null, previous); }
        return new Decision(key, Action.STOP_PENDING, survivors, null, previous);
    }
}
