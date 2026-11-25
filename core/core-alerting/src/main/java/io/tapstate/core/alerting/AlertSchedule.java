package io.tapstate.core.alerting;

import io.tapstate.core.alerting.AlertChannelPolicy.Deployment;
import io.tapstate.core.alerting.DeliveryEvidence.Outcome;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** Bounded planning and summary facts. The planner must commit each required intent and state change in the same transaction. */
public final class AlertSchedule {
    public static final int FORMAT_VERSION = 1;
    public static final int MAX_MEMBERS = 100;
    public static final int MAX_REFERENCES = 100;
    public static final int MAX_BYTES = 1024 * 1024;
    private AlertSchedule() { }
    public enum SubjectKind { EPISODE, GROUP, EVENT }
    public record Key(AlarmKey.Scope scope, SubjectKind kind, String subjectId, String subjectIncarnation,
            String routeId, String routeIncarnation, String channelType, String receiverIncarnation, String targetId) {
        public Key {
            Objects.requireNonNull(scope, "scope"); Objects.requireNonNull(kind, "kind");
            subjectId = text(subjectId, "scheduleSubject"); subjectIncarnation = text(subjectIncarnation, "scheduleIncarnation");
            routeId = text(routeId, "routeId"); routeIncarnation = text(routeIncarnation, "routeIncarnation");
            channelType = AlertValidation.text(channelType, "channelType", 64);
            receiverIncarnation = text(receiverIncarnation, "receiverIncarnation"); targetId = text(targetId, "targetId");
        }
        public static Key forDelivery(SubjectKind kind, DeliveryKey key) {
            var n = key.notification();
            return new Key(n.scope(), kind, n.subjectId(), n.subjectIncarnation(), n.routeId(), n.routeIncarnation(), key.channelType(),
                    key.receiverIncarnation(), key.targetId());
        }
        /** A stable group clock may reference its distinct frozen batches; its scope and destination never change. */
        public boolean contains(DeliveryKey key) {
            var n = key.notification();
            return scope.equals(n.scope()) && subjectId.equals(n.subjectId()) && (kind == SubjectKind.GROUP || subjectIncarnation.equals(n.subjectIncarnation()))
                    && routeId.equals(n.routeId()) && routeIncarnation.equals(n.routeIncarnation()) && channelType.equals(key.channelType())
                    && receiverIncarnation.equals(key.receiverIncarnation()) && targetId.equals(key.targetId())
                    && (kind == SubjectKind.EVENT ? n.kind() == DeliveryKey.Kind.EVENT : n.kind() != DeliveryKey.Kind.EVENT);
        }
    }
    public record Batch(DeliveryKey delivery, long revision, List<AlertIntent.Member> members) {
        public Batch {
            Objects.requireNonNull(delivery, "delivery"); members = checkedMembers(members, delivery.notification().scope(), false);
            AlertValidation.require(revision > 0 && (delivery.notification().kind() == DeliveryKey.Kind.FAULT || delivery.notification().kind() == DeliveryKey.Kind.REMINDER)
                    && members.stream().allMatch(member -> member.episode() > 0), "batch", "invalid-version-kind-or-episode");
        }
    }
    public record Disposition(DeliveryKey original, long originalRevision, FrozenGroupPolicy.Action action,
            List<AlertIntent.Member> pending, DeliveryKey successor, Instant recheckAt, Outcome previousOutcome) {
        public Disposition {
            Objects.requireNonNull(original, "original"); Objects.requireNonNull(action, "action"); Objects.requireNonNull(previousOutcome, "previousOutcome");
            pending = checkedMembers(pending, original.notification().scope(), true);
            AlertValidation.require(originalRevision > 0 && (successor == null || !successor.equals(original)), "disposition", "invalid-original-or-successor");
            switch (action) {
                case KEEP_ORIGINAL, STOP_NO_SUCCESSOR -> AlertValidation.require(pending.isEmpty() && successor == null, "disposition", "unexpected-successor-work");
                case STOP_COVERED -> AlertValidation.require(previousOutcome == Outcome.ACCEPTED
                        && (successor == null ? pending.isEmpty() : !pending.isEmpty()), "disposition", "missing-covered-evidence-or-successor-members");
                case STOP_WAIT -> AlertValidation.require(!pending.isEmpty() && successor == null && recheckAt != null, "disposition", "missing-original-wait");
                case STOP_PENDING -> AlertValidation.require(!pending.isEmpty() && recheckAt == null && previousOutcome != Outcome.ACCEPTED, "disposition", "invalid-pending-successor");
            }
        }
    }
    /** Final predecessor evidence only; unsettled requests remain protected by their original intent references. */
    public record Previous(AlarmKey alarm, long episode, Outcome outcome, boolean renderingFailed) {
        public Previous { Objects.requireNonNull(alarm, "alarm"); Objects.requireNonNull(outcome, "outcome"); AlertValidation.require(episode > 0, "summaryEpisode", "must-be-positive"); }
        public DeliveryEvidence.Previous evidence() { return new DeliveryEvidence.Previous(outcome, renderingFailed, null); }
    }
    public record Record(Key key, AlertIntent.Route route, AlertIntent.Policy retention, Instant createdAt, AlertOwnership owner,
            AlertGroupingClock.Policy grouping, AlertGroupingClock clock, long batchSequence, List<AlertIntent.Member> pending,
            Batch batch, Disposition disposition, List<Previous> summaries, List<DeliveryKey> protectedIntents,
            Instant retiredAt, boolean summariesHandedOff, List<AlertIntent.Member> members) {
        public Record(Key key, AlertIntent.Route route, AlertIntent.Policy retention, Instant createdAt, AlertOwnership owner,
                AlertGroupingClock.Policy grouping, AlertGroupingClock clock, long batchSequence, List<AlertIntent.Member> pending,
                Batch batch, Disposition disposition, List<Previous> summaries, List<DeliveryKey> protectedIntents,
                Instant retiredAt, boolean summariesHandedOff) {
            this(key, route, retention, createdAt, owner, grouping, clock, batchSequence, pending, batch, disposition,
                    summaries, protectedIntents, retiredAt, summariesHandedOff, batch == null ? pending : batch.members());
        }
        public Record {
            Objects.requireNonNull(key, "key"); Objects.requireNonNull(route, "route"); Objects.requireNonNull(retention, "retention");
            Objects.requireNonNull(createdAt, "createdAt"); Objects.requireNonNull(owner, "owner"); Objects.requireNonNull(grouping, "grouping"); Objects.requireNonNull(clock, "clock");
            pending = checkedMembers(pending, key.scope(), true); members = checkedMembers(members, key.scope(), true);
            summaries = List.copyOf(summaries); protectedIntents = List.copyOf(protectedIntents);
            AlertValidation.require(members.containsAll(pending), "pendingMembers", "not-from-current-membership");
            AlertValidation.require(route.id().equals(key.routeId()) && route.incarnation().equals(key.routeIncarnation())
                    && (route instanceof AlertIntent.ConfiguredRoute configured ? configured.identity().scope().equals(key.scope())
                    && retention.deployment() == Deployment.ON_PREM : retention.deployment() == Deployment.CLOUD_MANAGED), "scheduleRoute", "foreign-route-or-deployment");
            AlertValidation.require(batchSequence >= 0 && clock.round() >= 0
                    && (clock.round() == 0 ? clock.lastPlannedAt() == null : clock.firstCandidateAt() != null && clock.lastPlannedAt() != null)
                    && (clock.lastPlannedAt() == null || !clock.lastPlannedAt().isBefore(clock.firstCandidateAt())), "groupClock", "invalid-original-progress");
            AlertValidation.require(retiredAt == null || !retiredAt.isBefore(createdAt), "retiredAt", "before-schedule-creation");
            if (batch != null) {
                AlertValidation.require(key.kind() == SubjectKind.GROUP && key.subjectIncarnation().equals(batch.delivery().notification().subjectIncarnation())
                        && key.contains(batch.delivery()) && batchSequence > 0 && batch.delivery().notification().round() <= clock.round(),
                        "batch", "foreign-or-unplanned-batch");
            }
            if (disposition != null) {
                AlertValidation.require(batch != null && disposition.original().equals(batch.delivery()) && disposition.originalRevision() == batch.revision()
                        && batch.members().containsAll(disposition.pending()) && (disposition.successor() == null || key.contains(disposition.successor())),
                        "disposition", "not-from-original-frozen-batch");
                if (disposition.action() == FrozenGroupPolicy.Action.STOP_PENDING || disposition.action() == FrozenGroupPolicy.Action.STOP_WAIT) {
                    AlertValidation.require(disposition.pending().size() < batch.members().size() && members.containsAll(disposition.pending()),
                            "disposition", "not-a-proper-eligible-survivor-subset");
                }
            }
            AlertValidation.require(summaries.size() <= MAX_MEMBERS && summaries.stream().allMatch(value -> value.alarm().scope().equals(key.scope()))
                    && summaries.stream().map(value -> new FrozenGroupPolicy.Member(value.alarm(), value.episode())).distinct().count() == summaries.size(),
                    "summaries", "foreign-duplicate-or-unbounded");
            AlertValidation.require(protectedIntents.size() <= MAX_REFERENCES && protectedIntents.stream().allMatch(key::contains)
                    && protectedIntents.stream().distinct().count() == protectedIntents.size(), "protectedIntents", "foreign-duplicate-or-unbounded");
            AlertValidation.require(!summariesHandedOff || protectedIntents.isEmpty(), "summaryHandoff", "required-intents-still-referenced");
        }
        public boolean hasOpenWork() {
            return !pending.isEmpty() || disposition != null && (disposition.action() == FrozenGroupPolicy.Action.STOP_WAIT
                    || disposition.action() == FrozenGroupPolicy.Action.STOP_PENDING && disposition.successor() == null);
        }
        public Instant retiredCleanupAt() {
            return retiredAt == null ? null : DeliveryRetention.add(retiredAt,
                    retention.deployment() == Deployment.ON_PREM ? Duration.ofDays(7) : Duration.ofDays(1));
        }
    }
    private static List<AlertIntent.Member> checkedMembers(List<AlertIntent.Member> values, AlarmKey.Scope scope, boolean empty) {
        List<AlertIntent.Member> copy = List.copyOf(values);
        AlertValidation.require((empty || !copy.isEmpty()) && copy.size() <= MAX_MEMBERS
                && copy.stream().allMatch(member -> member.evaluation().key().scope().equals(scope))
                && copy.stream().map(member -> member.evaluation().key()).distinct().count() == copy.size(), "members", "foreign-duplicate-or-unbounded");
        return copy;
    }
    private static String text(String value, String field) { return AlertValidation.text(value, field, 256); }
}
