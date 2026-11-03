package io.tapstate.adapters.mongostore;

import io.tapstate.core.alerting.*;
import java.time.Duration;
import java.util.List;
import static io.tapstate.adapters.mongostore.AlertDeliveryFixtures.*;

final class AlertScheduleFixtures {
    static final AlertGroupingClock.Policy GROUPING = new AlertGroupingClock.Policy(Duration.ofSeconds(10), Duration.ofSeconds(30), Duration.ofSeconds(120));
    private AlertScheduleFixtures() { }
    static AlertSchedule.Record root() {
        var intent = business(); var frozen = intent.frozen(); var key = ((AlertIntent.Business) frozen.identity()).key();
        return new AlertSchedule.Record(AlertSchedule.Key.forDelivery(AlertSchedule.SubjectKind.EPISODE, key), frozen.route(), frozen.retention(), AT,
                AlertOwnership.unclaimed(), GROUPING, AlertGroupingClock.empty().changed(AT), 0, frozen.members(), null, null, List.of(), List.of(), null, false);
    }
    static AlertSchedule.Record planned(AlertSchedule.Record before, DeliveryKey intent) {
        var clock = before.clock().planned(new AlertGroupingClock.Candidate(AlertGroupingClock.Reason.INITIAL, 1, AT.plusSeconds(10)), AT.plusSeconds(10));
        return new AlertSchedule.Record(before.key(), before.route(), before.retention(), before.createdAt(), before.owner(), before.grouping(), clock,
                0, List.of(), null, null, List.of(), List.of(intent), null, false, before.members());
    }
    static AlertIntent.Record groupedIntent() {
        var original = business(); var f = original.frozen(); var previous = ((AlertIntent.Business) f.identity()).key();
        var key = DeliveryKey.NotificationRound.forGroup(SCOPE, "group-a", UUID, 1, previous.notification().routeId(), previous.notification().routeIncarnation(), DeliveryKey.Kind.FAULT, 1);
        var delivery = new DeliveryKey(key, previous.channelType(), previous.receiverIncarnation(), previous.targetId());
        var first = f.members().getFirst(); var old = first.evaluation();
        var secondAlarm = new AlarmKey(SCOPE, new AlarmKey.Resource("pipeline", "orders-b", "pipeline-b"), old.key().typeId(), old.key().qualifier());
        var second = new AlertIntent.Member(new AlertRule(secondAlarm, old.revision(), old.generation(), old.producerIncarnation(),
                old.triggerFor(), old.resolveFor(), old.maxAge(), old.maxGap()), 1, first.ruleIdentity(), 0);
        var frozen = new AlertIntent.Frozen(new AlertIntent.Business(delivery), f.channel(), f.route(), f.target(), f.content(), List.of(first, second), f.retry(), f.retention(), AT, f.deadline());
        return initial(frozen, PAYLOAD, null);
    }
    static AlertSchedule.Record groupBatch(AlertIntent.Record intent) {
        var f = intent.frozen(); var key = ((AlertIntent.Business) f.identity()).key();
        return new AlertSchedule.Record(AlertSchedule.Key.forDelivery(AlertSchedule.SubjectKind.GROUP, key), f.route(), f.retention(), AT,
                AlertOwnership.unclaimed(), GROUPING, new AlertGroupingClock(AT, AT.plusSeconds(10), false, 1, null), 1, List.of(),
                new AlertSchedule.Batch(key, 1, f.members()), null, List.of(), List.of(key), null, false);
    }
}
