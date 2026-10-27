package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.alerting.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static io.tapstate.adapters.mongostore.AlertDeliveryFixtures.*;
import static io.tapstate.adapters.mongostore.AlertScheduleFixtures.*;
import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class MongoAlertScheduleStoreIT {
    @Container static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @Test void originalClocksMembersAndRequiredReferencesReopenWithoutCreatingAnIntent() {
        withConfiguration(f -> {
            var original = root(); transaction(f, s -> f.store.stageSchedule(s, null, original));
            try (var client = MongoClients.create(MONGO.getReplicaSetUrl()); var session = client.startSession()) {
                var other = new MongoAlertNotificationStore(client.getDatabase(f.database.getName()), SCOPE, AlertChannelPolicy.Deployment.ON_PREM, () -> {}, f.clock);
                var found = session.withTransaction(() -> other.requiredSchedule(session, original.key(), 1));
                assertThat(found.value()).isEqualTo(original);
            }
            assertThat(SystemCollections.ALERT_DELIVERY_INTENTS.on(f.database).countDocuments()).isZero();
        });
    }

    @Test void planningAndIntentShareOneTransactionAndAMissingIntentCannotAdvanceTheClock() {
        withConfiguration(f -> {
            var original = root(); transaction(f, s -> f.store.stageSchedule(s, null, original));
            var before = transaction(f, s -> f.store.requiredSchedule(s, original.key(), 1));
            var intent = business(); var delivery = ((AlertIntent.Business) intent.frozen().identity()).key(); var plan = planned(original, delivery);
            f.clock.at(AT.plusSeconds(10));
            assertUnreadable(() -> transaction(f, s -> f.store.stageSchedule(s, before, plan)));
            assertThatThrownBy(() -> transaction(f, s -> {
                f.store.stageIntent(s, null, intent); f.store.stageSchedule(s, before, plan);
                ConfigurationVersion.advanceAlert(s, SystemCollections.CONFIGURATION_VERSION.on(f.database)); throw new AbortProbe();
            })).isInstanceOf(AbortProbe.class);
            assertThat(SystemCollections.ALERT_DELIVERY_INTENTS.on(f.database).countDocuments()).isZero();
            assertThat(MongoAlertScheduleStoreIT.<MongoAlertScheduleCodec.Stored>transaction(f, s -> f.store.requiredSchedule(s, original.key(), 1)).value()).isEqualTo(original);
            transaction(f, s -> { f.store.stageIntent(s, null, intent); return f.store.stageSchedule(s, before, plan); });
            assertThat(MongoAlertScheduleStoreIT.<MongoAlertScheduleCodec.Stored>transaction(f, s -> f.store.requiredSchedule(s, original.key(), 1)).value().clock().round()).isOne();
        });
    }

    @Test void settledIntentEvidenceMustBeHandedOffBeforeItsScheduleReferenceCanDisappear() {
        withConfiguration(f -> {
            var original = root(); var intent = business(); var key = ((AlertIntent.Business) intent.frozen().identity()).key();
            f.clock.at(AT.plusSeconds(10));
            transaction(f, s -> { f.store.stageIntent(s, null, intent); return f.store.stageSchedule(s, null, planned(original, key)); });
            var before = transaction(f, s -> f.store.requiredSchedule(s, original.key(), 1));
            var without = new AlertSchedule.Record(before.value().key(), before.value().route(), before.value().retention(), AT, before.value().owner(),
                    GROUPING, before.value().clock(), 0, List.of(), null, null, List.of(), List.of(), null, false, before.value().members());
            assertUnreadable(() -> transaction(f, s -> f.store.stageSchedule(s, before, without)));
            var storedIntent = transaction(f, s -> f.store.requiredIntent(s, intent.frozen().identity(), 1));
            var stopped = stopped(intent, AT.plusSeconds(11)); f.clock.at(AT.plusSeconds(11));
            transaction(f, s -> f.store.stageIntent(s, storedIntent, stopped));
            var member = intent.frozen().members().getFirst();
            var summary = new AlertSchedule.Previous(member.evaluation().key(), member.episode(), DeliveryEvidence.Outcome.NOT_ATTEMPTED, false);
            var handed = new AlertSchedule.Record(without.key(), without.route(), without.retention(), AT, without.owner(), GROUPING, without.clock(),
                    0, List.of(), null, null, List.of(summary), List.of(), null, false, without.members());
            transaction(f, s -> f.store.stageSchedule(s, before, handed));
            assertThat(MongoAlertScheduleStoreIT.<MongoAlertScheduleCodec.Stored>transaction(f, s -> f.store.requiredSchedule(s, original.key(), 1)).value().summaries())
                    .containsExactly(summary);
        });
    }

    @Test void aStoppedOriginalBatchAndItsUniqueSurvivorSuccessorMustBePresentTogether() {
        withConfiguration(f -> {
            var intent = groupedIntent(); var original = groupBatch(intent); f.clock.at(AT.plusSeconds(10));
            transaction(f, s -> { f.store.stageIntent(s, null, intent); return f.store.stageSchedule(s, null, original); });
            var previous = transaction(f, s -> f.store.requiredSchedule(s, original.key(), 1));
            var survivors = List.of(intent.frozen().members().getLast()); var oldKey = ((AlertIntent.Business) intent.frozen().identity()).key();
            var n = oldKey.notification();
            var nextKey = new DeliveryKey(new DeliveryKey.NotificationRound(n.scope(), n.subjectId(), "2", n.routeId(), n.routeIncarnation(), n.kind(), 2),
                    oldKey.channelType(), oldKey.receiverIncarnation(), oldKey.targetId());
            var disposition = new AlertSchedule.Disposition(oldKey, 1, FrozenGroupPolicy.Action.STOP_PENDING, survivors, nextKey, null, DeliveryEvidence.Outcome.NOT_ATTEMPTED);
            var next = new AlertSchedule.Record(original.key(), original.route(), original.retention(), AT, original.owner(), GROUPING, original.clock(), 1,
                    List.of(), original.batch(), disposition, List.of(), List.of(oldKey, nextKey), null, false, survivors);
            assertUnreadable(() -> transaction(f, s -> f.store.stageSchedule(s, previous, next)));
            f.clock.at(AT.plusSeconds(11)); var stored = transaction(f, s -> f.store.requiredIntent(s, intent.frozen().identity(), 1));
            var fz = intent.frozen();
            var successorFrozen = new AlertIntent.Frozen(new AlertIntent.Business(nextKey), fz.channel(), fz.route(), fz.target(), fz.content(), survivors,
                    fz.retry(), fz.retention(), AT.plusSeconds(11), fz.deadline());
            var successor = initial(successorFrozen, PAYLOAD, null);
            transaction(f, s -> {
                f.store.stageIntent(s, stored, stopped(intent, AT.plusSeconds(11))); f.store.stageIntent(s, null, successor);
                return f.store.stageSchedule(s, previous, next);
            });
            assertThat(MongoAlertScheduleStoreIT.<MongoAlertNotificationStore.Stage>transaction(f, s -> f.store.stageSchedule(s, previous, next)))
                    .isEqualTo(MongoAlertNotificationStore.Stage.UNCHANGED);
            assertThat(MongoAlertScheduleStoreIT.<MongoAlertScheduleCodec.Stored>transaction(f, s -> f.store.requiredSchedule(s, original.key(), 1)).value().disposition().successor())
                    .isEqualTo(nextKey);
        });
    }

    @Test void corruptOrMissingRequiredProgressCannotBeRecreatedAsANewClock() {
        withConfiguration(f -> {
            var original = root(); transaction(f, s -> f.store.stageSchedule(s, null, original));
            var rows = SystemCollections.ALERT_NOTIFICATION_SCHEDULES.on(f.database);
            rows.updateOne(new Document(), new Document("$set", new Document("recordRevision", 1)));
            assertUnreadable(() -> transaction(f, s -> f.store.stageSchedule(s, null, original)));
            rows.deleteMany(new Document()); assertUnreadable(() -> transaction(f, s -> f.store.requiredSchedule(s, original.key(), 1)));
            assertThat(rows.countDocuments()).isZero();
        });
    }

    @Test void aDispositionCannotLaunderTheOriginalUnknownEvidenceIntoNotAttempted() {
        withConfiguration(f -> {
            var seed = seedTerminalGroup(f, DeliveryEvidence.Outcome.UNKNOWN, 2);
            var original = seed.value(); var survivors = List.of(original.batch().members().getLast());
            var wrong = new AlertSchedule.Disposition(original.batch().delivery(), 1, FrozenGroupPolicy.Action.STOP_PENDING,
                    survivors, null, null, DeliveryEvidence.Outcome.NOT_ATTEMPTED);
            assertThatThrownBy(() -> transaction(f, s -> f.store.stageSchedule(s, seed, withDisposition(original, survivors, wrong, original.protectedIntents()))))
                    .isInstanceOf(TapstateException.class);
            var correct = new AlertSchedule.Disposition(original.batch().delivery(), 1, FrozenGroupPolicy.Action.STOP_PENDING,
                    survivors, null, null, DeliveryEvidence.Outcome.UNKNOWN);
            assertThat(MongoAlertScheduleStoreIT.<MongoAlertNotificationStore.Stage>transaction(f, s ->
                    f.store.stageSchedule(s, seed, withDisposition(original, survivors, correct, original.protectedIntents()))))
                    .isEqualTo(MongoAlertNotificationStore.Stage.STAGED);
        });
    }

    @Test void everyEligibleOriginalSurvivorMustBePreservedByTheDisposition() {
        withConfiguration(f -> {
            var seed = seedTerminalGroup(f, DeliveryEvidence.Outcome.NOT_ATTEMPTED, 3); var original = seed.value();
            var current = original.members().subList(0, 2);
            var incomplete = new AlertSchedule.Disposition(original.batch().delivery(), 1, FrozenGroupPolicy.Action.STOP_PENDING,
                    List.of(current.getFirst()), null, null, DeliveryEvidence.Outcome.NOT_ATTEMPTED);
            assertThatThrownBy(() -> transaction(f, s -> f.store.stageSchedule(s, seed, withDisposition(original, current, incomplete, original.protectedIntents()))))
                    .isInstanceOf(TapstateException.class);
            var complete = new AlertSchedule.Disposition(original.batch().delivery(), 1, FrozenGroupPolicy.Action.STOP_PENDING,
                    current, null, null, DeliveryEvidence.Outcome.NOT_ATTEMPTED);
            assertThat(MongoAlertScheduleStoreIT.<MongoAlertNotificationStore.Stage>transaction(f, s ->
                    f.store.stageSchedule(s, seed, withDisposition(original, current, complete, original.protectedIntents()))))
                    .isEqualTo(MongoAlertNotificationStore.Stage.STAGED);
        });
    }

    @Test void theUniqueSuccessorCannotChooseADifferentFrozenRetentionPolicy() {
        withConfiguration(f -> {
            var seed = seedTerminalGroup(f, DeliveryEvidence.Outcome.NOT_ATTEMPTED, 2); var original = seed.value();
            var oldKey = original.batch().delivery(); var oldIntent = transaction(f, s -> f.store.requiredIntent(s, new AlertIntent.Business(oldKey), 1)).value();
            var survivors = List.of(original.members().getLast()); var n = oldKey.notification();
            var nextKey = new DeliveryKey(new DeliveryKey.NotificationRound(n.scope(), n.subjectId(), "2", n.routeId(), n.routeIncarnation(), n.kind(), 2),
                    oldKey.channelType(), oldKey.receiverIncarnation(), oldKey.targetId());
            var values = new java.util.EnumMap<AlertRetentionPolicy.Setting, Duration>(AlertRetentionPolicy.Setting.class);
            values.putAll(oldIntent.frozen().retention().values()); values.put(AlertRetentionPolicy.Setting.FAILED_RESULT, Duration.ofDays(30));
            var altered = new AlertIntent.Policy(AlertChannelPolicy.Deployment.ON_PREM, values); var old = oldIntent.frozen();
            var frozen = new AlertIntent.Frozen(new AlertIntent.Business(nextKey), old.channel(), old.route(), old.target(), old.content(), survivors,
                    old.retry(), altered, AT.plusSeconds(11), old.deadline());
            transaction(f, s -> f.store.stageIntent(s, null, initial(frozen, PAYLOAD, null)));
            var disposition = new AlertSchedule.Disposition(oldKey, 1, FrozenGroupPolicy.Action.STOP_PENDING, survivors, nextKey, null, DeliveryEvidence.Outcome.NOT_ATTEMPTED);
            assertThatThrownBy(() -> transaction(f, s -> f.store.stageSchedule(s, seed, withDisposition(original, survivors, disposition, List.of(oldKey, nextKey)))))
                    .isInstanceOf(TapstateException.class);
        });
    }

    private static AlertSchedule.Record withDisposition(AlertSchedule.Record before, List<AlertIntent.Member> members,
            AlertSchedule.Disposition disposition, List<DeliveryKey> refs) {
        return new AlertSchedule.Record(before.key(), before.route(), before.retention(), before.createdAt(), before.owner(), before.grouping(),
                before.clock(), before.batchSequence(), List.of(), before.batch(), disposition, before.summaries(), refs, null, false, members);
    }
    private static MongoAlertScheduleCodec.Stored seedTerminalGroup(Fixture f, DeliveryEvidence.Outcome outcome, int size) {
        var base = groupedIntent(); var frozen = base.frozen(); var members = new java.util.ArrayList<>(frozen.members());
        if (size == 3) {
            var source = members.getFirst(); var rule = source.evaluation();
            var alarm = new AlarmKey(SCOPE, new AlarmKey.Resource("pipeline", "orders-c", "pipeline-c"), rule.key().typeId(), rule.key().qualifier());
            members.add(new AlertIntent.Member(new AlertRule(alarm, 1, 1, rule.producerIncarnation(), rule.triggerFor(), rule.resolveFor(), rule.maxAge(), rule.maxGap()),
                    1, source.ruleIdentity(), 0));
            frozen = new AlertIntent.Frozen(frozen.identity(), frozen.channel(), frozen.route(), frozen.target(), frozen.content(), members,
                    frozen.retry(), frozen.retention(), AT, frozen.deadline()); base = initial(frozen, PAYLOAD, null);
        }
        AlertIntent.Record terminal;
        if (outcome == DeliveryEvidence.Outcome.NOT_ATTEMPTED) { terminal = stopped(base, AT.plusSeconds(1)); }
        else {
            var permitted = pending(base, "owner-a", "attempt-a"); var first = permitted.state().attempts().getFirst().value();
            var ended = first.settleDeadline(AT.plusSeconds(60));
            var retained = base.state().retained().value(frozen).stop(ended.endedAt(), first.permit().deadline());
            terminal = new AlertIntent.Record(frozen, new AlertIntent.State(permitted.state().owner(), 0, true, AlertIntent.StopReason.CANCELLED, null,
                    1, List.of(new AlertIntent.Attempt(ended.permit(), ended.outcome(), ended.endedAt(), null, null, null)), outcome,
                    AlertIntent.Retained.capture(retained), PAYLOAD, null, null, null, null, ended.endedAt(), ended.endedAt()));
        }
        // Restore a valid closed original record; the regression concerns mutation against authoritative existing evidence.
        SystemCollections.ALERT_DELIVERY_INTENTS.on(f.database).insertOne(MongoAlertDeliveryCodec.write(terminal, 1));
        var schedule = groupBatch(terminal); f.clock.at(AT.plusSeconds(60));
        transaction(f, s -> f.store.stageSchedule(s, null, schedule));
        return transaction(f, s -> f.store.requiredSchedule(s, schedule.key(), 1));
    }
    private static AlertIntent.Record stopped(AlertIntent.Record before, Instant at) {
        var s = before.state(); var retained = s.retained().value(before.frozen()).stop(at, before.frozen().createdAt());
        return new AlertIntent.Record(before.frozen(), new AlertIntent.State(s.owner(), s.manualCycles(), true, AlertIntent.StopReason.CANCELLED,
                null, 0, List.of(), s.summary(), AlertIntent.Retained.capture(retained), s.payload(), s.candidate(), null, null, null,
                at, null, s.cycleStarts(), s.acceptedReceipt()));
    }
    private record Fixture(MongoClient client, MongoDatabase database, MongoAlertNotificationStore store, MutableClock clock) { }
    private static void withConfiguration(Consumer<Fixture> action) {
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = client.getDatabase("alert_schedule_" + java.util.UUID.randomUUID().toString().replace("-", ""));
            var settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(database.getName()), null, Duration.ofSeconds(5));
            try (var ignored = MongoConfigurationBootstrap.prepare(settings, SCOPE.clusterId(), "installation-a", "setup-a", 1)) {
                var clock = new MutableClock(); action.accept(new Fixture(client, database,
                        new MongoAlertNotificationStore(database, SCOPE, AlertChannelPolicy.Deployment.ON_PREM, () -> {}, clock), clock));
            } finally { database.drop(); }
        }
    }
    private static <T> T transaction(Fixture f, Function<ClientSession, T> work) { return new MongoReadBudget(Duration.ofSeconds(30)).transaction(f.client, work); }
    private static void assertUnreadable(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
    private static final class MutableClock extends Clock {
        private Instant now = AT; void at(Instant value) { now = value; }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }
    private static final class AbortProbe extends RuntimeException { }
}
