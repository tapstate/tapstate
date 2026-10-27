package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.alerting.*;
import io.tapstate.core.alerting.AlertIntent.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static io.tapstate.adapters.mongostore.AlertDeliveryFixtures.*;
import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class MongoAlertDeliveryStoreIT {
    @Container static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    @Test void immutableIntentAndOriginalEmbeddedPermitReopenWithoutANewIdentityOrAttempt() {
        withConfiguration(f -> {
            var original = business(); transaction(f, s -> f.store.stageIntent(s, null, original));
            var before = transaction(f, s -> f.store.requiredIntent(s, original.frozen().identity(), 1));
            var permitted = pending(original, "owner-a", "attempt-a");
            transaction(f, s -> f.store.stageIntent(s, before, permitted));
            assertThat(MongoAlertDeliveryStoreIT.<MongoAlertNotificationStore.Stage>transaction(f, s -> f.store.stageIntent(s, before, permitted)))
                    .isEqualTo(MongoAlertNotificationStore.Stage.UNCHANGED);
            try (var client = MongoClients.create(MONGO.getReplicaSetUrl()); var session = client.startSession()) {
                var store = new MongoAlertNotificationStore(client.getDatabase(f.database.getName()), SCOPE, AlertChannelPolicy.Deployment.ON_PREM, () -> {}, f.clock);
                var reopened = session.withTransaction(() -> store.requiredIntent(session, original.frozen().identity(), 1));
                assertThat(reopened.value()).isEqualTo(permitted);
                assertThat(reopened.recordRevision()).isEqualTo(2);
            }
            assertThat(SystemCollections.ALERT_DELIVERY_INTENTS.on(f.database).countDocuments()).isOne();
        });
    }

    @Test void concurrentOwnersCannotBothAppendAnOriginalFirstPermit() {
        withConfiguration(f -> {
            var original = business(); transaction(f, s -> f.store.stageIntent(s, null, original));
            CountDownLatch read = new CountDownLatch(2);
            try (var executor = Executors.newFixedThreadPool(2)) {
                var futures = List.of("owner-a", "owner-b").stream().map(owner -> {
                    AtomicBoolean first = new AtomicBoolean(true);
                    return executor.submit(() -> transaction(f, s -> {
                        var before = f.store.requiredIntent(s, original.frozen().identity(), 1);
                        if (first.compareAndSet(true, false)) { read.countDown(); await(read); }
                        if (before.value().state().owner().generation() != 0) { return MongoAlertNotificationStore.Stage.CONFLICT; }
                        return f.store.stageIntent(s, before, pending(original, owner, "attempt-" + owner));
                    }));
                }).toList();
                assertThat(List.of(futures.get(0).get(15, TimeUnit.SECONDS), futures.get(1).get(15, TimeUnit.SECONDS)))
                        .containsExactlyInAnyOrder(MongoAlertNotificationStore.Stage.STAGED, MongoAlertNotificationStore.Stage.CONFLICT);
                var stored = transaction(f, s -> f.store.requiredIntent(s, original.frozen().identity(), 1));
                assertThat(stored.value().state().attemptsConsumed()).isOne();
                assertThat(stored.value().state().attempts()).hasSize(1);
            } catch (Exception failure) { throw new AssertionError(failure); }
        });
    }

    @Test void staleOwnerCannotRenewOrGrantButCurrentOwnerMayMergeAnOriginalLateReceipt() {
        withConfiguration(f -> {
            var original = business(); transaction(f, s -> f.store.stageIntent(s, null, original));
            var first = transaction(f, s -> f.store.requiredIntent(s, original.frozen().identity(), 1));
            var request = pending(original, "owner-a", "attempt-a"); transaction(f, s -> f.store.stageIntent(s, first, request));
            var before = transaction(f, s -> f.store.requiredIntent(s, original.frozen().identity(), 1));
            f.clock.at(AT.plusSeconds(130));
            var a = request.state();
            var renewed = new AlertIntent.Record(request.frozen(), new State(new AlertOwnership(1, "owner-a", AT.plusSeconds(240)),
                    a.manualCycles(), false, null, null, 1, a.attempts(), a.summary(), a.retained(), a.payload(), null, null, null, null));
            assertThatThrownBy(() -> transaction(f, s -> f.store.stageIntent(s, before, renewed)))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(AlertError.STALE_ACTION));
            var taken = new AlertIntent.Record(request.frozen(), new State(new AlertOwnership(2, "owner-b", AT.plusSeconds(240)),
                    a.manualCycles(), false, null, null, 1, a.attempts(), a.summary(), a.retained(), a.payload(), null, null, null, null));
            transaction(f, s -> f.store.stageIntent(s, before, taken));
            var current = transaction(f, s -> f.store.requiredIntent(s, original.frozen().identity(), 1));
            var late = accepted(taken, f.clock.instant());
            assertThat(MongoAlertDeliveryStoreIT.<MongoAlertNotificationStore.Stage>transaction(f, s -> f.store.stageIntent(s, before, accepted(request, f.clock.instant()))))
                    .isEqualTo(MongoAlertNotificationStore.Stage.CONFLICT);
            transaction(f, s -> f.store.stageIntent(s, current, late));
            var result = transaction(f, s -> f.store.requiredIntent(s, original.frozen().identity(), 1));
            assertThat(result.value().state().owner().generation()).isEqualTo(2);
            assertThat(result.value().state().summary()).isEqualTo(DeliveryEvidence.Outcome.ACCEPTED);
            assertThat(result.value().state().attempts().getFirst().permit()).isEqualTo(request.state().attempts().getFirst().permit());
        });
    }

    @Test void aFrozenSnapshotOrConsumedBudgetCannotBeReplacedAndRetentionCannotChangeWithoutItsFacts() {
        withConfiguration(f -> {
            var original = business(); transaction(f, s -> f.store.stageIntent(s, null, original));
            var before = transaction(f, s -> f.store.requiredIntent(s, original.frozen().identity(), 1));
            var s = original.state();
            var shorter = new AlertIntent.Record(original.frozen(), new State(s.owner(), 0, false, null, AT, 0, List.of(), s.summary(),
                    new Retained(AT.plusSeconds(3600), AT.plusSeconds(86400), null, null), PAYLOAD, null, null, null, null));
            assertThatThrownBy(() -> transaction(f, session -> f.store.stageIntent(session, before, shorter))).isInstanceOf(IllegalArgumentException.class);
            var request = pending(original, "owner-a", "attempt-a"); transaction(f, session -> f.store.stageIntent(session, before, request));
            var used = transaction(f, session -> f.store.requiredIntent(session, original.frozen().identity(), 1));
            assertThatThrownBy(() -> transaction(f, session -> f.store.stageIntent(session, used, original))).isInstanceOf(IllegalArgumentException.class);
            assertThat(MongoAlertDeliveryStoreIT.<MongoAlertDeliveryCodec.Stored>transaction(f, session -> f.store.requiredIntent(session, original.frozen().identity(), 1)).value())
                    .isEqualTo(request);
        });
    }

    @Test void requiredMissingOrCorruptIntentCannotBeRecreatedAsANewEmptyLedger() {
        withConfiguration(f -> {
            var original = business(); transaction(f, s -> f.store.stageIntent(s, null, original));
            var rows = SystemCollections.ALERT_DELIVERY_INTENTS.on(f.database);
            rows.updateOne(new Document(), new Document("$set", new Document("recordRevision", 1)));
            assertUnreadable(() -> transaction(f, s -> f.store.intent(s, original.frozen().identity())));
            assertUnreadable(() -> transaction(f, s -> f.store.stageIntent(s, null, original)));
            rows.deleteMany(new Document());
            assertUnreadable(() -> transaction(f, s -> f.store.requiredIntent(s, original.frozen().identity(), 1)));
            assertThat(rows.countDocuments()).isZero();
        });
    }

    @Test void anEarlyRetryCannotDiscardTheOriginallyCommittedNextAttemptTime() {
        withConfiguration(f -> {
            var original = business(); transaction(f, s -> f.store.stageIntent(s, null, original));
            var initial = transaction(f, s -> f.store.requiredIntent(s, original.frozen().identity(), 1));
            var pending = pending(original, "owner-a", "attempt-a"); transaction(f, s -> f.store.stageIntent(s, initial, pending));
            var active = transaction(f, s -> f.store.requiredIntent(s, original.frozen().identity(), 1));
            var first = pending.state().attempts().getFirst().value();
            var rejected = first.receipt(first.permit(), DeliveryEvidence.Outcome.NOT_ACCEPTED, AT.plusSeconds(1)).attempt();
            var ended = new Attempt(rejected.permit(), rejected.outcome(), rejected.endedAt(), null,
                    new Protocol(Phase.FINAL_RESPONSE, Action.RETRY, 450, "smtp", null), null);
            var state = pending.state();
            var scheduled = new AlertIntent.Record(original.frozen(), new State(state.owner(), 0, false, null, AT.plusSeconds(1801),
                    1, List.of(ended), DeliveryEvidence.Outcome.NOT_ACCEPTED, state.retained(), PAYLOAD, null, null, null, null));
            f.clock.at(AT.plusSeconds(1)); transaction(f, s -> f.store.stageIntent(s, active, scheduled));
            var before = transaction(f, s -> f.store.requiredIntent(s, original.frozen().identity(), 1));
            var second = DeliveryAttempt.grant(SCOPE, original.frozen().identity().deliveryId(), "attempt-b", 2, 1, "owner-a",
                    original.frozen().retry(), AT, original.frozen().deadline(), AT.plusSeconds(2)).orElseThrow();
            var early = new AlertIntent.Record(original.frozen(), new State(state.owner(), 0, false, null, null, 2,
                    List.of(ended, Attempt.pending(second)), DeliveryEvidence.Outcome.UNKNOWN, state.retained(), PAYLOAD, null, null, null, null));
            f.clock.at(AT.plusSeconds(2));
            assertThatThrownBy(() -> transaction(f, s -> f.store.stageIntent(s, before, early)))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(AlertError.STALE_ACTION));
            var eligible = DeliveryAttempt.grant(SCOPE, original.frozen().identity().deliveryId(), "attempt-b", 2, 2, "owner-b",
                    original.frozen().retry(), AT, original.frozen().deadline(), AT.plusSeconds(1801)).orElseThrow();
            var allowed = new AlertIntent.Record(original.frozen(), new State(new AlertOwnership(2, "owner-b", AT.plusSeconds(1921)),
                    0, false, null, null, 2, List.of(ended, Attempt.pending(eligible)), DeliveryEvidence.Outcome.UNKNOWN,
                    state.retained(), PAYLOAD, null, null, null, null));
            f.clock.at(AT.plusSeconds(1801));
            assertThat(MongoAlertDeliveryStoreIT.<MongoAlertNotificationStore.Stage>transaction(f, s -> f.store.stageIntent(s, before, allowed)))
                    .isEqualTo(MongoAlertNotificationStore.Stage.STAGED);
        });
    }

    @Test void testIntentQuotaAndActualConfigurationContentionRollBackAndCommitTogether() {
        withConfiguration(f -> {
            var original = test(); var command = ((AlertIntent.Test) original.frozen().identity()).command();
            Function<ClientSession, Void> stage = s -> {
                f.store.stageQuota(s, null, AlertTestQuota.Bucket.empty(SCOPE, AT).accepted(command.key().actorPrincipal()));
                f.store.stageIntent(s, null, original);
                ConfigurationVersion.advanceAlert(s, SystemCollections.CONFIGURATION_VERSION.on(f.database)); return null;
            };
            assertThatThrownBy(() -> transaction(f, s -> { stage.apply(s); throw new AbortProbe(); })).isInstanceOf(AbortProbe.class);
            assertThat(SystemCollections.ALERT_TEST_QUOTAS.on(f.database).countDocuments()).isZero();
            assertThat(SystemCollections.ALERT_DELIVERY_INTENTS.on(f.database).countDocuments()).isZero();
            f.client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                    .append("data", new Document("failCommands", List.of("commitTransaction"))
                            .append("writeConcernError", new Document("code", 64).append("errmsg", "test commit reply lost"))
                            .append("errorLabels", List.of("UnknownTransactionCommitResult"))));
            try { transaction(f, stage); }
            finally { f.client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
            f.clock.at(AT.plus(Duration.ofDays(1)));
            assertThat(MongoAlertDeliveryStoreIT.<MongoAlertDeliveryCodec.Stored>transaction(f, s -> f.store.testIntent(s, command.key()).orElseThrow()).value()).isEqualTo(original);
            assertThat(MongoAlertDeliveryStoreIT.<MongoAlertNotificationCodec.Quota>transaction(f, s -> f.store.quota(s, AlertTestQuota.minute(AT)).orElseThrow()).value().scopeAccepted()).isOne();
            assertThat(SystemCollections.ALERT_INSTANCES.on(f.database).countDocuments()).isZero();
        });
    }

    @Test void explicitManualCycleAfterPermanentRejectionRetainsItsOwnStartAndReplaysOnlyOnce() {
        withConfiguration(f -> {
            var original = business(); transaction(f, s -> f.store.stageIntent(s, null, original));
            var initial = transaction(f, s -> f.store.requiredIntent(s, original.frozen().identity(), 1));
            var pending = pending(original, "owner-a", "attempt-a"); transaction(f, s -> f.store.stageIntent(s, initial, pending));
            var active = transaction(f, s -> f.store.requiredIntent(s, original.frozen().identity(), 1));
            var first = pending.state().attempts().getFirst().value();
            var rejected = first.receipt(first.permit(), DeliveryEvidence.Outcome.NOT_ACCEPTED, AT.plusSeconds(1)).attempt();
            var ended = new Attempt(rejected.permit(), rejected.outcome(), rejected.endedAt(), null,
                    new Protocol(Phase.AUTHENTICATION, Action.STOP, 535, "smtp", null), null);
            var old = pending.state();
            var closed = new AlertIntent.Record(original.frozen(), new State(old.owner(), 0, false, null, null, 1, List.of(ended),
                    DeliveryEvidence.Outcome.NOT_ACCEPTED, old.retained(), PAYLOAD, null, null, null, null));
            f.clock.at(AT.plusSeconds(1)); transaction(f, s -> f.store.stageIntent(s, active, closed));
            var before = transaction(f, s -> f.store.requiredIntent(s, original.frozen().identity(), 1));
            var opened = new AlertIntent.Record(original.frozen(), new State(old.owner(), 1, false, null, AT.plusSeconds(2), 1, List.of(ended),
                    DeliveryEvidence.Outcome.NOT_ACCEPTED, old.retained(), PAYLOAD, null, null, null, null, null, rejected.endedAt(), List.of(0, 1)));
            f.clock.at(AT.plusSeconds(2)); transaction(f, s -> f.store.stageIntent(s, before, opened));
            assertThat(MongoAlertDeliveryStoreIT.<MongoAlertNotificationStore.Stage>transaction(f, s -> f.store.stageIntent(s, before, opened)))
                    .isEqualTo(MongoAlertNotificationStore.Stage.UNCHANGED);
            var current = transaction(f, s -> f.store.requiredIntent(s, original.frozen().identity(), 1));
            var second = DeliveryAttempt.grant(SCOPE, original.frozen().identity().deliveryId(), "attempt-b", 2, 1, "owner-a",
                    original.frozen().retry(), AT, original.frozen().deadline(), AT.plusSeconds(2)).orElseThrow();
            var next = new AlertIntent.Record(original.frozen(), new State(old.owner(), 1, false, null, null, 2,
                    List.of(ended, Attempt.pending(second)), DeliveryEvidence.Outcome.UNKNOWN, old.retained(), PAYLOAD, null,
                    null, null, null, null, rejected.endedAt(), List.of(0, 1)));
            transaction(f, s -> f.store.stageIntent(s, current, next));
            var result = transaction(f, s -> f.store.requiredIntent(s, original.frozen().identity(), 1));
            assertThat(result.value().state().manualCycles()).isOne();
            assertThat(result.value().state().currentCycleConsumed()).isOne();
            assertThat(result.value().state().attemptsConsumed()).isEqualTo(2);
            assertThat(result.value().state().cycleStarts()).containsExactly(0, 1);
        });
    }

    @Test void clearingThenRepopulatingEligibilityCannotLaunderThePersistedRetryLowerBound() {
        withConfiguration(f -> {
            var before = settledForEligibility(f, false); f.clock.at(AT.plusSeconds(2));
            assertThatThrownBy(() -> transaction(f, session -> {
                var cleared = nextTime(before.value(), null); f.store.stageIntent(session, before, cleared);
                var empty = f.store.requiredIntent(session, before.value().frozen().identity(), 1);
                return f.store.stageIntent(session, empty, nextTime(cleared, AT.plusSeconds(2)));
            })).isInstanceOf(IllegalArgumentException.class);
            var stored = transaction(f, session -> f.store.requiredIntent(session, before.value().frozen().identity(), 1));
            assertThat(stored.value().state().nextAttemptAt()).isEqualTo(AT.plusSeconds(1801));
        });
    }

    @Test void aPermanentRejectionCannotReopenItsAutomaticCycleThroughAnOrdinaryReschedule() {
        withConfiguration(f -> {
            var before = settledForEligibility(f, true); f.clock.at(AT.plusSeconds(2));
            assertThatThrownBy(() -> transaction(f, session -> f.store.stageIntent(session, before, nextTime(before.value(), AT.plusSeconds(2)))))
                    .isInstanceOf(IllegalArgumentException.class);
            var s = before.value().state(); var original = s.attempts().getFirst();
            var changed = new Attempt(original.permit(), original.outcome(), original.endedAt(), null,
                    new Protocol(Phase.FINAL_RESPONSE, Action.RETRY, 450, "smtp", null), null);
            var recategorized = new AlertIntent.Record(before.value().frozen(), new State(s.owner(), 0, false, null, null, 1,
                    List.of(changed), s.summary(), s.retained(), PAYLOAD, null, null, null, null));
            assertThatThrownBy(() -> transaction(f, session -> f.store.stageIntent(session, before, recategorized))).isInstanceOf(IllegalArgumentException.class);
            var stored = transaction(f, session -> f.store.requiredIntent(session, before.value().frozen().identity(), 1));
            assertThat(stored.value().state().manualCycles()).isZero();
            assertThat(stored.value().state().nextAttemptAt()).isNull();
        });
    }

    @Test void anOverDeadlineResponseRestrictionCannotBeClearedToReopenAnOrdinaryRetry() {
        withConfiguration(f -> {
            var before = settledForEligibility(f, false, AT.plusSeconds(48 * 3600)); f.clock.at(AT.plusSeconds(2));
            var s = before.value().state(); var original = s.attempts().getFirst();
            var changed = new Attempt(original.permit(), original.outcome(), original.endedAt(), null,
                    new Protocol(Phase.FINAL_RESPONSE, Action.RETRY, 450, "smtp", null), null);
            var reopened = new AlertIntent.Record(before.value().frozen(), new State(s.owner(), 0, false, null, AT.plusSeconds(1801),
                    1, List.of(changed), s.summary(), s.retained(), PAYLOAD, null, null, null, null));
            assertThatThrownBy(() -> transaction(f, session -> f.store.stageIntent(session, before, reopened))).isInstanceOf(IllegalArgumentException.class);
        });
    }

    @Test void aNewManualCycleCannotIgnoreTheOriginalCrossCycleResponseRestriction() {
        withConfiguration(f -> {
            var before = settledForEligibility(f, false, AT.plusSeconds(48 * 3600)); f.clock.at(AT.plusSeconds(2));
            var s = before.value().state();
            var opened = new AlertIntent.Record(before.value().frozen(), new State(s.owner(), 1, false, null, AT.plusSeconds(2),
                    s.attemptsConsumed(), s.attempts(), s.summary(), s.retained(), PAYLOAD, null, null, null, null,
                    null, s.lastAttemptEnd(), List.of(0, 1)));
            assertThatThrownBy(() -> transaction(f, session -> f.store.stageIntent(session, before, opened))).isInstanceOf(IllegalArgumentException.class);
        });
    }

    private static AlertIntent.Record nextTime(AlertIntent.Record value, Instant next) {
        var s = value.state();
        return new AlertIntent.Record(value.frozen(), new State(s.owner(), s.manualCycles(), s.permitsStopped(), s.stopReason(), next,
                s.attemptsConsumed(), s.attempts(), s.summary(), s.retained(), s.payload(), s.candidate(), s.payloadPrunedAt(), s.candidatePrunedAt(),
                s.detailsPrunedAt(), s.permitsStoppedAt(), s.lastAttemptEnd(), s.cycleStarts(), s.acceptedReceipt()));
    }
    private static MongoAlertDeliveryCodec.Stored settledForEligibility(Fixture f, boolean permanent) {
        return settledForEligibility(f, permanent, null);
    }
    private static MongoAlertDeliveryCodec.Stored settledForEligibility(Fixture f, boolean permanent, Instant retryAfter) {
        var original = business(); transaction(f, session -> f.store.stageIntent(session, null, original));
        var initial = transaction(f, session -> f.store.requiredIntent(session, original.frozen().identity(), 1));
        var pending = pending(original, "owner-a", "attempt-a"); transaction(f, session -> f.store.stageIntent(session, initial, pending));
        var active = transaction(f, session -> f.store.requiredIntent(session, original.frozen().identity(), 1));
        var attempt = pending.state().attempts().getFirst().value();
        var result = attempt.receipt(attempt.permit(), DeliveryEvidence.Outcome.NOT_ACCEPTED, AT.plusSeconds(1)).attempt();
        var ended = new Attempt(result.permit(), result.outcome(), result.endedAt(), null,
                new Protocol(Phase.FINAL_RESPONSE, permanent ? Action.STOP : Action.RETRY, permanent ? 535 : 450, "smtp", retryAfter), null);
        var body = new AlertIntent.Record(original.frozen(), new State(pending.state().owner(), 0, false, null,
                permanent || retryAfter != null && !retryAfter.isBefore(original.frozen().deadline()) ? null : AT.plusSeconds(1801), 1, List.of(ended), DeliveryEvidence.Outcome.NOT_ACCEPTED,
                pending.state().retained(), PAYLOAD, null, null, null, null));
        f.clock.at(AT.plusSeconds(1)); transaction(f, session -> f.store.stageIntent(session, active, body));
        return transaction(f, session -> f.store.requiredIntent(session, original.frozen().identity(), 1));
    }

    private record Fixture(MongoClient client, MongoDatabase database, MongoAlertNotificationStore store, MutableClock clock) { }
    private static void withConfiguration(Consumer<Fixture> action) {
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = client.getDatabase("alert_delivery_" + java.util.UUID.randomUUID().toString().replace("-", ""));
            var settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(database.getName()), null, Duration.ofSeconds(5));
            try (var ignored = MongoConfigurationBootstrap.prepare(settings, SCOPE.clusterId(), "installation-a", "setup-a", 1)) {
                var clock = new MutableClock();
                action.accept(new Fixture(client, database, new MongoAlertNotificationStore(database, SCOPE, AlertChannelPolicy.Deployment.ON_PREM, () -> {}, clock), clock));
            } finally { database.drop(); }
        }
    }
    private static <T> T transaction(Fixture f, Function<ClientSession, T> action) { return new MongoReadBudget(Duration.ofSeconds(30)).transaction(f.client, action); }
    private static void await(CountDownLatch latch) {
        try { if (!latch.await(10, TimeUnit.SECONDS)) { throw new AssertionError("transaction barrier timed out"); } }
        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
    }
    private static void assertUnreadable(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
    private static final class MutableClock extends Clock {
        private volatile Instant now = AT;
        void at(Instant value) { now = value; }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }
    private static final class AbortProbe extends RuntimeException { }
}
