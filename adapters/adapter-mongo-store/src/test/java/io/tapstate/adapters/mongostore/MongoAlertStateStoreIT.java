package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.core.alerting.AlarmSeverity;
import io.tapstate.core.alerting.AlertLifecycle;
import io.tapstate.core.alerting.AlertRule;
import io.tapstate.core.alerting.AlertSignal;
import io.tapstate.core.alerting.AlertSnapshot;
import io.tapstate.core.alerting.AlertUpdate;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
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
import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class MongoAlertStateStoreIT {
    @Container static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final Instant T = Instant.parse("2026-10-27T00:00:00.123456789Z");
    private static final AlarmKey.Scope SCOPE = new AlarmKey.Scope("org-a", "cluster-a");
    private static final AlarmKey KEY = new AlarmKey(SCOPE,
            new AlarmKey.Resource("pipeline", "orders", "resource-1"), "pipeline.failed", "rule-a");
    private static final AlertRule RULE = new AlertRule(KEY, 1, 1, "producer-1", Duration.ZERO,
            Duration.ZERO, Duration.ofSeconds(15), Duration.ofSeconds(10));
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();
    private static final TransactionOptions PREPARATION = TransactionOptions.builder()
            .readConcern(ReadConcern.LOCAL).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();

    @Test void stateEpisodeAndTransitionsSurviveASeparateClientWithoutOpeningCache() {
        withConfiguration(fixture -> {
            AlertSnapshot initial = AlertSnapshot.initial(RULE);
            AlertUpdate fired = signal(initial, 1, T, AlertSignal.Condition.BAD, AlarmSeverity.MAJOR);
            assertThat(MongoAlertStateStoreIT.<MongoAlertStateStore.Stage>transaction(fixture, session -> fixture.store().stage(session, 0, initial, fired)))
                    .isEqualTo(MongoAlertStateStore.Stage.STAGED);
            assertThat(count(fixture, SystemCollections.ALERT_INSTANCES)).isOne();
            assertThat(count(fixture, SystemCollections.ALERT_EPISODES)).isOne();
            assertThat(count(fixture, SystemCollections.ALERT_TRANSITIONS)).isEqualTo(fired.transitions().size());
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl()); var session = other.startSession()) {
                var reopened = new MongoAlertStateStore(other.getDatabase(fixture.database().getName()), SCOPE, () -> {});
                assertThat(session.withTransaction(() -> reopened.load(session, KEY).orElseThrow(), TRANSACTION))
                        .isEqualTo(new MongoAlertStateCodec.State(1, fired.snapshot()));
            }
            var actual = fixture.database().listCollectionNames().into(new ArrayList<>());
            for (SystemCollections row : SystemCollections.values()) {
                if (row.resourceId().startsWith("alert_")) { assertThat(actual).contains(row.collectionName()); }
            }
            assertThat(actual).doesNotContain("cache_identity", "srs_events", "workload_claims", "alert_read_receipts");
        });
    }

    @Test void abortAfterAllStagingRollsBackEveryStateResource() {
        withConfiguration(fixture -> {
            AlertSnapshot initial = AlertSnapshot.initial(RULE);
            AlertUpdate fired = signal(initial, 1, T, AlertSignal.Condition.BAD, AlarmSeverity.MAJOR);
            assertThatThrownBy(() -> transaction(fixture, session -> {
                assertThat(fixture.store().stage(session, 0, initial, fired)).isEqualTo(MongoAlertStateStore.Stage.STAGED);
                throw new InterruptedCommit();
            })).isInstanceOf(InterruptedCommit.class);
            for (SystemCollections row : MongoAlertStateStore.COLLECTIONS) { assertThat(count(fixture, row)).isZero(); }
        });
    }

    @Test void twoConcurrentWritersCannotStageDifferentSuccessorsFromTheSameHead() {
        withConfiguration(fixture -> {
            AlertSnapshot initial = AlertSnapshot.initial(RULE);
            AlertUpdate fired = signal(initial, 1, T, AlertSignal.Condition.BAD, AlarmSeverity.MAJOR);
            transaction(fixture, session -> fixture.store().stage(session, 0, initial, fired));
            var candidates = List.of(signal(fired.snapshot(), 2, T.plusSeconds(1), AlertSignal.Condition.GOOD, AlarmSeverity.MAJOR),
                    signal(fired.snapshot(), 2, T.plusSeconds(1), AlertSignal.Condition.BAD, AlarmSeverity.CRITICAL));
            var start = new CountDownLatch(1);
            var bothReadOriginal = new CountDownLatch(2);
            try (var executor = Executors.newFixedThreadPool(2)) {
                var futures = candidates.stream().map(candidate -> executor.submit(() -> {
                    if (!start.await(10, TimeUnit.SECONDS)) { throw new AssertionError("test start was not released"); }
                    var firstAttempt = new AtomicBoolean(true);
                    return transaction(fixture, session -> {
                        if (firstAttempt.compareAndSet(true, false)) {
                            assertThat(fixture.store().load(session, KEY).orElseThrow().recordRevision()).isEqualTo(1);
                            bothReadOriginal.countDown();
                            try {
                                if (!bothReadOriginal.await(10, TimeUnit.SECONDS)) {
                                    throw new AssertionError("both original transaction snapshots were not established");
                                }
                            } catch (InterruptedException interrupted) {
                                Thread.currentThread().interrupt(); throw new AssertionError(interrupted);
                            }
                        }
                        return fixture.store().stage(session, 1, fired.snapshot(), candidate);
                    });
                })).toList();
                start.countDown();
                var outcomes = new ArrayList<MongoAlertStateStore.Stage>();
                for (var future : futures) { outcomes.add(future.get(15, TimeUnit.SECONDS)); }
                assertThat(outcomes).containsExactlyInAnyOrder(MongoAlertStateStore.Stage.STAGED, MongoAlertStateStore.Stage.CONFLICT);
                var actual = transaction(fixture, session -> fixture.store().load(session, KEY).orElseThrow());
                assertThat(actual.recordRevision()).isEqualTo(2);
                assertThat(candidates).extracting(AlertUpdate::snapshot).contains(actual.snapshot());
                assertThat(count(fixture, SystemCollections.ALERT_INSTANCES)).isOne();
                assertThat(count(fixture, SystemCollections.ALERT_EPISODES)).isOne();
            } catch (Exception failure) { throw new AssertionError(failure); }
        });
    }

    @Test void replacementGenerationAndCapturedBeforeStateFenceOldCallbacksEvenWithANewRevision() {
        withConfiguration(fixture -> {
            AlertSnapshot initial = AlertSnapshot.initial(RULE);
            AlertUpdate fired = signal(initial, 1, T, AlertSignal.Condition.BAD, AlarmSeverity.MAJOR);
            transaction(fixture, session -> fixture.store().stage(session, 0, initial, fired));
            var replacement = new AlertRule(KEY, 2, 2, "producer-2", Duration.ZERO, Duration.ZERO,
                    Duration.ofSeconds(15), Duration.ofSeconds(10));
            AlertUpdate changed = AlertLifecycle.replace(fired.snapshot(), replacement, T.plusSeconds(1));
            transaction(fixture, session -> fixture.store().stage(session, 1, fired.snapshot(), changed));
            AlertUpdate late = signal(fired.snapshot(), 2, T.plusSeconds(2), AlertSignal.Condition.GOOD, AlarmSeverity.MAJOR);
            assertThat(MongoAlertStateStoreIT.<MongoAlertStateStore.Stage>transaction(fixture, session -> fixture.store().stage(session, 1, fired.snapshot(), late)))
                    .isEqualTo(MongoAlertStateStore.Stage.CONFLICT);
            assertThat(MongoAlertStateStoreIT.<MongoAlertStateStore.Stage>transaction(fixture, session -> fixture.store().stage(session, 2, fired.snapshot(), late)))
                    .isEqualTo(MongoAlertStateStore.Stage.CONFLICT);
            assertThat(MongoAlertStateStoreIT.<AlertSnapshot>transaction(fixture, session -> fixture.store().load(session, KEY).orElseThrow().snapshot()))
                    .isEqualTo(changed.snapshot());
        });
    }

    @Test void missingEpisodeOrMissingHeadRefusesInsteadOfInventingReplacementHistory() {
        withConfiguration(fixture -> {
            AlertSnapshot initial = AlertSnapshot.initial(RULE);
            AlertUpdate fired = signal(initial, 1, T, AlertSignal.Condition.BAD, AlarmSeverity.MAJOR);
            transaction(fixture, session -> fixture.store().stage(session, 0, initial, fired));
            SystemCollections.ALERT_EPISODES.on(fixture.database()).deleteMany(new Document());
            assertUnreadable(() -> transaction(fixture, session -> fixture.store().load(session, KEY)));
            assertThat(count(fixture, SystemCollections.ALERT_EPISODES)).isZero();
            SystemCollections.ALERT_INSTANCES.on(fixture.database()).deleteMany(new Document());
            assertUnreadable(() -> transaction(fixture, session -> fixture.store().stage(session, 0, initial, fired)));
            assertThat(count(fixture, SystemCollections.ALERT_INSTANCES)).isZero();
            assertThat(count(fixture, SystemCollections.ALERT_TRANSITIONS)).isEqualTo(fired.transitions().size());
        });
    }

    @Test void stateFragmentsRequireAnActiveTransactionAndTheCompleteOriginalScope() {
        withConfiguration(fixture -> {
            try (var session = fixture.client().startSession()) {
                assertThatThrownBy(() -> fixture.store().load(session, KEY)).isInstanceOf(IllegalStateException.class);
            }
            var foreign = new AlarmKey(new AlarmKey.Scope("org-b", SCOPE.clusterId()), KEY.resource(), KEY.typeId(), KEY.qualifier());
            assertUnreadable(() -> transaction(fixture, session -> fixture.store().load(session, foreign)));
            for (SystemCollections row : MongoAlertStateStore.COLLECTIONS) { assertThat(count(fixture, row)).isZero(); }
        });
    }

    private record Fixture(MongoClient client, MongoDatabase database, MongoAlertStateStore store) { }
    private static <T> T transaction(Fixture fixture, Function<ClientSession, T> action) {
        try (var session = fixture.client().startSession()) {
            return StoreIo.call(() -> session.withTransaction(() -> action.apply(session), TRANSACTION));
        }
    }
    private static void withConfiguration(Consumer<Fixture> scenario) {
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = client.getDatabase("alert_state_" + UUID.randomUUID().toString().replace("-", ""));
            var settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(database.getName()), null, Duration.ofSeconds(5));
            try (var original = MongoConfigurationBootstrap.prepare(settings, SCOPE.clusterId(), "installation-a", "setup-a", 1)) {
                assertThat(original.clusterId()).isEqualTo(SCOPE.clusterId());
                // Fixture preparation exercises the real declared DDL in one CONFIG transaction.
                // Host startup, authorization and full state/schedule/intent acceptance remain separate tests.
                try (var session = client.startSession()) {
                    session.withTransaction(() -> {
                        MongoAlertConfigurationStore.prepare(session, database);
                        MongoAlertStateStore.prepare(session, database);
                        MongoAlertNotificationStore.prepare(session, database);
                        return null;
                    }, PREPARATION);
                }
                scenario.accept(new Fixture(client, database, new MongoAlertStateStore(database, SCOPE, () -> {})));
            } finally { database.drop(); }
        }
    }
    private static long count(Fixture fixture, SystemCollections collection) {
        return collection.on(fixture.database()).countDocuments();
    }
    private static AlertUpdate signal(AlertSnapshot old, long sequence, Instant at, AlertSignal.Condition condition, AlarmSeverity severity) {
        AlertRule rule = old.rule();
        return AlertLifecycle.evaluate(old, new AlertSignal(rule.key(), rule.revision(), rule.generation(),
                rule.producerIncarnation(), sequence, at, condition, severity), at);
    }
    private static void assertUnreadable(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
    private static final class InterruptedCommit extends RuntimeException { }
}
