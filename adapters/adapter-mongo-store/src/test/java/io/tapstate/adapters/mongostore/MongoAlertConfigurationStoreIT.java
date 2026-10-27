package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.alerting.*;
import io.tapstate.core.alerting.AlertConfiguration.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
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
import static io.tapstate.adapters.mongostore.AlertConfigurationFixtures.*;
import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class MongoAlertConfigurationStoreIT {
    @Container static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final TransactionOptions OPTIONS = TransactionOptions.builder()
            .readConcern(ReadConcern.SNAPSHOT).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();

    @Test void threeDefinitionsSurviveReopeningWithoutCreatingAnAdditionalResource() {
        withConfiguration(f -> {
            for (Definition definition : List.of(rule(), route(), silence())) {
                assertThat(MongoAlertConfigurationStoreIT.<MongoAlertConfigurationStore.Stage>transaction(f, session -> f.store.stage(session, null, definition)))
                        .isEqualTo(MongoAlertConfigurationStore.Stage.STAGED);
            }
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl()); var session = other.startSession()) {
                var reopened = new MongoAlertConfigurationStore(other.getDatabase(f.database.getName()), SCOPE, BuiltinAlarmTypes.catalog(), () -> {});
                for (Definition definition : List.of(rule(), route(), silence())) {
                    var id = definition.header().identity();
                    assertThat(session.withTransaction(() -> reopened.load(session, id.kind(), id.id()).orElseThrow().definition(), OPTIONS))
                            .isEqualTo(definition);
                }
            }
            assertThat(SystemCollections.ALERT_RECEIVERS.on(f.database).countDocuments()).isZero();
            assertThat(SystemCollections.ALERT_INSTANCES.on(f.database).countDocuments()).isZero();
        });
    }

    @Test void abortRollsBackDefinitionAndProtectedRowWritesTogether() {
        withConfiguration(f -> {
            transaction(f, s -> f.store.stage(s, null, rule()));
            assertThatThrownBy(() -> transaction(f, s -> {
                assertThat(f.store.fence(s, rule())).isTrue();
                assertThat(f.store.stage(s, null, silence())).isEqualTo(MongoAlertConfigurationStore.Stage.STAGED);
                throw new InterruptedCommit();
            })).isInstanceOf(InterruptedCommit.class);
            assertThat(SystemCollections.ALERT_SILENCES.on(f.database).countDocuments()).isZero();
            assertThat(MongoAlertConfigurationStoreIT.<Long>transaction(f, s -> f.store.load(s, Kind.RULE, "rule-a").orElseThrow().writeFence())).isZero();
        });
    }

    @Test void committedRetryUsesOriginalIdentityAndDoesNotCreateAnotherRevision() {
        withConfiguration(f -> {
            Rule original = rule(); Rule edited = edit(original, "Edited");
            transaction(f, s -> f.store.stage(s, null, original));
            assertThat(MongoAlertConfigurationStoreIT.<MongoAlertConfigurationStore.Stage>transaction(f, s -> f.store.stage(s, null, original)))
                    .isEqualTo(MongoAlertConfigurationStore.Stage.UNCHANGED);
            transaction(f, s -> f.store.stage(s, original, edited));
            assertThat(MongoAlertConfigurationStoreIT.<MongoAlertConfigurationStore.Stage>transaction(f, s -> f.store.stage(s, original, edited)))
                    .isEqualTo(MongoAlertConfigurationStore.Stage.UNCHANGED);
            var stored = transaction(f, s -> f.store.load(s, Kind.RULE, "rule-a").orElseThrow());
            assertThat(stored.definition()).isEqualTo(edited);
            assertThat(stored.writeFence()).isEqualTo(1);
            assertThat(SystemCollections.ALERT_RULES.on(f.database).countDocuments()).isOne();
        });
    }

    @Test void acceptedUnexpandedDefaultsRemainUsableForFencingEditingAndRetirement() {
        withConfiguration(f -> {
            Rule expanded = rule();
            Rule original = new Rule(expanded.header(), expanded.name(), expanded.typeId(), expanded.typeVersion(),
                    expanded.resources(), java.util.Map.of(), null, true, null);
            transaction(f, s -> f.store.stage(s, null, original));
            Rule edited = edit(original, "Edited without explicit defaults");
            assertThat(MongoAlertConfigurationStoreIT.<Boolean>transaction(f, s -> f.store.fence(s, original))).isTrue();
            assertThat(MongoAlertConfigurationStoreIT.<MongoAlertConfigurationStore.Stage>transaction(f, s -> f.store.stage(s, original, edited)))
                    .isEqualTo(MongoAlertConfigurationStore.Stage.STAGED);
            assertThat(MongoAlertConfigurationStoreIT.<MongoAlertConfigurationStore.Stage>transaction(f, s -> f.store.stage(s, edited, retire(edited))))
                    .isEqualTo(MongoAlertConfigurationStore.Stage.STAGED);
        });
    }

    @Test void concurrentEditsCannotBothSucceedFromOneOriginalRevision() {
        withConfiguration(f -> {
            Rule original = rule(); transaction(f, s -> f.store.stage(s, null, original));
            CountDownLatch bothRead = new CountDownLatch(2);
            try (var executor = Executors.newFixedThreadPool(2)) {
                var futures = List.of("One", "Two").stream().map(name -> executor.submit(() -> {
                    AtomicBoolean first = new AtomicBoolean(true);
                    return transaction(f, s -> {
                        if (first.compareAndSet(true, false)) {
                            assertThat(f.store.load(s, Kind.RULE, "rule-a").orElseThrow().definition()).isEqualTo(original);
                            bothRead.countDown(); await(bothRead);
                        }
                        return f.store.stage(s, original, edit(original, name));
                    });
                })).toList();
                var outcomes = new ArrayList<MongoAlertConfigurationStore.Stage>();
                for (var future : futures) { outcomes.add(future.get(15, TimeUnit.SECONDS)); }
                assertThat(outcomes).containsExactlyInAnyOrder(MongoAlertConfigurationStore.Stage.STAGED, MongoAlertConfigurationStore.Stage.CONFLICT);
                assertThat(MongoAlertConfigurationStoreIT.<Long>transaction(f, s -> f.store.load(s, Kind.RULE, "rule-a")
                        .orElseThrow().definition().header().revision())).isEqualTo(2);
            } catch (Exception failure) { throw new AssertionError(failure); }
        });
    }

    @Test void protectedRowFenceForcesAnOldSnapshotToRetryAndRejectAfterRetirement() {
        withConfiguration(f -> {
            Rule original = rule(); transaction(f, s -> f.store.stage(s, null, original));
            CountDownLatch snapshot = new CountDownLatch(1); CountDownLatch retired = new CountDownLatch(1);
            try (var executor = Executors.newSingleThreadExecutor()) {
                AtomicBoolean first = new AtomicBoolean(true);
                var future = executor.submit(() -> transaction(f, s -> {
                    if (first.compareAndSet(true, false)) {
                        assertThat(f.store.load(s, Kind.RULE, "rule-a").orElseThrow().definition()).isEqualTo(original);
                        snapshot.countDown(); await(retired);
                    }
                    if (!f.store.fence(s, original)) { return false; }
                    f.store.stage(s, null, silence()); return true;
                }));
                await(snapshot);
                transaction(f, s -> f.store.stage(s, original, retire(original)));
                retired.countDown();
                assertThat(future.get(15, TimeUnit.SECONDS)).isFalse();
                assertThat(SystemCollections.ALERT_SILENCES.on(f.database).countDocuments()).isZero();
                assertThat(MongoAlertConfigurationStoreIT.<Definition>transaction(f, s -> f.store.load(s, Kind.RULE, "rule-a")
                        .orElseThrow().definition())).isEqualTo(retire(original));
            } catch (Exception failure) { throw new AssertionError(failure); }
            finally { retired.countDown(); }
        });
    }

    @Test void retainedRetirementBlocksReplacementAndOldIdentityCannotFenceARecreatedRow() {
        withConfiguration(f -> {
            Rule original = rule(); transaction(f, s -> f.store.stage(s, null, original));
            transaction(f, s -> f.store.stage(s, original, retire(original)));
            Header recreatedHeader = Header.create(Identity.create(Kind.RULE, SCOPE, "rule-a"), original.header().deployment(), AT, "actor-a");
            Rule recreated = new Rule(recreatedHeader, original.name(), original.typeId(), original.typeVersion(), original.resources(),
                    original.parameters(), original.severityOverride(), true, null);
            assertThat(MongoAlertConfigurationStoreIT.<MongoAlertConfigurationStore.Stage>transaction(f, s -> f.store.stage(s, null, recreated)))
                    .isEqualTo(MongoAlertConfigurationStore.Stage.CONFLICT);
            // The fixture removes an unreferenced retired row; production cleanup has separate guards.
            SystemCollections.ALERT_RULES.on(f.database).deleteOne(new Document("_id", MongoAlertConfigurationCodec.key(SCOPE, "rule-a")));
            transaction(f, s -> f.store.stage(s, null, recreated));
            assertThat(MongoAlertConfigurationStoreIT.<Boolean>transaction(f, s -> f.store.fence(s, original))).isFalse();
            assertThat(MongoAlertConfigurationStoreIT.<Boolean>transaction(f, s -> f.store.fence(s, recreated))).isTrue();
        });
    }

    @Test void malformedOriginalAndWrongScopeAreRefusedWithoutRepairOrPartialWrites() {
        withConfiguration(f -> {
            transaction(f, s -> f.store.stage(s, null, rule()));
            SystemCollections.ALERT_RULES.on(f.database).updateOne(new Document(), new Document("$set", new Document("recordRevision", 1)));
            assertUnreadable(() -> transaction(f, s -> f.store.load(s, Kind.RULE, "rule-a")));
            assertThat(SystemCollections.ALERT_RULES.on(f.database).find().first().get("recordRevision")).isInstanceOf(Integer.class);
            Identity id = Identity.create(Kind.RULE, new AlarmKey.Scope("foreign", SCOPE.clusterId()), "rule-b");
            Header header = Header.create(id, AlertChannelPolicy.Deployment.ON_PREM, AT, "actor-a");
            Rule foreign = new Rule(header, "Foreign", rule().typeId(), 1, resources(), rule().parameters(), null, true, null);
            assertUnreadable(() -> transaction(f, s -> f.store.stage(s, null, foreign)));
            try (var session = f.client.startSession()) {
                assertThatThrownBy(() -> f.store.load(session, Kind.RULE, "rule-a")).isInstanceOf(IllegalStateException.class);
            }
            assertThat(SystemCollections.ALERT_RULES.on(f.database).countDocuments()).isOne();
        });
    }

    private record Fixture(MongoClient client, MongoDatabase database, MongoAlertConfigurationStore store) { }
    private static <T> T transaction(Fixture f, Function<ClientSession, T> action) {
        try (var session = f.client.startSession()) { return StoreIo.call(() -> session.withTransaction(() -> action.apply(session), OPTIONS)); }
    }
    private static void withConfiguration(Consumer<Fixture> action) {
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = client.getDatabase("alert_configuration_" + UUID.randomUUID().toString().replace("-", ""));
            var settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(database.getName()), null, Duration.ofSeconds(5));
            try (var domain = MongoConfigurationBootstrap.prepare(settings, SCOPE.clusterId(), "installation-a", "setup-a", 1)) {
                assertThat(domain.alertSchemaVersion()).isEqualTo(1);
                action.accept(new Fixture(client, database, new MongoAlertConfigurationStore(database, SCOPE, BuiltinAlarmTypes.catalog(), () -> {})));
            } finally { database.drop(); }
        }
    }
    private static void await(CountDownLatch latch) {
        try { if (!latch.await(10, TimeUnit.SECONDS)) { throw new AssertionError("transaction barrier timed out"); } }
        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
    }
    private static void assertUnreadable(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
    private static final class InterruptedCommit extends RuntimeException { }
}
