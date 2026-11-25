package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.alerting.*;
import io.tapstate.core.alerting.AlertConfiguration.*;
import io.tapstate.core.alerting.AlertReceiver.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
import static io.tapstate.adapters.mongostore.AlertReceiverFixtures.*;
import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class MongoAlertReceiverStoreIT {
    @Container static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @Test void legacyUnpreparedAlertResourcesCannotBeImplicitlyCreatedThroughComposition() {
        withConfiguration(f -> {
            for (SystemCollections collection : SystemCollections.values()) {
                if (collection.resourceId().startsWith("alert_")) { collection.on(f.database).drop(); }
            }
            SystemCollections.SCHEMA_MIGRATIONS.on(f.database).updateOne(new Document("_id", "config"),
                    new Document("$unset", new Document("alerting", "")));
            SystemCollections.CONFIGURATION_VERSION.on(f.database).updateOne(new Document("_id", ConfigurationVersion.ID),
                    new Document("$unset", new Document(ConfigurationVersion.ALERT_FENCE, "")));
            try (var reopened = MongoConfigurationDomain.openExisting(f.settings); var input = material(PASSWORD)) {
                assertThat(reopened.alertSchemaVersion()).isZero();
                assertThatThrownBy(() -> {
                    var store = reopened.alertDefinitions(SCOPE, BuiltinAlarmTypes.catalog());
                    var prepared = store.prepareReceiver(email(), input);
                    reopened.alertTransaction(new MongoReadBudget(Duration.ofSeconds(30)), s -> store.stageReceiver(s, null, prepared));
                }).isInstanceOf(TapstateException.class);
            }
            assertThat(f.database.listCollectionNames().into(new java.util.ArrayList<>())).doesNotContain("alert_receivers");
        });
    }

    @Test void disabledReceiverCanBeRetiredAfterReopeningWithoutReenteringCredentials() {
        withConfiguration(f -> {
            var receiver = email(); save(f, null, receiver, PASSWORD);
            var captured = transaction(f, s -> f.store.captureReceiver(s, receiver.header().identity()));
            var disabled = receiver.edit(receiver.name(), false, receiver.transport(), receiver.credentials(), AT.plusSeconds(1), "actor-a");
            var prepared = f.store.retainReceiverMaterial(disabled, captured);
            transaction(f, s -> f.store.stageReceiver(s, receiver, prepared));
            try (var reopened = MongoConfigurationDomain.openExisting(f.settings)) {
                var store = reopened.alertDefinitions(SCOPE, BuiltinAlarmTypes.catalog());
                var originalMaterial = reopened.alertTransaction(new MongoReadBudget(Duration.ofSeconds(30)),
                        s -> store.captureReceiverForManagement(s, receiver.header().identity()));
                assertThat(reopened.<Boolean>alertTransaction(new MongoReadBudget(Duration.ofSeconds(30)),
                        s -> store.fenceReceiverUse(s, originalMaterial))).isFalse();
                assertThatThrownBy(() -> store.resolveReceiver(originalMaterial)).isInstanceOf(IllegalArgumentException.class);
                var retired = disabled.retire(AT.plusSeconds(2), "actor-a");
                var retained = store.retainReceiverMaterial(retired, originalMaterial);
                assertThat(reopened.<MongoAlertConfigurationStore.Stage>alertTransaction(new MongoReadBudget(Duration.ofSeconds(30)),
                        s -> store.stageReceiver(s, disabled, retained))).isEqualTo(MongoAlertConfigurationStore.Stage.STAGED);
            }
            assertThat(raw(f).get("privateSlot")).isEqualTo(captured.privateSlot());
        });
    }

    @Test void originalReceiverAndPairedKeySurviveReopeningWithoutExposingMaterial() {
        withConfiguration(f -> {
            var receiver = email();
            try (var input = material(PASSWORD)) {
                var prepared = f.store.prepareReceiver(receiver, input);
                assertThat(MongoAlertReceiverStoreIT.<MongoAlertConfigurationStore.Stage>transaction(f, s -> f.store.stageReceiver(s, null, prepared)))
                        .isEqualTo(MongoAlertConfigurationStore.Stage.STAGED);
                assertThat(raw(f).toJson()).doesNotContain(PASSWORD);
                assertThat(transaction(f, s -> f.store.load(s, Kind.RECEIVER, "receiver-a").orElseThrow()).toString()).doesNotContain(PASSWORD, "ciphertext");
            }
            try (var reopened = MongoConfigurationDomain.openExisting(f.settings)) {
                var store = reopened.alertDefinitions(SCOPE, BuiltinAlarmTypes.catalog());
                var captured = reopened.alertTransaction(new MongoReadBudget(Duration.ofSeconds(30)), s -> store.captureReceiver(s, receiver.header().identity()));
                try (var resolved = store.resolveReceiver(captured)) {
                    assertThat(resolved.authentication()).isEqualTo(PASSWORD.getBytes(StandardCharsets.UTF_8));
                }
                assertThat(reopened.<Boolean>alertTransaction(new MongoReadBudget(Duration.ofSeconds(30)), s -> store.fenceReceiverUse(s, captured))).isTrue();
            }
        });
    }

    @Test void equivalentRotationKeepsTheReferenceButAnOldResolvedCaptureCannotObtainPermission() {
        withConfiguration(f -> {
            var receiver = email(); save(f, null, receiver, PASSWORD);
            var old = transaction(f, s -> f.store.captureReceiver(s, receiver.header().identity()));
            var rotated = receiver.edit(receiver.name(), true, receiver.transport(), receiver.credentials(), AT.plusSeconds(1), "actor-b");
            try (var input = material("rotated-test-password")) {
                var prepared = f.store.prepareReceiver(rotated, input);
                transaction(f, s -> f.store.stageReceiver(s, receiver, prepared));
                assertThat(MongoAlertReceiverStoreIT.<MongoAlertConfigurationStore.Stage>transaction(f, s -> f.store.stageReceiver(s, receiver, prepared)))
                        .isEqualTo(MongoAlertConfigurationStore.Stage.UNCHANGED);
                var anotherNonce = f.store.prepareReceiver(rotated, input);
                assertThat(MongoAlertReceiverStoreIT.<MongoAlertConfigurationStore.Stage>transaction(f, s -> f.store.stageReceiver(s, receiver, anotherNonce)))
                        .isEqualTo(MongoAlertConfigurationStore.Stage.CONFLICT);
            }
            assertThat(rotated.credentials()).isEqualTo(receiver.credentials());
            assertThat(rotated.bindingRevision()).isOne();
            assertThat(MongoAlertReceiverStoreIT.<Boolean>transaction(f, s -> f.store.fenceReceiverUse(s, old))).isFalse();
            var current = transaction(f, s -> f.store.captureReceiver(s, receiver.header().identity()));
            try (var material = f.store.resolveReceiver(current)) {
                assertThat(material.authentication()).isEqualTo("rotated-test-password".getBytes(StandardCharsets.UTF_8));
            }
            assertThat(MongoAlertReceiverStoreIT.<Boolean>transaction(f, s -> f.store.fenceReceiverUse(s, current))).isTrue();
        });
    }

    @Test void publicMetadataEditRetainsTheExactMaterialAndTargetChangeRequiresExplicitPrivateInput() {
        withConfiguration(f -> {
            var receiver = email(); save(f, null, receiver, PASSWORD);
            var original = transaction(f, s -> f.store.captureReceiver(s, receiver.header().identity()));
            var renamed = receiver.edit("Renamed", true, receiver.transport(), receiver.credentials(), AT.plusSeconds(1), "actor-a");
            var retained = f.store.retainReceiverMaterial(renamed, original);
            transaction(f, s -> f.store.stageReceiver(s, receiver, retained));
            assertThat(MongoAlertReceiverStoreIT.<Boolean>transaction(f, s -> f.store.fenceReceiverUse(s, original))).isTrue();
            var old = (Email) receiver.transport();
            var target = new Email("another.example.invalid", old.port(), old.encryption(), old.authentication(), old.username(), old.from(), old.recipients(), old.trust());
            var changed = renamed.edit("New target", true, target, renamed.credentials(), AT.plusSeconds(2), "actor-a");
            assertThatThrownBy(() -> f.store.retainReceiverMaterial(changed, original)).isInstanceOf(TapstateException.class);
            save(f, renamed, changed, PASSWORD);
            assertThat(MongoAlertReceiverStoreIT.<Boolean>transaction(f, s -> f.store.fenceReceiverUse(s, original))).isFalse();
        });
    }

    @Test void disabledRetiredAndRecreatedReceiversNeverReuseTheOldUseCapture() {
        withConfiguration(f -> {
            var original = email(); save(f, null, original, PASSWORD);
            var captured = transaction(f, s -> f.store.captureReceiver(s, original.header().identity()));
            var disabled = original.edit(original.name(), false, original.transport(), original.credentials(), AT.plusSeconds(1), "actor-a");
            transaction(f, s -> f.store.stageReceiver(s, original, f.store.retainReceiverMaterial(disabled, captured)));
            assertThat(MongoAlertReceiverStoreIT.<Boolean>transaction(f, s -> f.store.fenceReceiverUse(s, captured))).isFalse();
            var retired = disabled.retire(AT.plusSeconds(2), "actor-a");
            var retained = f.store.retainReceiverMaterial(retired, captured);
            transaction(f, s -> f.store.stageReceiver(s, disabled, retained));
            Header header = Header.create(Identity.create(Kind.RECEIVER, SCOPE, "receiver-a"), original.header().deployment(), AT, "actor-a");
            var rebuilt = new AlertReceiver(header, original.name(), true, 1, original.transport(),
                    CredentialReference.create(header.identity(), original.credentials().authenticationIdentity()));
            try (var input = material(PASSWORD)) {
                var prepared = f.store.prepareReceiver(rebuilt, input);
                assertThat(MongoAlertReceiverStoreIT.<MongoAlertConfigurationStore.Stage>transaction(f, s -> f.store.stageReceiver(s, null, prepared)))
                        .isEqualTo(MongoAlertConfigurationStore.Stage.CONFLICT);
                // Only the isolated fixture simulates removal of an unreferenced retired record.
                SystemCollections.ALERT_RECEIVERS.on(f.database).deleteOne(new Document("_id", MongoAlertConfigurationCodec.key(SCOPE, "receiver-a")));
                transaction(f, s -> f.store.stageReceiver(s, null, prepared));
            }
            assertThat(MongoAlertReceiverStoreIT.<Boolean>transaction(f, s -> f.store.fenceReceiverUse(s, captured))).isFalse();
        });
    }

    @Test void actualKeyLossIsNotHiddenByTheCachedKeyAndDoesNotPartiallyInsertOrAdvanceTheReceiver() {
        withConfiguration(f -> {
            var receiver = email();
            try (var input = material(PASSWORD)) {
                var prepared = f.store.prepareReceiver(receiver, input);
                Document key = SystemCollections.CLUSTER_KEYS.on(f.database).find().first();
                SystemCollections.CLUSTER_KEYS.on(f.database).deleteMany(new Document());
                assertThatThrownBy(() -> transaction(f, s -> f.store.stageReceiver(s, null, prepared))).isInstanceOf(TapstateException.class);
                assertThat(SystemCollections.ALERT_RECEIVERS.on(f.database).countDocuments()).isZero();
                assertThat(SystemCollections.CLUSTER_KEYS.on(f.database).countDocuments()).isZero();
                SystemCollections.CLUSTER_KEYS.on(f.database).insertOne(key);
                transaction(f, s -> f.store.stageReceiver(s, null, prepared));
                var captured = transaction(f, s -> f.store.captureReceiver(s, receiver.header().identity()));
                long before = raw(f).getLong("writeFence");
                SystemCollections.CLUSTER_KEYS.on(f.database).deleteMany(new Document());
                assertThatThrownBy(() -> transaction(f, s -> f.store.fenceReceiverUse(s, captured))).isInstanceOf(TapstateException.class);
                assertThat(raw(f).getLong("writeFence")).isEqualTo(before);
            }
        });
    }

    @Test void abortRollsBackKeyReceiverAndCompanionConfigWritesTogether() {
        withConfiguration(f -> {
            Document originalKey = SystemCollections.CLUSTER_KEYS.on(f.database).find().first();
            try (var input = material(PASSWORD)) {
                var prepared = f.store.prepareReceiver(email(), input);
                assertThatThrownBy(() -> transaction(f, s -> {
                    f.store.stageReceiver(s, null, prepared);
                    f.store.stage(s, null, silence());
                    throw new InterruptedCommit();
                })).isInstanceOf(InterruptedCommit.class);
                assertThat(SystemCollections.ALERT_RECEIVERS.on(f.database).countDocuments()).isZero();
                assertThat(SystemCollections.ALERT_SILENCES.on(f.database).countDocuments()).isZero();
                assertThat(SystemCollections.CLUSTER_KEYS.on(f.database).find().first()).isEqualTo(originalKey);
            }
        });
    }

    @Test void aMatchedButUnchangedKeyRowCannotAuthorizeReceiverUse() {
        withConfiguration(f -> {
            var receiver = email(); save(f, null, receiver, PASSWORD);
            var captured = transaction(f, s -> f.store.captureReceiver(s, receiver.header().identity()));
            SystemCollections.CLUSTER_KEYS.on(f.database).updateOne(new Document(),
                    new Document("$set", new Document("sourceWriteFence", Double.POSITIVE_INFINITY)));
            long before = raw(f).getLong("writeFence");
            assertThatThrownBy(() -> transaction(f, s -> f.store.fenceReceiverUse(s, captured))).isInstanceOf(TapstateException.class);
            assertThat(raw(f).getLong("writeFence")).isEqualTo(before);
        });
    }

    @Test void oldSnapshotCannotGainPermissionAfterConcurrentDisableCommits() {
        withConfiguration(f -> {
            var receiver = email(); save(f, null, receiver, PASSWORD);
            var captured = transaction(f, s -> f.store.captureReceiver(s, receiver.header().identity()));
            var disabled = receiver.edit(receiver.name(), false, receiver.transport(), receiver.credentials(), AT.plusSeconds(1), "actor-a");
            var prepared = f.store.retainReceiverMaterial(disabled, captured);
            var snapshot = new CountDownLatch(1); var committed = new CountDownLatch(1);
            try (var executor = Executors.newSingleThreadExecutor()) {
                var first = new AtomicBoolean(true);
                var future = executor.submit(() -> transaction(f, s -> {
                    if (first.compareAndSet(true, false)) {
                        assertThat(f.store.load(s, Kind.RECEIVER, "receiver-a").orElseThrow().definition()).isEqualTo(receiver);
                        snapshot.countDown(); await(committed);
                    }
                    if (!f.store.fenceReceiverUse(s, captured)) { return false; }
                    f.store.stage(s, null, silence()); return true;
                }));
                await(snapshot); transaction(f, s -> f.store.stageReceiver(s, receiver, prepared)); committed.countDown();
                assertThat(future.get(15, TimeUnit.SECONDS)).isFalse();
                assertThat(SystemCollections.ALERT_SILENCES.on(f.database).countDocuments()).isZero();
            } catch (Exception failure) { throw new AssertionError(failure); }
            finally { committed.countDown(); }
        });
    }

    @Test void malformedPrivateSlotAndReceiverSizeAreNeverSilentlyRepaired() {
        withConfiguration(f -> {
            save(f, null, email(), PASSWORD);
            SystemCollections.ALERT_RECEIVERS.on(f.database).updateOne(new Document(), new Document("$set", new Document("privateSlot.credentialId", "mismatched")));
            assertUnreadable(() -> transaction(f, s -> f.store.load(s, Kind.RECEIVER, "receiver-a")));
            assertThat(raw(f).get("privateSlot", Document.class).getString("credentialId")).isEqualTo("mismatched");
            SystemCollections.ALERT_RECEIVERS.on(f.database).updateOne(new Document(), new Document("$set", new Document("body.name", "x".repeat(65536))));
            assertUnreadable(() -> transaction(f, s -> f.store.load(s, Kind.RECEIVER, "receiver-a")));
        });
    }

    private record Fixture(MongoConfigurationDomain domain, MongoConnectionSettings settings, MongoDatabase database, MongoAlertConfigurationStore store) { }
    private static <T> T transaction(Fixture f, Function<com.mongodb.client.ClientSession, T> action) {
        return f.domain.alertTransaction(new MongoReadBudget(Duration.ofSeconds(30)), action);
    }
    private static void save(Fixture f, AlertReceiver before, AlertReceiver candidate, String password) {
        try (var input = material(password)) { var prepared = f.store.prepareReceiver(candidate, input); transaction(f, s -> f.store.stageReceiver(s, before, prepared)); }
    }
    private static Document raw(Fixture f) { return SystemCollections.ALERT_RECEIVERS.on(f.database).find().first(); }
    private static void withConfiguration(Consumer<Fixture> action) {
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var db = client.getDatabase("alert_receiver_" + UUID.randomUUID().toString().replace("-", ""));
            var settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(db.getName()), null, Duration.ofSeconds(5));
            try (var domain = MongoConfigurationBootstrap.prepare(settings, SCOPE.clusterId(), "installation-a", "setup-a", 1)) {
                action.accept(new Fixture(domain, settings, db, domain.alertDefinitions(SCOPE, BuiltinAlarmTypes.catalog())));
            } finally { db.drop(); }
        }
    }
    private static void await(CountDownLatch latch) {
        try { if (!latch.await(10, TimeUnit.SECONDS)) { throw new AssertionError("transaction barrier timed out"); } }
        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
    }
    private static void assertUnreadable(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
    private static final class InterruptedCommit extends RuntimeException { }
}
