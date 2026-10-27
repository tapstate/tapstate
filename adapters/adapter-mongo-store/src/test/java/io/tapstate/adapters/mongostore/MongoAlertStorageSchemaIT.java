package io.tapstate.adapters.mongostore;

import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import io.tapstate.core.alerting.AlarmKey;
import io.tapstate.core.alerting.AlertRule;
import io.tapstate.core.alerting.AlertSnapshot;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.ArrayList;
import java.util.UUID;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class MongoAlertStorageSchemaIT {
    @Container static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    @Test void firstInstallationPublishesTheWholeFeatureWithoutChangingTheGlobalConfigVersion() {
        withConfiguration(false, fixture -> {
            assertThat(fixture.domain().schemaVersion()).isEqualTo(1);
            assertThat(fixture.domain().alertSchemaVersion()).isEqualTo(MongoAlertStorageSchema.VERSION);
            assertThat(schema(fixture).getInteger("installedVersion")).isEqualTo(1);
            assertThat(schema(fixture).get(MongoAlertStorageSchema.FIELD, Document.class).getString("operationId"))
                    .isEqualTo("setup-a");
            assertAllTables(fixture);
        });
    }

    @Test void ordinaryLegacyOpenIsLoadOnlyAndExplicitPreparationPreservesOtherFeatureFactsAndKeys() {
        withConfiguration(true, fixture -> {
            var schemas = SystemCollections.SCHEMA_MIGRATIONS.on(fixture.database());
            schemas.updateOne(new Document("_id", "config"), new Document("$set",
                    new Document("anotherFeature", new Document("installedVersion", 7))));
            Document before = schema(fixture);
            String keyId = SystemCollections.CLUSTER_KEYS.on(fixture.database()).find().first().getString("keyId");
            assertThat(fixture.domain().alertSchemaVersion()).isZero();
            assertNoAlertTables(fixture);
            assertThat(fixture.domain().prepareAlertStorage("alert-upgrade-a")).isEqualTo(MongoAlertStorageSchema.VERSION);
            assertThat(schema(fixture).get("installedVersion")).isEqualTo(before.get("installedVersion"));
            assertThat(schema(fixture).get("anotherFeature")).isEqualTo(before.get("anotherFeature"));
            assertThat(SystemCollections.CLUSTER_KEYS.on(fixture.database()).find().first().getString("keyId")).isEqualTo(keyId);
            assertThat(fixture.domain().prepareAlertStorage("another-operation-does-not-replace-the-winner"))
                    .isEqualTo(MongoAlertStorageSchema.VERSION);
            assertThat(schema(fixture).get(MongoAlertStorageSchema.FIELD, Document.class).getString("operationId"))
                    .isEqualTo("alert-upgrade-a");
            try (var reopened = MongoConfigurationDomain.open(fixture.settings(), "cluster-a")) {
                assertThat(reopened.alertSchemaVersion()).isEqualTo(MongoAlertStorageSchema.VERSION);
            }
            assertAllTables(fixture);
        });
    }

    @Test void missingUsedCollectionIsRefusedByReopenAndByPreparationWithoutRecreatingIt() {
        withConfiguration(false, fixture -> {
            SystemCollections.ALERT_EPISODES.on(fixture.database()).drop();
            assertUnreadable(fixture.domain()::alertSchemaVersion);
            assertUnreadable(() -> fixture.domain().prepareAlertStorage("cannot-repair-loss"));
            try (var reopened = MongoConfigurationDomain.open(fixture.settings(), "cluster-a")) {
                assertUnreadable(reopened::alertSchemaVersion);
            }
            assertThat(names(fixture)).doesNotContain(SystemCollections.ALERT_EPISODES.collectionName());
        });
    }

    @Test void missingIndexAndUnapprovedExpiryRemainVisibleAndAreNeverRepairedOnLoad() {
        withConfiguration(false, fixture -> {
            var episodes = SystemCollections.ALERT_EPISODES.on(fixture.database());
            episodes.dropIndex("alert_cleanup");
            assertUnreadable(fixture.domain()::alertSchemaVersion);
            assertUnreadable(() -> fixture.domain().prepareAlertStorage("cannot-repair-index"));
            assertThat(episodes.listIndexes().into(new ArrayList<>())).extracting(row -> row.getString("name"))
                    .doesNotContain("alert_cleanup");
        });
        withConfiguration(false, fixture -> {
            var episodes = SystemCollections.ALERT_EPISODES.on(fixture.database());
            episodes.createIndex(new Document("createdAt", 1), new IndexOptions().name("unexpected_expiry")
                    .expireAfter(1L, TimeUnit.SECONDS));
            assertUnreadable(fixture.domain()::alertSchemaVersion);
            assertThat(episodes.listIndexes().into(new ArrayList<>())).extracting(row -> row.getString("name"))
                    .contains("unexpected_expiry");
        });
    }

    @Test void unmarkedPartialStructuresAreNotAdoptedAndDoNotResetTheGlobalSchema() {
        withConfiguration(true, fixture -> {
            try (var session = fixture.client().startSession()) {
                session.startTransaction();
                SystemCollections.ALERT_RULES.prepareCollectionOn(fixture.database(), session);
                session.commitTransaction();
            }
            assertUnreadable(fixture.domain()::alertSchemaVersion);
            assertUnreadable(() -> fixture.domain().prepareAlertStorage("cannot-adopt-partial"));
            assertThat(schema(fixture)).doesNotContainKey(MongoAlertStorageSchema.FIELD);
            assertThat(schema(fixture).getInteger("installedVersion")).isEqualTo(1);
            assertThat(names(fixture)).contains("alert_rules").doesNotContain("alert_instances");
        });
    }

    @Test void unknownFeatureVersionIsNeverReinterpretedOrOverwritten() {
        withConfiguration(false, fixture -> {
            SystemCollections.SCHEMA_MIGRATIONS.on(fixture.database()).updateOne(new Document("_id", "config"),
                    new Document("$set", new Document(MongoAlertStorageSchema.FIELD + ".installedVersion", 99)));
            assertUnreadable(fixture.domain()::alertSchemaVersion);
            assertUnreadable(() -> fixture.domain().prepareAlertStorage("cannot-downgrade"));
            assertThat(schema(fixture).get(MongoAlertStorageSchema.FIELD, Document.class).getInteger("installedVersion"))
                    .isEqualTo(99);
        });
    }

    @Test void unexpectedUniqueIndexMustBeRejectedBeforeItCanConflateIndependentAlarms() {
        withConfiguration(false, fixture -> {
            var heads = SystemCollections.ALERT_INSTANCES.on(fixture.database());
            heads.createIndex(new Document("_id.clusterId", 1), new IndexOptions().unique(true).name("unexpected_cluster_unique"));
            heads.insertOne(initialHead("orders-a"));
            assertThatThrownBy(() -> heads.insertOne(initialHead("orders-b")))
                    .isInstanceOfSatisfying(MongoWriteException.class,
                            failure -> assertThat(failure.getError().getCode()).isEqualTo(11000));
            assertUnreadable(fixture.domain()::alertSchemaVersion);
            assertUnreadable(() -> fixture.domain().prepareAlertStorage("cannot-adopt-extra-constraint"));
            assertThat(heads.listIndexes().into(new ArrayList<>())).extracting(row -> row.getString("name"))
                    .contains("unexpected_cluster_unique");
        });
    }

    @Test void keyRefreshSharesTheSchemaOperationDeadline() {
        for (boolean prepare : new boolean[] {false, true}) {
            withConfiguration(false, fixture -> {
                var admin = fixture.client().getDatabase("admin");
                admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                        .append("data", new Document("failCommands", java.util.List.of("find"))
                                .append("blockConnection", true).append("blockTimeMS", 8000)));
                long started = System.nanoTime();
                try {
                    assertThatThrownBy(() -> {
                        if (prepare) { fixture.domain().prepareAlertStorage("bounded-preparation"); }
                        else { fixture.domain().alertSchemaVersion(); }
                    }).isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(IoError.STORE_UNAVAILABLE));
                    assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(7));
                } finally { admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
            });
        }
    }

    @Test void failureDuringIndexCreationRollsBackTheMarkerAndEveryCreatedCollection() {
        withConfiguration(true, fixture -> {
            var admin = fixture.client().getDatabase("admin");
            admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                    .append("data", new Document("failCommands", java.util.List.of("createIndexes")).append("errorCode", 8)));
            try {
                assertThatThrownBy(() -> fixture.domain().prepareAlertStorage("failed-preparation"))
                        .isInstanceOfSatisfying(TapstateException.class,
                                failure -> assertThat(failure.code()).isEqualTo(IoError.STORE_UNAVAILABLE));
            } finally { admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
            assertThat(schema(fixture)).doesNotContainKey(MongoAlertStorageSchema.FIELD);
            assertNoAlertTables(fixture);
            assertThat(fixture.domain().alertSchemaVersion()).isZero();
            assertThat(fixture.domain().prepareAlertStorage("original-retry"))
                    .isEqualTo(MongoAlertStorageSchema.VERSION);
            assertAllTables(fixture);
        });
    }

    @Test void supportedVersionOneIsLoadOnlyUntilExplicitAuthorityPreparation() {
        withConfiguration(false, f -> {
            versionOne(f);
            Document before = schema(f);
            Document source = before.get(MongoSourceIdentitySchema.FIELD, Document.class);
            assertThat(f.domain().alertSchemaVersion()).isEqualTo(1);
            try (var reopened = MongoConfigurationDomain.open(f.settings(), "cluster-a")) {
                assertThat(reopened.alertSchemaVersion()).isEqualTo(1);
            }
            assertThat(schema(f)).isEqualTo(before);
            assertThat(versions(f).find().first()).doesNotContainKey(ConfigurationVersion.ALERT_FENCE);
            assertThat(f.domain().prepareAlertStorage("authority-upgrade-a")).isEqualTo(2);
            Document after = schema(f);
            assertThat(after.getInteger("installedVersion")).isEqualTo(1);
            assertThat(after.get(MongoSourceIdentitySchema.FIELD, Document.class)).isEqualTo(source);
            Document marker = after.get(MongoAlertStorageSchema.FIELD, Document.class);
            assertThat(marker.getString("operationId")).isEqualTo("setup-a");
            assertThat(marker.getString("authorityPreparationOperationId")).isEqualTo("authority-upgrade-a");
            assertThat(MongoAlertConfigurationCodec.size(marker)).isLessThanOrEqualTo(2048);
            assertThat(f.domain().prepareAlertStorage("different-retry-does-not-replace-operation")).isEqualTo(2);
            assertThat(schema(f)).isEqualTo(after);
            assertThat(versions(f).find().first().getLong(ConfigurationVersion.ALERT_FENCE)).isZero();
        });
    }

    @Test void preparedCounterLossOrCorruptionRefusesOrdinaryReopenAndPreparationWithoutReset() {
        for (Object bad : java.util.Arrays.asList(null, 0, -1L, Double.POSITIVE_INFINITY, Long.MAX_VALUE)) {
            withConfiguration(false, f -> {
                if (bad == null) { versions(f).updateOne(new Document(), new Document("$unset", new Document(ConfigurationVersion.ALERT_FENCE, ""))); }
                else { versions(f).updateOne(new Document(), new Document("$set", new Document(ConfigurationVersion.ALERT_FENCE, bad))); }
                Document before = versions(f).find().first();
                assertUnreadable(f.domain()::alertSchemaVersion);
                assertUnreadable(() -> f.domain().prepareAlertStorage("must-not-reset"));
                assertUnreadable(() -> { try (var ignored = MongoConfigurationDomain.open(f.settings(), "cluster-a")) { fail("corrupt capability opened"); } });
                assertThat(versions(f).find().first()).isEqualTo(before);
                assertThat(schema(f).get(MongoAlertStorageSchema.FIELD, Document.class).getString("operationId")).isEqualTo("setup-a");
            });
        }
    }

    @Test void partialVersionOneEnrollmentIsNotAdopted() {
        withConfiguration(false, f -> {
            versionOne(f);
            versions(f).updateOne(new Document(), new Document("$set", new Document(ConfigurationVersion.ALERT_FENCE, 7L)));
            Document before = schema(f);
            assertUnreadable(f.domain()::alertSchemaVersion);
            assertUnreadable(() -> f.domain().prepareAlertStorage("partial-not-approved"));
            assertThat(schema(f)).isEqualTo(before);
            assertThat(versions(f).find().first().getLong(ConfigurationVersion.ALERT_FENCE)).isEqualTo(7L);
        });
    }

    @Test void failedUpgradeRollsBackBothCounterAndMarkerAndKeepsOriginalSourcePreparation() {
        withConfiguration(false, f -> {
            versionOne(f); Document before = schema(f);
            failCommit(f, new Document("errorCode", 8));
            try { assertThatThrownBy(() -> f.domain().prepareAlertStorage("failed-authority-upgrade")).isInstanceOf(TapstateException.class); }
            finally { clearFailPoint(f); }
            assertThat(schema(f)).isEqualTo(before);
            assertThat(versions(f).find().first()).doesNotContainKey(ConfigurationVersion.ALERT_FENCE);
            assertThat(f.domain().prepareAlertStorage("failed-authority-upgrade")).isEqualTo(2);
            assertThat(schema(f).get(MongoSourceIdentitySchema.FIELD)).isEqualTo(before.get(MongoSourceIdentitySchema.FIELD));
        });
    }

    @Test void unknownUpgradeCommitAndRepeatedOpenKeepTheOriginalOperationAndCounter() {
        withConfiguration(false, f -> {
            versionOne(f);
            failCommit(f, new Document("writeConcernError", new Document("code", 64).append("errmsg", "test commit reply lost"))
                    .append("errorLabels", List.of("UnknownTransactionCommitResult")));
            try { assertThat(f.domain().prepareAlertStorage("original-authority-operation")).isEqualTo(2); }
            finally { clearFailPoint(f); }
            transaction(f, s -> ConfigurationVersion.advanceAlert(s, versions(f)));
            Document committed = schema(f);
            assertThat(f.domain().prepareAlertStorage("original-authority-operation")).isEqualTo(2);
            assertThat(schema(f)).isEqualTo(committed);
            assertThat(versions(f).find().first().getLong(ConfigurationVersion.ALERT_FENCE)).isEqualTo(1L);
        });
    }

    @Test void pureAlertTransactionsLeaveTheImportRevisionUnchangedAndRollBackWithTheirFacts() {
        withConfiguration(false, f -> {
            f.domain().tokens().save(new io.tapstate.spi.store.TokenRecord("actor-a", "admin", "secret-hash", false, java.time.Instant.EPOCH));
            long original = f.domain().configurationRevision();
            for (int i = 0; i < 100; i++) { transaction(f, s -> ConfigurationVersion.advanceAlert(s, versions(f))); }
            assertThat(f.domain().configurationRevision()).isEqualTo(original);
            assertThat(versions(f).find().first().getLong(ConfigurationVersion.ALERT_FENCE)).isEqualTo(100L);
            var store = new MongoAlertConfigurationStore(f.database(), AlertConfigurationFixtures.SCOPE,
                    io.tapstate.core.alerting.BuiltinAlarmTypes.catalog(), () -> {});
            assertThatThrownBy(() -> transaction(f, s -> {
                ConfigurationVersion.advanceAlert(s, versions(f));
                store.stage(s, null, AlertConfigurationFixtures.silence());
                throw new AbortProbe();
            })).isInstanceOf(AbortProbe.class);
            assertThat(SystemCollections.ALERT_SILENCES.on(f.database()).countDocuments()).isZero();
            assertThat(versions(f).find().first().getLong(ConfigurationVersion.ALERT_FENCE)).isEqualTo(100L);
            assertThat(MongoAlertConfigurationCodec.size(versions(f).find().first())).isLessThanOrEqualTo(256);
        });
    }

    @Test void tokenRevocationBeforePermissionForcesTheOldSnapshotToRetryAndReject() {
        withConfiguration(false, f -> {
            f.domain().tokens().save(new io.tapstate.spi.store.TokenRecord("actor-a", "admin", "secret-hash", false, java.time.Instant.EPOCH));
            CountDownLatch read = new CountDownLatch(1), revoked = new CountDownLatch(1);
            AtomicBoolean first = new AtomicBoolean(true);
            try (var executor = Executors.newSingleThreadExecutor()) {
                var permission = executor.submit(() -> transaction(f, s -> {
                    Document actor = SystemCollections.TOKENS.on(f.database()).find(s, new Document("_id", "actor-a")).first();
                    if (first.compareAndSet(true, false)) { read.countDown(); await(revoked); }
                    if (Boolean.TRUE.equals(actor.get("revoked"))) { return false; }
                    ConfigurationVersion.advanceAlert(s, versions(f)); return true;
                }));
                await(read); f.domain().tokens().revoke("actor-a"); revoked.countDown();
                assertThat(permission.get(15, TimeUnit.SECONDS)).isFalse();
                assertThat(versions(f).find().first().getLong(ConfigurationVersion.ALERT_FENCE)).isZero();
                assertThat(f.domain().configurationRevision()).isEqualTo(2L);
            } catch (Exception failure) { throw new AssertionError(failure); }
            finally { revoked.countDown(); }
        });
    }

    @Test void aNewSilenceAfterTheSnapshotCannotBeMissedByAPermissionTransaction() {
        withConfiguration(false, f -> {
            CountDownLatch read = new CountDownLatch(1), changed = new CountDownLatch(1);
            AtomicBoolean first = new AtomicBoolean(true);
            try (var executor = Executors.newSingleThreadExecutor()) {
                var permission = executor.submit(() -> transaction(f, s -> {
                    long silences = SystemCollections.ALERT_SILENCES.on(f.database()).countDocuments(s);
                    if (first.compareAndSet(true, false)) { read.countDown(); await(changed); }
                    if (silences != 0) { return false; }
                    ConfigurationVersion.advanceAlert(s, versions(f)); return true;
                }));
                await(read);
                var store = new MongoAlertConfigurationStore(f.database(), AlertConfigurationFixtures.SCOPE,
                        io.tapstate.core.alerting.BuiltinAlarmTypes.catalog(), () -> {});
                transaction(f, s -> {
                    ConfigurationVersion.advance(s, versions(f));
                    return store.stage(s, null, AlertConfigurationFixtures.silence());
                });
                changed.countDown(); assertThat(permission.get(15, TimeUnit.SECONDS)).isFalse();
                assertThat(versions(f).find().first().getLong(ConfigurationVersion.ALERT_FENCE)).isZero();
                assertThat(f.domain().configurationRevision()).isEqualTo(1L);
            } catch (Exception failure) { throw new AssertionError(failure); }
            finally { changed.countDown(); }
        });
    }

    @Test void exhaustedCounterCannotCommitAProtectedMutation() {
        withConfiguration(false, f -> {
            versions(f).updateOne(new Document(), new Document("$set", new Document(ConfigurationVersion.ALERT_FENCE, Long.MAX_VALUE - 1)));
            assertUnreadable(() -> transaction(f, s -> ConfigurationVersion.advanceAlert(s, versions(f))));
            assertThat(versions(f).find().first().getLong(ConfigurationVersion.ALERT_FENCE)).isEqualTo(Long.MAX_VALUE - 1);
            assertThat(f.domain().configurationRevision()).isZero();
        });
    }

    @Test void ordinaryReopenReadsOneCommittedMarkerCounterPairWhileExplicitUpgradeCommits() {
        withConfiguration(false, f -> {
            versionOne(f);
            CountDownLatch schemaRead = new CountDownLatch(1), upgraded = new CountDownLatch(1);
            AtomicBoolean pause = new AtomicBoolean(false);
            var listener = new com.mongodb.event.CommandListener() {
                private volatile int schemaRequest;
                @Override public void commandStarted(com.mongodb.event.CommandStartedEvent event) {
                    if ("find".equals(event.getCommandName()) && "schema_migrations".equals(event.getCommand().getString("find").getValue())) {
                        schemaRequest = event.getRequestId();
                    }
                }
                @Override public void commandSucceeded(com.mongodb.event.CommandSucceededEvent event) {
                    if (event.getRequestId() == schemaRequest && pause.compareAndSet(false, true)) {
                        schemaRead.countDown(); await(upgraded);
                    }
                }
            };
            try (var client = MongoClients.create(com.mongodb.MongoClientSettings.builder()
                    .applyConnectionString(new com.mongodb.ConnectionString(MONGO.getReplicaSetUrl(f.database().getName())))
                    .addCommandListener(listener).build()); var executor = Executors.newSingleThreadExecutor()) {
                var reopening = executor.submit(() -> {
                    var facts = MongoConfigurationDomain.openingFacts(client, client.getDatabase(f.database().getName()));
                    return MongoAlertStorageSchema.verifyAuthorityState(facts.schema(), facts.version(), "cluster-a", facts.key().getString("keyId"));
                });
                await(schemaRead);
                assertThat(f.domain().prepareAlertStorage("racing-authority-upgrade")).isEqualTo(2);
                upgraded.countDown();
                assertThat(reopening.get(15, TimeUnit.SECONDS).getInteger("installedVersion")).isEqualTo(1);
                try (var reopened = MongoConfigurationDomain.open(f.settings(), "cluster-a")) { assertThat(reopened.alertSchemaVersion()).isEqualTo(2); }
            } catch (Exception failure) { throw new AssertionError(failure); }
            finally { upgraded.countDown(); }
        });
    }

    private static com.mongodb.client.MongoCollection<Document> versions(Fixture f) { return SystemCollections.CONFIGURATION_VERSION.on(f.database()); }
    private static void versionOne(Fixture f) {
        SystemCollections.SCHEMA_MIGRATIONS.on(f.database()).updateOne(new Document("_id", "config"),
                new Document("$set", new Document("alerting.installedVersion", 1))
                        .append("$unset", new Document("alerting.authorityPreparationOperationId", "")));
        versions(f).updateOne(new Document(), new Document("$unset", new Document(ConfigurationVersion.ALERT_FENCE, "")));
    }
    private static <T> T transaction(Fixture f, java.util.function.Function<com.mongodb.client.ClientSession, T> body) {
        return new MongoReadBudget(Duration.ofSeconds(30)).transaction(f.client(), body);
    }
    private static void failCommit(Fixture f, Document failure) {
        f.client().getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                .append("data", new Document(failure).append("failCommands", List.of("commitTransaction"))));
    }
    private static void clearFailPoint(Fixture f) { f.client().getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
    private static void await(CountDownLatch latch) {
        try { if (!latch.await(10, TimeUnit.SECONDS)) { throw new AssertionError("transaction barrier timed out"); } }
        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
    }
    private static final class AbortProbe extends RuntimeException { }

    private record Fixture(MongoClient client, MongoDatabase database, MongoConfigurationDomain domain,
            MongoConnectionSettings settings) { }
    private static void withConfiguration(boolean legacy, Consumer<Fixture> scenario) {
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = client.getDatabase("alert_schema_" + UUID.randomUUID().toString().replace("-", ""));
            var settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(database.getName()), null, Duration.ofSeconds(5));
            try {
                try (var first = MongoConfigurationBootstrap.prepare(settings, "cluster-a", "installation-a", "setup-a", 1)) {
                    assertThat(first.clusterId()).isEqualTo("cluster-a");
                }
                if (legacy) {
                    // This fixture is exactly the pre-feature shape: original schema/key facts, no feature marker or tables.
                    SystemCollections.SCHEMA_MIGRATIONS.on(database).updateOne(new Document("_id", "config"),
                            new Document("$unset", new Document(MongoAlertStorageSchema.FIELD, "")));
                    SystemCollections.CONFIGURATION_VERSION.on(database).updateOne(new Document("_id", ConfigurationVersion.ID),
                            new Document("$unset", new Document(ConfigurationVersion.ALERT_FENCE, "")));
                    for (SystemCollections collection : MongoAlertStorageSchema.COLLECTIONS) { collection.on(database).drop(); }
                }
                try (var domain = MongoConfigurationDomain.open(settings, "cluster-a")) {
                    scenario.accept(new Fixture(client, database, domain, settings));
                }
            } finally { database.drop(); }
        }
    }
    private static Document schema(Fixture fixture) {
        return SystemCollections.SCHEMA_MIGRATIONS.on(fixture.database()).find(new Document("_id", "config")).first();
    }
    private static java.util.List<String> names(Fixture fixture) {
        return fixture.database().listCollectionNames().into(new ArrayList<>());
    }
    private static void assertAllTables(Fixture fixture) {
        assertThat(names(fixture)).containsAll(MongoAlertStorageSchema.COLLECTIONS.stream().map(SystemCollections::collectionName).toList());
    }
    private static void assertNoAlertTables(Fixture fixture) {
        assertThat(names(fixture)).doesNotContainAnyElementsOf(
                MongoAlertStorageSchema.COLLECTIONS.stream().map(SystemCollections::collectionName).toList());
    }
    private static void assertUnreadable(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }

    private static Document initialHead(String resourceId) {
        var key = new AlarmKey(new AlarmKey.Scope("org-a", "cluster-a"),
                new AlarmKey.Resource("pipeline", resourceId, "resource-1"), "pipeline.failed", "rule-a");
        var rule = new AlertRule(key, 1, 1, "producer-1", Duration.ZERO, Duration.ZERO,
                Duration.ofSeconds(15), Duration.ofSeconds(10));
        return MongoAlertStateCodec.head(AlertSnapshot.initial(rule), 1);
    }
}
