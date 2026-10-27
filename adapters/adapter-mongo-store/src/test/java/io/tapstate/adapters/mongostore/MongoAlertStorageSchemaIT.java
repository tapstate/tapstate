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
