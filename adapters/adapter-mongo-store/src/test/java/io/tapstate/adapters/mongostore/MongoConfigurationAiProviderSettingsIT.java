package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClients;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.AiProviderSettingsStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static org.assertj.core.api.Assertions.*;

@RequiresDocker
class MongoConfigurationAiProviderSettingsIT {
    @Container static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
    private static final String CLUSTER = "provider-cluster";
    private static final String OWNER = JsonWriter.write(List.of("cloud", "organization", CLUSTER));
    private static final String SECRET = "provider-key-private-sentinel";
    private static AiProviderSettingsStore.Settings setting() {
        return new AiProviderSettingsStore.Settings("openai-compatible", "https://api.example.com/v1", "model", SECRET);
    }
    @Test void encryptedConfigurationSurvivesReopenAndExplicitDeletionWithoutAnyCache() {
        fixture((uri, config, raw) -> {
            var store = config.aiProviderSettings();
            assertThat(store.get(OWNER)).isEmpty();
            assertThat(store.selectManaged(OWNER, true)).isFalse();
            store.put(OWNER, setting());
            var saved = SystemCollections.CONFIG_AI_PROVIDER_SETTINGS.on(raw).find().first();
            assertThat(saved.toJson()).doesNotContain(SECRET);
            assertThat(saved.getString("secret")).startsWith("tsai:1:");
            assertThat(raw.listCollectionNames().into(new java.util.ArrayList<>())).doesNotContain("system_meta", "cache_identity");
            try (var reopened = MongoConfigurationDomain.open(new MongoConnectionSettings(uri, null, Duration.ofSeconds(5)), CLUSTER)) {
                var persisted = reopened.aiProviderSettings();
                assertThat(persisted.get(OWNER)).contains(setting());
                assertThat(persisted.selectManaged(OWNER, true)).isTrue();
                assertThat(store.get(OWNER)).hasValueSatisfying(value -> {
                    assertThat(value.managedSelected()).isTrue();
                    assertThat(value.apiKey()).isEqualTo(SECRET);
                });
                var parked = SystemCollections.CONFIG_AI_PROVIDER_SETTINGS.on(raw).find().first();
                assertThat(parked.getString("secret")).isEqualTo(saved.getString("secret"));
                assertThat(persisted.selectManaged(OWNER, false)).isTrue();
                assertThat(store.get(OWNER)).contains(setting());
                persisted.delete(OWNER);
            }
            try (var reopened = MongoConfigurationDomain.open(new MongoConnectionSettings(uri, null, Duration.ofSeconds(5)), CLUSTER)) {
                assertThat(reopened.aiProviderSettings().get(OWNER)).isEmpty();
            }
        });
    }
    @Test void onPremProviderIsEncryptedAndSurvivesReopen() {
        fixture((uri, config, raw) -> {
            var owner = JsonWriter.write(List.of("on-prem"));
            config.aiProviderSettings().put(owner, setting());
            assertThat(SystemCollections.CONFIG_AI_PROVIDER_SETTINGS.on(raw).find().first().toJson()).doesNotContain(SECRET);
            try (var reopened = MongoConfigurationDomain.open(new MongoConnectionSettings(uri, null, Duration.ofSeconds(5)), CLUSTER)) {
                assertThat(reopened.aiProviderSettings().get(owner)).contains(setting());
                reopened.aiProviderSettings().delete(owner);
                assertThat(reopened.aiProviderSettings().get(owner)).isEmpty();
            }
        });
    }
    @Test void responsesUsesTheExistingEncryptedRowAndReopensWithoutChangingLegacySettings() {
        fixture((uri, config, raw) -> {
            var store = config.aiProviderSettings();
            store.put(OWNER, setting());
            assertThat(store.get(OWNER)).contains(setting());
            var responses = new AiProviderSettingsStore.Settings("openai-responses", setting().baseUrl(), "gpt-6.1-sol", SECRET);
            store.put(OWNER, responses);
            var rows = SystemCollections.CONFIG_AI_PROVIDER_SETTINGS.on(raw);
            assertThat(rows.countDocuments()).isEqualTo(1);
            var saved = rows.find().first();
            assertThat(saved.getInteger("formatVersion")).isEqualTo(1);
            assertThat(saved.toJson()).doesNotContain(SECRET);
            try (var reopened = MongoConfigurationDomain.open(new MongoConnectionSettings(uri, null, Duration.ofSeconds(5)), CLUSTER)) {
                assertThat(reopened.aiProviderSettings().get(OWNER)).contains(responses);
            }
            var tampered = new Document(saved).append("providerId", "openai-compatible");
            rows.replaceOne(new Document(), tampered);
            assertThatThrownBy(() -> store.get(OWNER)).isInstanceOf(TapstateException.class);
            rows.replaceOne(new Document(), saved);
            store.put(OWNER, setting());
            assertThat(store.get(OWNER)).contains(setting());
        });
    }
    @Test void corruptedOrForeignRowsCannotBeReadOverwrittenOrDeleted() {
        fixture((uri, config, raw) -> {
            var store = config.aiProviderSettings(); store.put(OWNER, setting());
            var rows = SystemCollections.CONFIG_AI_PROVIDER_SETTINGS.on(raw); var saved = rows.find().first();
            for (var mutation : List.of(new Document("formatVersion", 1L), new Document("owner", "foreign"),
                    new Document("providerId", "other"), new Document("baseUrl", "https://changed.example/v1"),
                    new Document("secret", "tsai:1:bad"), new Document("extra", "unknown"))) {
                var changed = new Document(saved); changed.putAll(mutation); rows.replaceOne(new Document(), changed);
                assertThatThrownBy(() -> store.get(OWNER)).isInstanceOf(TapstateException.class);
                assertThatThrownBy(() -> store.put(OWNER, setting())).isInstanceOf(TapstateException.class);
                assertThatThrownBy(() -> store.delete(OWNER)).isInstanceOf(TapstateException.class);
                assertThat(rows.find().first()).isEqualTo(changed); rows.replaceOne(new Document(), saved);
            }
            assertThatThrownBy(() -> store.get(JsonWriter.write(List.of("cloud", "other", CLUSTER))))
                    .isInstanceOf(TapstateException.class);
            assertThat(rows.find().first()).isEqualTo(saved);
        });
    }
    @Test void ordinarySettingsOperationsLeaveCatalogDamageToInspection() {
        fixture((uri, config, raw) -> {
            var store = config.aiProviderSettings();
            var rows = SystemCollections.CONFIG_AI_PROVIDER_SETTINGS.on(raw);
            rows.createIndex(new Document("model", 1), new com.mongodb.client.model.IndexOptions()
                    .name("extra_expiry").expireAfter(1L, java.util.concurrent.TimeUnit.SECONDS));
            store.put(OWNER, setting());
            assertThat(store.get(OWNER)).contains(setting());
            assertThat(config.inspectStructure(new MongoReadBudget(Duration.ofSeconds(5))))
                    .anySatisfy(issue -> {
                        assertThat(issue.index()).isEqualTo("extra_expiry");
                        assertThat(issue.problem()).isEqualTo(MongoStructureInspection.Problem.UNEXPECTED_CONSTRAINT);
                    });
            rows.drop();
            assertThat(store.get(OWNER)).isEmpty();
            assertThat(config.inspectStructure(new MongoReadBudget(Duration.ofSeconds(5))))
                    .anySatisfy(issue -> {
                        assertThat(issue.collection()).isEqualTo(rows.getNamespace().getCollectionName());
                        assertThat(issue.problem()).isEqualTo(MongoStructureInspection.Problem.COLLECTION_MISSING);
                    });
            try (var diagnostics = MongoHttpDiagnosticsScope.open()) {
                store.put(OWNER, setting());
                assertThat(store.get(OWNER)).contains(setting());
                assertThat((List<?>) diagnostics.snapshot().get("mongo_groups")).noneSatisfy(group ->
                        assertThat(((java.util.Map<?, ?>) group).get("command")).isIn("listCollections", "listIndexes"));
            }
        });
    }
    @Test void legacyVersionTwoBackfillsOnceAndReopensWithoutCatalogQueries() {
        fixture((uri, config, raw) -> {
            config.aiProviderSettings().put(OWNER, setting());
            var schemas = SystemCollections.SCHEMA_MIGRATIONS.on(raw);
            var key = new Document("_id", "config");
            schemas.updateOne(key, new Document("$unset", new Document("aiOnly", "")));
            var legacy = schemas.find(key).first();
            var revision = SystemCollections.CONFIGURATION_VERSION.on(raw).find().first();
            var settings = new MongoConnectionSettings(uri, null, Duration.ofSeconds(5));

            try (var diagnostics = MongoHttpDiagnosticsScope.open()) {
                assertThatThrownBy(() -> MongoConfigurationDomain.open(settings, CLUSTER))
                        .isInstanceOfSatisfying(TapstateException.class, error ->
                            assertThat(error.code()).isEqualTo(StoreError.CONFIG_COMPONENT_IDENTITY_REQUIRED));
                assertThat(catalogCommands(diagnostics)).isEmpty();
            }
            assertThat(schemas.find(key).first()).isEqualTo(legacy);

            try (var diagnostics = MongoHttpDiagnosticsScope.open()) {
                assertThat(MongoConfigurationDomain.prepareConfigComponentIdentity(settings, CLUSTER)).isTrue();
                assertThat(catalogCommands(diagnostics)).containsExactly("listCollections");
            }
            try (var diagnostics = MongoHttpDiagnosticsScope.open()) {
                assertThat(MongoConfigurationDomain.prepareConfigComponentIdentity(settings, CLUSTER)).isFalse();
                assertThat(catalogCommands(diagnostics)).isEmpty();
            }
            assertThat(schemas.find(key).first()).isEqualTo(new Document(legacy).append("aiOnly", true));
            assertThat(SystemCollections.CONFIGURATION_VERSION.on(raw).find().first()).isEqualTo(revision);
            try (var diagnostics = MongoHttpDiagnosticsScope.open();
                 var reopened = MongoConfigurationDomain.open(settings, CLUSTER)) {
                assertThat(catalogCommands(diagnostics)).isEmpty();
                var mongo = (Map<?, ?>) diagnostics.snapshot().get("mongo");
                System.out.println("CONFIG_COMPONENT_OPEN mongoCommands=" + mongo.get("started") + " catalogCommands=0");
                assertThat(reopened.aiProviderSettings().get(OWNER)).contains(setting());
                assertThat(catalogCommands(diagnostics)).isEmpty();
            }

            SystemCollections.CONFIG_AI_PROVIDER_SETTINGS.on(raw).drop();
            try (var diagnostics = MongoHttpDiagnosticsScope.open();
                 var reopened = MongoConfigurationDomain.openExisting(settings)) {
                assertThat(catalogCommands(diagnostics)).isEmpty();
                var mongo = (Map<?, ?>) diagnostics.snapshot().get("mongo");
                System.out.println("CONFIG_COMPONENT_OPEN_EXISTING mongoCommands=" + mongo.get("started") + " catalogCommands=0");
                assertThat(reopened.aiProviderSettings().get(OWNER)).isEmpty();
                assertThat(catalogCommands(diagnostics)).isEmpty();
            }
        });
    }

    private static List<String> catalogCommands(MongoHttpDiagnosticsScope diagnostics) {
        var snapshot = diagnostics.snapshot();
        assertThat(snapshot).containsEntry("complete", true);
        return ((List<?>) snapshot.get("mongo_groups")).stream()
                .map(group -> (String) ((Map<?, ?>) group).get("command"))
                .filter(name -> name.equals("listCollections") || name.equals("listIndexes") || name.equals("getMore"))
                .toList();
    }

    @Test void markerChangedDuringCatalogReadRefusesBackfill() {
        fixture((uri, config, raw) -> {
            var schemas = SystemCollections.SCHEMA_MIGRATIONS.on(raw);
            var key = new Document("_id", "config");
            schemas.updateOne(key, new Document("$unset", new Document("aiOnly", "")));
            var legacy = schemas.find(key).first();
            var changed = new java.util.concurrent.atomic.AtomicBoolean();
            MongoCommandObservation.COLLECTIONS.set((command, collection) -> {
                if (command.equals("listCollections") && changed.compareAndSet(false, true)) {
                    schemas.updateOne(key, new Document("$set", new Document("fixtureConcurrentChange", true)));
                }
            });
            try {
                var settings = new MongoConnectionSettings(uri, null, Duration.ofSeconds(5));
                assertThatThrownBy(() -> MongoConfigurationDomain.prepareConfigComponentIdentity(settings, CLUSTER))
                        .isInstanceOfSatisfying(TapstateException.class, error ->
                            assertThat(error.code()).isEqualTo(StoreError.CONFIG_COMPONENT_IDENTITY_UNVERIFIED));
            } finally {
                MongoCommandObservation.COLLECTIONS.remove();
            }
            assertThat(changed).isTrue();
            assertThat(schemas.find(key).first()).isEqualTo(new Document(legacy).append("fixtureConcurrentChange", true));
        });
    }

    @Test void absentLegacyAiNamespaceRequiresManualReviewWithoutPublishingIdentity() {
        fixture((uri, config, raw) -> {
            var schemas = SystemCollections.SCHEMA_MIGRATIONS.on(raw);
            var key = new Document("_id", "config");
            schemas.updateOne(key, new Document("$unset", new Document("aiOnly", "")));
            var legacy = schemas.find(key).first();
            SystemCollections.CONFIG_AI_PROVIDER_SETTINGS.on(raw).drop();
            var settings = new MongoConnectionSettings(uri, null, Duration.ofSeconds(5));
            assertThatThrownBy(() -> MongoConfigurationDomain.prepareConfigComponentIdentity(settings, CLUSTER))
                    .isInstanceOfSatisfying(TapstateException.class, error ->
                        assertThat(error.code()).isEqualTo(StoreError.CONFIG_COMPONENT_IDENTITY_UNVERIFIED));
            assertThat(schemas.find(key).first()).isEqualTo(legacy);
            assertThat(raw.listCollectionNames()).doesNotContain("ai_provider_settings");
        });
    }

    @Test void oldSchemaKeepsSourceConfigurationButRefusesProviderAccess() {
        fixture((uri, config, raw) -> {
            SystemCollections.SCHEMA_MIGRATIONS.on(raw).updateOne(new Document("_id", "config"), new Document("$set", new Document("installedVersion", 1)));
            SystemCollections.CONFIG_AI_PROVIDER_SETTINGS.on(raw).drop();
            try (var old = MongoConfigurationDomain.open(new MongoConnectionSettings(uri, null, Duration.ofSeconds(5)), CLUSTER)) {
                old.artifacts().save(new io.tapstate.core.model.SourceResource("source", null, "mongodb", Map.of("uri", "mongodb://localhost/data"), null, null, null, null));
                assertThat(old.artifacts().get("source")).isPresent();
                assertThatThrownBy(() -> old.aiProviderSettings().get(OWNER)).isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(StoreError.AI_PROVIDER_SCHEMA_UNSUPPORTED));
                assertThat(SystemCollections.SCHEMA_MIGRATIONS.on(raw).find().first().getInteger("installedVersion")).isEqualTo(1);
            }
        });
    }
    @Test void inputBoundsAndMissingOriginalKeyLeaveConfigurationUnchanged() {
        fixture((uri, config, raw) -> {
            var store = config.aiProviderSettings(); var before = SystemCollections.CONFIGURATION_VERSION.on(raw).find().first();
            assertThatThrownBy(() -> store.put(OWNER, new AiProviderSettingsStore.Settings("openai-compatible", "https://api.example.com/v1", "model", "x".repeat(8193))))
                    .isInstanceOf(TapstateException.class);
            assertThat(SystemCollections.CONFIGURATION_VERSION.on(raw).find().first()).isEqualTo(before);
            store.put(OWNER, setting()); SystemCollections.CLUSTER_KEYS.on(raw).deleteMany(new Document());
            assertThatThrownBy(() -> store.get(OWNER)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> store.put(OWNER, setting())).isInstanceOf(TapstateException.class);
        });
    }
    @Test void independentClientsCompetingForDifferentOwnersPublishOnlyOneRow() {
        fixture((uri, config, raw) -> {
            try (var other = MongoConfigurationDomain.open(new MongoConnectionSettings(uri, null, Duration.ofSeconds(5)), CLUSTER);
                    var workers = java.util.concurrent.Executors.newFixedThreadPool(2)) {
                var ready = new java.util.concurrent.CountDownLatch(2); var start = new java.util.concurrent.CountDownLatch(1);
                var stores = List.of(config.aiProviderSettings(), other.aiProviderSettings());
                var owners = List.of(OWNER, JsonWriter.write(List.of("cloud", "other", CLUSTER)));
                var results = new java.util.ArrayList<java.util.concurrent.Future<Boolean>>();
                for (int index = 0; index < 2; index++) {
                    int selected = index;
                    results.add(workers.submit(() -> {
                        ready.countDown(); assertThat(start.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                        try { stores.get(selected).put(owners.get(selected), setting()); return true; }
                        catch (TapstateException refused) { assertThat(refused.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE); return false; }
                    }));
                }
                assertThat(ready.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue(); start.countDown();
                int winners = 0;
                for (var result : results) if (result.get(30, java.util.concurrent.TimeUnit.SECONDS)) winners++;
                assertThat(winners).isEqualTo(1);
                assertThat(SystemCollections.CONFIG_AI_PROVIDER_SETTINGS.on(raw).countDocuments()).isOne();
                assertThat(SystemCollections.CONFIGURATION_VERSION.on(raw).find().first().getLong("revision")).isEqualTo(1);
                var row = SystemCollections.CONFIG_AI_PROVIDER_SETTINGS.on(raw).find().first();
                assertThat(config.aiProviderSettings().get(row.getString("owner"))).contains(setting());
            }
        });
    }

    @Test void aFailedRowWriteRollsBackBothConfigurationAndKeyFences() {
        fixture((uri, config, raw) -> {
            var store = config.aiProviderSettings();
            var version = SystemCollections.CONFIGURATION_VERSION.on(raw).find().first();
            var key = SystemCollections.CLUSTER_KEYS.on(raw).find().first();
            try (var client = MongoClients.create(uri)) {
                client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                        .append("data", new Document("failCommands", List.of("insert")).append("errorCode", 121)));
                try { assertThatThrownBy(() -> store.put(OWNER, setting())).isInstanceOf(TapstateException.class); }
                finally { client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
            }
            assertThat(SystemCollections.CONFIGURATION_VERSION.on(raw).find().first()).isEqualTo(version);
            assertThat(SystemCollections.CLUSTER_KEYS.on(raw).find().first()).isEqualTo(key);
            assertThat(SystemCollections.CONFIG_AI_PROVIDER_SETTINGS.on(raw).countDocuments()).isZero();
        });
    }
    @FunctionalInterface interface Scenario { void run(String uri, MongoConfigurationDomain config, com.mongodb.client.MongoDatabase raw) throws Exception; }
    private static void fixture(Scenario scenario) {
        String database = "ai_config_" + UUID.randomUUID().toString().replace("-", ""); String uri = MONGO.getReplicaSetUrl(database);
        try (var client = MongoClients.create(uri)) {
            try (var config = MongoConfigurationBootstrap.prepare(new MongoConnectionSettings(uri, null, Duration.ofSeconds(5)), CLUSTER, "installation", "operation", 1)) {
                scenario.run(uri, config, client.getDatabase(database));
            } finally { client.getDatabase(database).drop(); }
        } catch (RuntimeException | Error failure) { throw failure; }
        catch (Exception failure) { throw new AssertionError(failure); }
    }
}
