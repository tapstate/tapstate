package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CachePreparation;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The public controlled preparation entry publishes real structures and never repairs unknown history. */
@RequiresDocker
class MongoCacheBusinessPreparationIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final Runnable ACTIVE = () -> {};

    @Test void authenticationOnlyFactsCannotAuthorizeBusinessBirth() {
        withCache(StorageBinding.Mode.OP, Duration.ofDays(15), false, f -> {
            var before = f.names(); var root = f.root();
            assertThatThrownBy(() -> f.domain.prepareInitialBusiness(f.preparation, ACTIVE)).isInstanceOf(TapstateException.class);
            assertThat(f.names()).containsExactlyInAnyOrderElementsOf(before); assertThat(f.root()).isEqualTo(root);
        });
    }

    @Test void bothModesPublishAllBusinessStructuresAndRetainMinimalAuth() {
        for (var mode : StorageBinding.Mode.values()) withCache(mode, Duration.ofDays(7), f -> {
            Document before = f.root();
            assertThat(f.domain.prepareInitialBusiness(f.preparation, ACTIVE)).isEqualTo(f.preparation.businessScope());
            assertThat(f.root()).containsEntry("businessPhase", "READY").containsEntry("businessPreparationId", "setup-a")
                    .containsEntry("claimAdmissionFence", 0L).containsEntry("authPhase", "READY")
                    .containsEntry("authWriteFence", before.get("authWriteFence")).containsEntry("authGeneration", 1L);
            assertThat(f.domain.loadAuthScope(f.preparation.authScope())).isEqualTo(f.preparation.authScope());
            assertThat(f.domain.clusterMembership(f.preparation.businessScope()).read("cluster-a")).isEmpty();
            var owner = new WorkloadOwner("node-a", "boot-a");
            assertThat(f.domain.workloadClaims(f.preparation.businessScope()).acquire(
                    new WorkloadClaimKey("cluster-a", WorkloadClaimType.NODE_SESSION, "node-a"), owner, 0, Duration.ofMinutes(1)).claim()).isNotNull();
            Document ttl = SystemCollections.CACHE_PIPELINE_RATE_HISTORY.on(f.database).listIndexes().into(new ArrayList<>()).stream()
                    .filter(row -> row.containsKey("expireAfterSeconds")).findFirst().orElseThrow();
            assertThat(((Number) ttl.get("expireAfterSeconds")).longValue()).isEqualTo(Duration.ofDays(7).toSeconds());
            assertThat(f.database.listCollectionNames().into(new ArrayList<>())).doesNotContain("system_meta", "artifacts", "users", "pdk_notes");
        });
    }

    @Test void readinessReplayAndReopenDoNoDdlOrCounterWrites() {
        withCache(StorageBinding.Mode.OP, Duration.ofDays(15), f -> {
            f.domain.prepareInitialBusiness(f.preparation, ACTIVE);
            Document before = f.root(); var schema = f.schemas();
            try (var reopened = connect(f.endpoint, Duration.ofDays(15))) {
                assertThat(reopened.prepareInitialBusiness(f.preparation, ACTIVE)).isEqualTo(f.preparation.businessScope());
                assertThat(reopened.loadBusinessScope(f.preparation.businessScope())).isEqualTo(f.preparation.businessScope());
            }
            assertThat(f.root()).isEqualTo(before); assertThat(f.schemas()).isEqualTo(schema);
        });
    }

    @Test void lateFailureRollsBackAllDdlGenesisAndBusinessFactsThenSameOperationCanResume() {
        withCache(StorageBinding.Mode.OP, Duration.ofDays(15), f -> {
            var published = new AtomicBoolean(); var ddl = new AtomicInteger();
            MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(new CommandListener() {
                        @Override public void commandStarted(CommandStartedEvent event) {
                            if (!f.database.getName().equals(event.getDatabaseName())) return;
                            if (event.getCommandName().equals("createIndexes")) ddl.incrementAndGet();
                            var command = event.getCommand();
                            if (event.getCommandName().equals("insert") && command.getString("insert").getValue().equals("schema_migrations")
                                    && command.getArray("documents").getValues().stream().anyMatch(row -> row.asDocument().getString("_id").getValue().equals("business"))) published.set(true);
                        }
                    }).build();
            var rootBefore = f.root(); var namesBefore = f.names();
            try (var client = MongoClients.create(settings)) {
                var preparation = new MongoCacheBusinessPreparation(client, client.getDatabase(f.database.getName()), f.preparation, Duration.ofDays(15), () -> {
                    if (published.get()) throw refused();
                });
                assertThatThrownBy(preparation::prepare).isInstanceOf(TapstateException.class);
                assertThat(ddl.get()).isGreaterThan(7);
            }
            assertThat(f.names()).containsExactlyInAnyOrderElementsOf(namesBefore);
            assertThat(f.root()).isEqualTo(rootBefore); assertThat(f.schemas()).hasSize(1);
            assertThat(f.domain.loadAuthScope(f.preparation.authScope())).isEqualTo(f.preparation.authScope());
            assertThat(f.domain.prepareInitialBusiness(f.preparation, ACTIVE)).isEqualTo(f.preparation.businessScope());
        });
    }

    @Test void twoIndependentClientsConvergeOnOneReadyBusinessDomain() {
        withCache(StorageBinding.Mode.OP, Duration.ofDays(15), f -> {
            try (var other = connect(f.endpoint, Duration.ofDays(15)); var executor = Executors.newFixedThreadPool(2)) {
                var start = new CountDownLatch(1); var ready = new CountDownLatch(2);
                var a = executor.submit(() -> { ready.countDown(); assertThat(start.await(10, TimeUnit.SECONDS)).isTrue(); return f.domain.prepareInitialBusiness(f.preparation, ACTIVE); });
                var b = executor.submit(() -> { ready.countDown(); assertThat(start.await(10, TimeUnit.SECONDS)).isTrue(); return other.prepareInitialBusiness(f.preparation, ACTIVE); });
                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue(); start.countDown();
                assertThat(a.get(30, TimeUnit.SECONDS)).isEqualTo(f.preparation.businessScope());
                assertThat(b.get(30, TimeUnit.SECONDS)).isEqualTo(f.preparation.businessScope());
                assertThat(SystemCollections.CACHE_CLUSTER_MEMBERSHIP.on(f.database).countDocuments()).isOne();
                assertThat(f.root()).containsEntry("claimAdmissionFence", 0L); assertThat(f.schemas()).hasSize(2);
            } catch (Exception failure) { throw new AssertionError(failure); }
        });
    }

    @Test void aConcurrentWinnerAfterTheOriginalPendingReadIsReturnedWithoutRepairOrFalseRefusal() {
        withCache(StorageBinding.Mode.OP, Duration.ofDays(15), f -> {
            var readPending = new CountDownLatch(1); var resume = new CountDownLatch(1); var once = new AtomicBoolean();
            var creates = new AtomicInteger();
            MongoClientSettings settings = MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(new CommandListener() {
                        @Override public void commandStarted(CommandStartedEvent event) {
                            if (event.getCommandName().equals("create")) creates.incrementAndGet();
                        }
                        @Override public void commandSucceeded(com.mongodb.event.CommandSucceededEvent event) {
                            if (!event.getCommandName().equals("find") || !event.getResponse().containsKey("cursor")) return;
                            var batch = event.getResponse().getDocument("cursor").getArray("firstBatch");
                            if (batch.isEmpty()) return;
                            var row = batch.get(0).asDocument();
                            if (!row.containsKey("businessPhase") || !row.getString("businessPhase").getValue().equals("PREPARING")
                                    || !once.compareAndSet(false, true)) return;
                            readPending.countDown();
                            try { if (!resume.await(15, TimeUnit.SECONDS)) throw new AssertionError("resume barrier"); }
                            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
                        }
                    }).build();
            try (var observed = MongoClients.create(settings); var executor = Executors.newSingleThreadExecutor()) {
                var original = executor.submit(() -> new MongoCacheBusinessPreparation(observed, observed.getDatabase(f.database.getName()),
                        f.preparation, Duration.ofDays(15), ACTIVE).prepare());
                assertThat(readPending.await(10, TimeUnit.SECONDS)).isTrue();
                try { assertThat(f.domain.prepareInitialBusiness(f.preparation, ACTIVE)).isEqualTo(f.preparation.businessScope()); }
                finally { resume.countDown(); }
                assertThat(original.get(20, TimeUnit.SECONDS)).isEqualTo(f.preparation.businessScope());
                assertThat(creates.get()).as("the old pending attempt must observe the winner before doing its own DDL").isZero();
                assertThat(f.root()).containsEntry("claimAdmissionFence", 0L);
            } catch (Exception failure) { throw new AssertionError(failure); }
            finally { resume.countDown(); }
        });
    }

    @Test void missingRootOrSharedSchemaCollectionNeverGrantsBusinessCreation() {
        for (String fault : List.of("root", "schema")) withCache(StorageBinding.Mode.OP, Duration.ofDays(15), f -> {
            switch (fault) {
                case "root" -> SystemCollections.CACHE_IDENTITY.on(f.database).drop();
                case "schema" -> SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(f.database).drop();
                default -> throw new AssertionError(fault);
            }
            var before = f.names(); var root = f.root();
            assertThatThrownBy(() -> f.domain.prepareInitialBusiness(f.preparation, ACTIVE)).isInstanceOf(TapstateException.class);
            assertThat(f.names()).containsExactlyInAnyOrderElementsOf(before); assertThat(f.root()).isEqualTo(root);
        });
    }

    @Test void pendingBusinessWithDamagedControlOrRetainedCollectionsIsUnknownAndNeverAdopted() {
        for (String fault : List.of("preparation", "counter", "retirement", "schema", "collection", "legacy")) withCache(StorageBinding.Mode.OP, Duration.ofDays(15), f -> {
            switch (fault) {
                case "preparation" -> f.damageRoot("businessPreparationId", "old-op");
                case "counter" -> f.damageRoot("claimAdmissionFence", 0L);
                case "retirement" -> f.damageRoot("businessRetirementId", "retired");
                case "schema" -> SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(f.database).insertOne(new Document("_id", "business").append("installedVersion", 1));
                case "collection" -> f.database.createCollection("srs_models");
                case "legacy" -> f.database.createCollection("system_meta");
                default -> throw new AssertionError(fault);
            }
            var before = f.names(); var root = f.root(); var schemas = f.schemas();
            assertThatThrownBy(() -> f.domain.prepareInitialBusiness(f.preparation, ACTIVE)).isInstanceOf(TapstateException.class);
            assertThat(f.names()).containsExactlyInAnyOrderElementsOf(before); assertThat(f.root()).isEqualTo(root); assertThat(f.schemas()).isEqualTo(schemas);
        });
    }

    @Test void missingReadyStructuresIndexesOrGenesisRefuseWithoutRecreation() {
        for (String fault : List.of("claims", "chunks", "model-index", "genesis", "counter")) withCache(StorageBinding.Mode.OP, Duration.ofDays(15), f -> {
            f.domain.prepareInitialBusiness(f.preparation, ACTIVE);
            switch (fault) {
                case "claims" -> SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database).drop();
                case "chunks" -> SystemCollections.CACHE_CONNECTOR_ARTIFACTS.chunksOn(f.database).drop();
                case "model-index" -> SystemCollections.SRS_MODELS.on(f.database).dropIndex("srs_models_ring_version");
                case "genesis" -> SystemCollections.CACHE_CLUSTER_MEMBERSHIP.on(f.database).deleteMany(new Document());
                case "counter" -> SystemCollections.CACHE_IDENTITY.on(f.database).updateOne(new Document(), new Document("$unset", new Document("claimAdmissionFence", "")));
                default -> throw new AssertionError(fault);
            }
            var before = f.names(); var root = f.root();
            assertThatThrownBy(() -> f.domain.prepareInitialBusiness(f.preparation, ACTIVE)).isInstanceOf(TapstateException.class);
            assertThat(f.names()).containsExactlyInAnyOrderElementsOf(before); assertThat(f.root()).isEqualTo(root);
            assertThat(f.domain.loadAuthScope(f.preparation.authScope())).isEqualTo(f.preparation.authScope());
        });
    }

    @Test void wrongOperationScopeRetiredDomainAndClosedOwnerNeverReopenBusiness() {
        withCache(StorageBinding.Mode.OP, Duration.ofDays(15), f -> {
            assertThatThrownBy(() -> f.domain.prepareInitialBusiness(new CachePreparation("different-op", f.preparation.authScope()), ACTIVE)).isInstanceOf(TapstateException.class);
            f.domain.prepareInitialBusiness(f.preparation, ACTIVE);
            assertThatThrownBy(() -> f.domain.prepareInitialBusiness(new CachePreparation("different-op", f.preparation.authScope()), ACTIVE)).isInstanceOf(TapstateException.class);
            f.domain.retireBusiness(f.preparation.businessScope(), "retire-a", ACTIVE);
            var before = f.root();
            assertThatThrownBy(() -> f.domain.prepareInitialBusiness(f.preparation, ACTIVE)).isInstanceOf(TapstateException.class);
            assertThat(f.root()).isEqualTo(before); f.domain.close();
            assertThatThrownBy(() -> f.domain.prepareInitialBusiness(f.preparation, ACTIVE)).isInstanceOf(TapstateException.class);
        });
    }

    @Test void changedRetentionRequiresOfflineTtlMaintenanceAndAuthRemainsUsable() {
        withCache(StorageBinding.Mode.OP, Duration.ofDays(15), f -> {
            f.domain.prepareInitialBusiness(f.preparation, ACTIVE); var root = f.root();
            try (var changed = connect(f.endpoint, Duration.ofDays(7))) {
                assertThatThrownBy(() -> changed.prepareInitialBusiness(f.preparation, ACTIVE)).isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(StoreError.HISTORY_RETENTION_MISMATCH));
                assertThat(changed.loadAuthScope(f.preparation.authScope())).isEqualTo(f.preparation.authScope());
            }
            assertThat(f.root()).isEqualTo(root);
        });
    }

    @Test void authenticationResetDoesNotInvalidatePendingOrReadyBusinessPreparation() {
        for (boolean ready : List.of(false, true)) withCache(StorageBinding.Mode.OP, Duration.ofDays(15), f -> {
            if (ready) f.domain.prepareInitialBusiness(f.preparation, ACTIVE);
            var next = f.domain.advanceAuthGeneration(f.preparation.authScope(), 2, "auth-reset", ACTIVE);
            var authBefore = SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(f.database).find(new Document("_id", "auth")).first();
            long authFence = f.root().getLong("authWriteFence");
            assertThat(f.domain.prepareInitialBusiness(f.preparation, ACTIVE)).isEqualTo(f.preparation.businessScope());
            assertThat(f.domain.loadAuthScope(next)).isEqualTo(next);
            assertThat(f.root()).containsEntry("authGeneration", 2L).containsEntry("authPreparationId", "auth-reset").containsEntry("authWriteFence", authFence);
            assertThat(SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(f.database).find(new Document("_id", "auth")).first()).isEqualTo(authBefore);
        });
    }

    @Test void businessBirthPreparationDoesNotRequireAuthenticationReadinessOrCreateAuthResources() {
        withCache(StorageBinding.Mode.OP, Duration.ofDays(15), f -> {
            f.damageRoot("authPhase", "PREPARING");
            SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(f.database).updateOne(new Document("_id", "auth"), new Document("$set", new Document("phase", "PREPARING").append("installedVersion", 0)));
            SystemCollections.OP_SESSIONS.on(f.database).drop();
            var authBefore = SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(f.database).find(new Document("_id", "auth")).first();
            assertThat(f.domain.prepareInitialBusiness(f.preparation, ACTIVE)).isEqualTo(f.preparation.businessScope());
            assertThat(f.root()).containsEntry("authPhase", "PREPARING");
            assertThat(SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(f.database).find(new Document("_id", "auth")).first()).isEqualTo(authBefore);
            assertThat(f.names()).doesNotContain("op_sessions");
            assertThatThrownBy(() -> f.domain.loadAuthScope(f.preparation.authScope())).isInstanceOf(TapstateException.class);
        });
    }

    private static void withCache(StorageBinding.Mode mode, Duration retention, java.util.function.Consumer<Fixture> action) {
        withCache(mode, retention, true, action);
    }
    private static void withCache(StorageBinding.Mode mode, Duration retention, boolean birth, java.util.function.Consumer<Fixture> action) {
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            MongoDatabase db = client.getDatabase("business_" + UUID.randomUUID().toString().replace("-", ""));
            var endpoint = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, MONGO.getReplicaSetUrl(), db.getName(), null);
            var preparation = new CachePreparation("setup-a", new CacheAuthScope("cluster-a", 1, mode, UUID.randomUUID().toString(), 1));
            try (var domain = connect(endpoint, retention)) {
                if (birth) {
                    String configName = "birth_config_" + UUID.randomUUID().toString().replace("-", "");
                    var settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(configName), null, Duration.ofSeconds(10));
                    try (var config = MongoConfigurationBootstrap.prepare(settings, "cluster-a", "installation-a", "setup-a", 1)) {
                        var source = new io.tapstate.core.model.SourceResource(StorageBinding.defaultSourceId(mode), null, "mongodb", Map.of("uri", "mongodb://master.invalid/views"), null, null, null, null);
                        var committed = config.bindings().commitWithFirstCache(new io.tapstate.spi.store.StorageSetupScope("cluster-a", "installation-a", "setup-a", 1), mode, endpoint, source);
                        preparation = committed.preparation();
                        domain.prepareFirstAuth(committed.firstCreation().orElseThrow());
                        action.accept(new Fixture(client, db, domain, endpoint, preparation));
                    } finally { client.getDatabase(configName).drop(); }
                } else {
                    domain.prepareAuth(preparation.authScope(), preparation.operationId(), ACTIVE);
                    action.accept(new Fixture(client, db, domain, endpoint, preparation));
                }
            } finally { db.drop(); }
        }
    }
    private static MongoCacheDomain connect(StorageBinding.Connection endpoint, Duration retention) {
        return MongoCacheDomain.connect(endpoint, Duration.ofSeconds(10), "business-it", retention);
    }
    private record Fixture(MongoClient client, MongoDatabase database, MongoCacheDomain domain, StorageBinding.Connection endpoint, CachePreparation preparation) {
        Document root() { return SystemCollections.CACHE_IDENTITY.on(database).find().first(); }
        List<Document> schemas() { return SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).find().sort(new Document("_id", 1)).into(new ArrayList<>()); }
        List<String> names() { return database.listCollectionNames().into(new ArrayList<>()); }
        void damageRoot(String field, Object value) { SystemCollections.CACHE_IDENTITY.on(database).updateOne(new Document(), new Document("$set", new Document(field, value))); }
    }
    private static TapstateException refused() { return new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null); }
}
