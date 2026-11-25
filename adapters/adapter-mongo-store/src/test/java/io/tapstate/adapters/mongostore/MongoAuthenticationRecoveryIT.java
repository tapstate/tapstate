package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Durable minimum authentication recovery, independent from member or business initialization. */
@RequiresDocker
class MongoAuthenticationRecoveryIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final StorageSetupScope SETUP = new StorageSetupScope("cluster", "installation", "setup", 1);
    private static final Runnable ACTIVE = () -> {};

    @Test void anOlderBusinessIntentCannotBeReplannedOntoTheNewAuthenticationScope() {
        withFixture(StorageBinding.Mode.OP, f -> {
            var selected = new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.CLUSTER, Set.of());
            var original = f.configuration.recoveryCommands().plan(f.scope(), f.manifest(selected), null, null);
            f.configuration.recoveryCommands().confirm(original);
            Document retained = f.config.getCollection("storage_recovery").find().first().get("pendingRecovery", Document.class);
            f.cache.drop();
            try (var cache = connect(f.binding)) {
                var recovered = f.configuration.prepareAuthenticationRecovery(cache, ACTIVE);
                assertThat(f.configuration.recoveryCommands().current(recovered.authScope()).orElseThrow().originalScope()).isEqualTo(original.originalScope());
                assertThatThrownBy(() -> f.configuration.recoveryCommands().confirm(original)).isInstanceOf(TapstateException.class);
                assertThatThrownBy(() -> f.configuration.recoveryCommands().plan(recovered.authScope(), f.manifest(selected), original.operationId(), null))
                        .isInstanceOf(TapstateException.class);
                assertThat(f.config.getCollection("storage_recovery").find().first().get("pendingRecovery")).isEqualTo(retained);
            }
        });
    }

    @ParameterizedTest @EnumSource(StorageBinding.Mode.class)
    void concurrentClientsReserveOneTupleAndCrashContinuationDoesNotAllocateAnother(StorageBinding.Mode mode) {
        withFixture(mode, f -> {
            var original = f.configuration.bindings().cachePreparation().orElseThrow(); f.cache.drop();
            CachePreparation reserved;
            try (var other = MongoConfigurationDomain.open(f.settings, "cluster"); var workers = Executors.newFixedThreadPool(2)) {
                var start = new CountDownLatch(1);
                var first = workers.submit(() -> { start.await(); return ((MongoStorageBindingStore)f.configuration.bindings()).reserveAuthenticationRecovery(original); });
                var second = workers.submit(() -> { start.await(); return ((MongoStorageBindingStore)other.bindings()).reserveAuthenticationRecovery(original); });
                start.countDown();
                var a = first.get(15, TimeUnit.SECONDS); var b = second.get(15, TimeUnit.SECONDS);
                assertThat(a).isEqualTo(b); assertThat(a.pending()).isTrue(); reserved = a.preparation();
                assertThat(reserved.authScope().authGeneration()).isEqualTo(2);
            }
            try (var reopened = MongoConfigurationDomain.open(f.settings, "cluster"); var cache = connect(f.binding)) {
                // Simulate CACHE commit followed by process loss before CONFIG publishes readiness.
                var bindings = (MongoStorageBindingStore)reopened.bindings();
                cache.prepareRecoveryAuthentication(reserved, () -> bindings.requireAuthenticationRecoveryPending(reserved));
            }
            try (var reopened = MongoConfigurationDomain.open(f.settings, "cluster"); var cache = connect(f.binding)) {
                assertThat(reopened.prepareAuthenticationRecovery(cache, ACTIVE)).isEqualTo(reserved);
                Document published = f.config.getCollection("storage_recovery").find().first();
                assertThat(reopened.prepareAuthenticationRecovery(cache, ACTIVE)).isEqualTo(reserved);
                assertThat(f.config.getCollection("storage_recovery").find().first()).isEqualTo(published);
                assertThat(published).containsEntry("phase", "AUTH_RECOVERY_READY").containsEntry("revision", 2L);
                assertThat(f.cache.listCollectionNames().into(new ArrayList<>())).containsExactlyInAnyOrderElementsOf(mode == StorageBinding.Mode.OP
                        ? List.of("cache_identity", "schema_migrations", "op_sessions")
                        : List.of("cache_identity", "schema_migrations", "cloud_sessions", "cloud_revocations"));
                f.cache.drop();
                var next = reopened.prepareAuthenticationRecovery(cache, ACTIVE);
                assertThat(next.authScope().authGeneration()).isEqualTo(3);
                assertThat(next.operationId()).isNotEqualTo(reserved.operationId());
                assertThat(next.authScope().cacheIncarnation()).isNotEqualTo(reserved.authScope().cacheIncarnation());
                assertThatThrownBy(() -> reopened.requireAuthenticationPublished(reserved)).isInstanceOf(TapstateException.class);
                assertThatThrownBy(() -> cache.prepareRecoveryAuthentication(reserved, () -> bindings(reopened).requireAuthenticationRecoveryPending(reserved)))
                        .isInstanceOf(TapstateException.class);
            }
        });
    }

    @Test void aDistinctRecoveryCanExplicitlyNameItsCompletedPredecessorAfterAnotherCacheLoss() {
        withFixture(StorageBinding.Mode.OP, f -> {
            var selected = new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.CLUSTER, Set.of());
            var commands = f.configuration.recoveryCommands();
            var original = commands.plan(f.scope(), f.manifest(selected), null, null);
            var completed = commands.complete(commands.start(commands.confirm(original)));
            f.cache.drop();
            try (var cache = connect(f.binding)) {
                var recovered = f.configuration.prepareAuthenticationRecovery(cache, ACTIVE);
                assertThatThrownBy(() -> commands.plan(recovered.authScope(), f.manifest(selected), original.operationId(), null))
                        .isInstanceOf(TapstateException.class);
                var next = commands.plan(recovered.authScope(), f.manifest(selected), null, completed.operationId());
                assertThat(next.operationId()).isNotEqualTo(original.operationId());
                assertThat(next.previousOperationId()).isEqualTo(original.operationId());
                assertThat(next.originalScope()).isEqualTo(recovered.authScope());
                assertThat(next.phase()).isEqualTo(StorageRecoveryCommands.Phase.PLANNED);
            }
        });
    }

    @Test void partialContentsAndMissingOriginalAdministratorRefuseWithoutRepair() {
        withFixture(StorageBinding.Mode.OP, f -> {
            Document authority = f.config.getCollection("storage_recovery").find().first();
            f.cache.getCollection("op_sessions").drop();
            try (var cache = connect(f.binding)) {
                assertThatThrownBy(() -> f.configuration.prepareAuthenticationRecovery(cache, ACTIVE)).isInstanceOf(TapstateException.class);
                assertThat(f.cache.listCollectionNames().into(new ArrayList<>())).doesNotContain("op_sessions");
                assertThat(f.config.getCollection("storage_recovery").find().first()).isEqualTo(authority);
                f.cache.drop(); f.cache.createCollection("foreign_data");
                assertThatThrownBy(() -> f.configuration.prepareAuthenticationRecovery(cache, ACTIVE)).isInstanceOf(TapstateException.class);
                assertThat(f.cache.listCollectionNames().into(new ArrayList<>())).containsExactly("foreign_data");
                assertThat(f.config.getCollection("storage_recovery").find().first()).isEqualTo(authority);
                f.cache.drop(); f.config.getCollection("users").deleteMany(new Document());
                assertThatThrownBy(() -> f.configuration.prepareAuthenticationRecovery(cache, ACTIVE)).isInstanceOf(TapstateException.class);
                assertThat(f.cache.listCollectionNames().into(new ArrayList<>())).isEmpty();
                assertThat(f.config.getCollection("storage_recovery").find().first()).isEqualTo(authority);
            }
        });
    }

    private static MongoStorageBindingStore bindings(MongoConfigurationDomain configuration) { return (MongoStorageBindingStore)configuration.bindings(); }
    private static MongoCacheDomain connect(StorageBinding binding) { return MongoCacheDomain.connect(binding.cache(), Duration.ofSeconds(5), "authentication-recovery-test"); }
    private record Fixture(MongoConfigurationDomain configuration, MongoDatabase config, MongoDatabase cache,
            MongoConnectionSettings settings, StorageBinding binding, SourceResource target) {
        CacheAuthScope scope() { return configuration.bindings().cachePreparation().orElseThrow().authScope(); }
        StorageRecoveryCommands.Manifest manifest(StorageRecoveryCommands.Selection selection) {
            return new StorageRecoveryCommands.Manifest(selection, Set.of(), Map.of(target.id(), CanonicalHash.of(target)),
                    "a".repeat(64), configuration.configurationRevision());
        }
    }
    private static void withFixture(StorageBinding.Mode mode, Scenario scenario) {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        try (var admin = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var config = admin.getDatabase("auth_config_" + suffix); var cache = admin.getDatabase("auth_cache_" + suffix);
            var settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(config.getName()), null, Duration.ofSeconds(5));
            try (var configuration = MongoConfigurationBootstrap.prepare(settings, "cluster", "installation", "setup", 1)) {
                var connection = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, MONGO.getReplicaSetUrl(cache.getName()), cache.getName(), null);
                var target = new SourceResource(StorageBinding.defaultSourceId(mode), null, "mongodb", Map.of("uri", "mongodb://master.invalid/master"), null, null, null, null);
                var committed = configuration.bindings().commitWithFirstCache(SETUP, mode, connection, target);
                try (var initial = connect(committed.binding())) {
                    initial.prepareFirstAuth(committed.firstCreation().orElseThrow()); initial.prepareInitialBusiness(committed.preparation(), ACTIVE);
                }
                configuration.bindings().complete(SETUP);
                if (mode == StorageBinding.Mode.OP) configuration.firstAdmin().create(configuration.firstAdmin().load().orElseThrow().scope(), new User("admin", "hash-fixture", "admin"));
                scenario.run(new Fixture(configuration, config, cache, settings, committed.binding(), target));
            } finally { config.drop(); cache.drop(); }
        } catch (Exception failure) { throw new AssertionError(failure); }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
