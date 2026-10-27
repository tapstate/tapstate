package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClients;
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
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Native winner, original-grant fencing and exact retained-operation recovery on real CONFIG and CACHE. */
@RequiresDocker
class MongoInitialOpRecoveryIT {
    @Container static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    private static final class Fixture implements AutoCloseable {
        final String suffix = UUID.randomUUID().toString().replace("-", "");
        final String configName = "config_" + suffix, cacheName = "cache_" + suffix;
        final StorageSetupScope setup = new StorageSetupScope("cluster-" + suffix, "installation", "setup", 1);
        final MongoConnectionSettings settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(configName), null, Duration.ofSeconds(5));
        final MongoConfigurationDomain configuration = MongoConfigurationBootstrap.prepare(settings, setup.clusterId(), setup.installationId(), setup.operationId(), 1);
        final StorageBinding.Connection endpoint = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, MONGO.getReplicaSetUrl(cacheName), cacheName, null);
        final SourceResource source = new SourceResource("views", null, "mongodb", Map.of("isUri", true, "uri", MONGO.getReplicaSetUrl("master_" + suffix)), null, null, null, null);
        final StorageBindingCommit binding = configuration.bindings().commitWithFirstCache(setup, StorageBinding.Mode.OP, endpoint, source);
        final MongoCacheDomain cache = MongoCacheDomain.connect(endpoint, Duration.ofSeconds(5), "initial-op-recovery");
        StorageRecoveryCommands.Manifest manifest() {
            var actual = configuration.initialOpRecoverySource(binding.preparation().authScope(), false);
            cache.verifyEmptyInitialRecovery();
            return new StorageRecoveryCommands.Manifest(new StorageRecoveryCommands.Selection(StorageRecoveryCommands.Extent.CLUSTER, Set.of()),
                    Set.of(), Map.of(actual.id(), CanonicalHash.of(actual)), "a".repeat(64), configuration.configurationRevision());
        }
        StorageRecoveryCommands.Plan plan(String previous) {
            return configuration.recoveryCommands().plan(binding.preparation().authScope(), manifest(), null, previous);
        }
        @Override public void close() { cache.close(); configuration.close(); }
    }

    @Test void onlyTheCommittedStartWinnerCanCreateAndPublishingAPlanFencesTheOriginalGrant() throws Exception {
        try (var f = new Fixture(); var other = MongoConfigurationDomain.openExisting(f.settings); var executor = Executors.newFixedThreadPool(2)) {
            var planned = f.plan(null);
            assertThatThrownBy(() -> f.cache.prepareFirstAuth(f.binding.firstCreation().orElseThrow())).isInstanceOf(TapstateException.class);
            f.cache.verifyEmptyInitialRecovery();
            var confirmed = f.configuration.recoveryCommands().confirm(planned);
            var start = new CountDownLatch(1);
            var a = executor.submit(() -> { start.await(); return f.configuration.startInitialOpRecovery(confirmed, () -> {}); });
            var b = executor.submit(() -> { start.await(); return other.startInitialOpRecovery(confirmed, () -> {}); }); start.countDown();
            var first = a.get(15, TimeUnit.SECONDS); var second = b.get(15, TimeUnit.SECONDS);
            assertThat(List.of(first, second).stream().filter(MongoConfigurationDomain.InitialOpRecovery::mayCreate).count()).isOne();
            var winner = first.mayCreate() ? first : second;
            var loser = first.mayCreate() ? second : first;
            var winningDomain = first.mayCreate() ? f.configuration : other;
            var losingDomain = first.mayCreate() ? other : f.configuration;
            assertThatThrownBy(() -> losingDomain.prepareInitialOpRecovery(f.cache, loser, () -> {})).isInstanceOf(TapstateException.class);
            f.cache.verifyEmptyInitialRecovery();
            winningDomain.prepareInitialOpRecovery(f.cache, winner, () -> {});
            losingDomain.prepareInitialOpRecovery(f.cache, loser, () -> {});
            try (var raw = MongoClients.create(MONGO.getReplicaSetUrl())) {
                var identity = raw.getDatabase(f.cacheName).getCollection("cache_identity").find().first();
                assertThat(identity.getString("authPreparationId")).isEqualTo(confirmed.operationId());
                assertThat(identity.getString("businessPreparationId")).isEqualTo(confirmed.operationId());
                assertThat(identity.getLong("authGeneration")).isEqualTo(1L);
                assertThat(raw.getDatabase(f.configName).getCollection("op_bootstrap").countDocuments()).isZero();
            }
        }
    }

    @Test void missingIdentityAfterRunningNeedsAnExplicitReplacementAndInvalidatesTheOldInvocation() {
        try (var f = new Fixture()) {
            var commands = f.configuration.recoveryCommands();
            var first = f.configuration.startInitialOpRecovery(commands.confirm(f.plan(null)), () -> {});
            var restarted = f.configuration.startInitialOpRecovery(first.plan(), () -> {});
            assertThatThrownBy(() -> f.configuration.prepareInitialOpRecovery(f.cache, restarted, () -> {})).isInstanceOf(TapstateException.class);
            f.cache.verifyEmptyInitialRecovery();
            var replacement = f.plan(first.plan().operationId());
            assertThat(replacement.operationId()).isNotEqualTo(first.plan().operationId());
            assertThat(replacement.phase()).isEqualTo(StorageRecoveryCommands.Phase.PLANNED);
            assertThatThrownBy(() -> f.configuration.prepareInitialOpRecovery(f.cache, first, () -> {})).isInstanceOf(TapstateException.class);
            f.cache.verifyEmptyInitialRecovery();
            var current = f.configuration.startInitialOpRecovery(commands.confirm(replacement), () -> {});
            f.configuration.prepareInitialOpRecovery(f.cache, current, () -> {});
            try (var raw = MongoClients.create(MONGO.getReplicaSetUrl())) { raw.getDatabase(f.cacheName).drop(); }
            assertThatThrownBy(() -> f.configuration.prepareInitialOpRecovery(f.cache, current, () -> {})).isInstanceOf(TapstateException.class);
            f.cache.verifyEmptyInitialRecovery();
        }
    }

    @Test void completedRecoveryIsVerifiedAfterReopenBeforeOriginalSetupCompletes() {
        try (var f = new Fixture()) {
            var commands = f.configuration.recoveryCommands();
            var invocation = f.configuration.startInitialOpRecovery(commands.confirm(f.plan(null)), () -> {});
            f.configuration.prepareInitialOpRecovery(f.cache, invocation, () -> {});
            var completed = commands.complete(invocation.plan());
            assertThat(f.configuration.setupStates().load().phase()).isEqualTo(StorageSetupStateStore.Phase.STORAGE_BOUND);
            try (var reopened = MongoConfigurationDomain.openExisting(f.settings)) {
                var retained = reopened.startInitialOpRecovery(completed, () -> {});
                assertThat(retained.mayCreate()).isFalse(); reopened.prepareInitialOpRecovery(f.cache, retained, () -> {});
                reopened.bindings().complete(reopened.setupStates().load().scope());
                assertThat(reopened.firstAdmin().load().orElseThrow().phase()).isEqualTo(FirstAdminStore.Phase.PENDING);
                assertThatThrownBy(() -> reopened.initialOpRecoverySource(f.binding.preparation().authScope(), false)).isInstanceOf(TapstateException.class);
                reopened.prepareInitialOpRecovery(f.cache, retained, () -> {});
            }
        }
    }

    @Test void foreignCacheObjectsAndUnexpectedConfigurationHistoryRefuseTheInitialClosure() {
        try (var f = new Fixture(); var raw = MongoClients.create(MONGO.getReplicaSetUrl())) {
            raw.getDatabase(f.cacheName).createCollection("foreign");
            assertThatThrownBy(f::manifest).isInstanceOf(TapstateException.class);
            raw.getDatabase(f.cacheName).drop();
            raw.getDatabase(f.configName).getCollection("tokens").insertOne(new Document("_id", "unexpected"));
            assertThatThrownBy(f::manifest).isInstanceOf(TapstateException.class);
            assertThat(raw.getDatabase(f.configName).getCollection("storage_recovery").find().first().containsKey("pendingRecovery")).isFalse();
        }
    }

    @Test void schemaUpgradeDuringInitialSetupDoesNotRequireACacheIncarnationReplacement() {
        try (var f = new Fixture()) {
            var commands = f.configuration.recoveryCommands();
            var invocation = f.configuration.startInitialOpRecovery(commands.confirm(f.plan(null)), () -> {});
            f.configuration.prepareInitialOpRecovery(f.cache, invocation, () -> {});
            try (var upgrade = MongoConfigurationDomain.prepareValidationSchema(f.settings, f.setup.clusterId(), UUID.randomUUID())) {
                upgrade.prepareCache(f.cache); assertThat(upgrade.advance(20).ready()).isTrue();
            }
            assertThatCode(() -> commands.complete(invocation.plan())).doesNotThrowAnyException();
            assertThat(f.configuration.bindings().cachePreparation().orElseThrow().authScope()).isEqualTo(f.binding.preparation().authScope());
        }
    }
}
