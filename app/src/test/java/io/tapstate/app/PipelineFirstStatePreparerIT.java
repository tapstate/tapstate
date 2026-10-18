package io.tapstate.app;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationBootstrap;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.FirstPipelineCreationResult;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelinePreparation;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Concrete app orchestration over isolated CONFIG/CACHE replica sets; business root setup is a fixture. */
@RequiresDocker
class PipelineFirstStatePreparerIT {
    @Container private static final MongoDBContainer CONFIG = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @Container private static final MongoDBContainer CACHE = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final Runnable ACTIVE = () -> {};
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-18T08:00:00Z"), ZoneOffset.UTC);
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");

    @Test void theAppPreparerConsumesConcretePrivateGrantAndReceiptAcrossTwoInstances() {
        withFixture(f -> {
            long revision = f.version();
            var complete = f.preparer(f.configuration, f.cache).prepare(f.scope);
            assertThat(complete.phase()).isEqualTo(PipelinePreparation.Phase.COMPLETE);
            assertThat(f.configuration.intents().loadForActuation("flow")).isPresent();
            assertThat(f.cache.pipelineStates(f.scope).read("flow").orElseThrow().stateJson()).isEqualTo("NEW");
            assertThat(f.state()).containsEntry("epoch", 0L).containsEntry("runtimeWriteFence", 1L);
            assertThat(f.claim()).containsEntry("claimGeneration", 1L).containsEntry("executionGeneration", 0L);
            assertThat(f.version()).isEqualTo(revision);
            assertThat(f.configDatabase.listCollectionNames().into(new ArrayList<>())).doesNotContain("pipeline_state", "workload_claims");
            assertThat(f.cacheDatabase.listCollectionNames().into(new ArrayList<>())).doesNotContain("operator_generations", "pipeline_intents");
            // COMPLETE is historical preparation status, never proof that lost CACHE state is healthy.
            f.cacheDatabase.getCollection("pipeline_state").deleteMany(new Document());
            f.cacheDatabase.getCollection("workload_claims").deleteOne(f.pipelineFilter());
            var missing = f.snapshot();
            assertThat(f.preparer(f.configuration, f.cache).prepare(f.scope)).isEqualTo(complete);
            assertCode(() -> f.cache.workloadClaims(f.business()).acquirePipeline(f.scope, OWNER, 1, TTL), IoError.DOCUMENT_UNREADABLE);
            assertThat(f.snapshot()).isEqualTo(missing);
        });
    }

    @Test void reopeningClientsCannotReissueAnAuthorityWhoseCacheHistoryIsAbsent() {
        withFixture(f -> {
            var issued = f.configuration.preparations().issueFirstCreate(f.scope, f.node);
            assertThat(issued).isInstanceOf(FirstPipelineCreationResult.Issued.class);
            var before = f.snapshot(); var mapping = f.configDatabase.getCollection("operator_generations").find().first();
            try (var reopened = MongoConfigurationDomain.open(f.settings, "cluster-a");
                 var cache = MongoCacheDomain.connect(f.cacheConnection, Duration.ofSeconds(5), "reopened-test")) {
                assertCode(() -> f.preparer(reopened, cache).prepare(f.scope), IoError.PIPELINE_STATE_REBUILD_REQUIRED);
                assertThat(reopened.preparations().issueFirstCreate(f.scope, f.node)).isInstanceOf(FirstPipelineCreationResult.AlreadyIssued.class);
            }
            assertThat(f.snapshot()).isEqualTo(before);
            assertThat(f.configDatabase.getCollection("operator_generations").find().first()).isEqualTo(mapping);
        });
    }

    @Test void reopeningAfterSeedCompletesFromTheConcreteReceiptWithoutAnyCacheMutation() {
        withFixture(f -> {
            var grant = ((FirstPipelineCreationResult.Issued) f.configuration.preparations().issueFirstCreate(f.scope, f.node)).creation();
            var preparer = f.cache.pipelinePreparations(f.business(), ACTIVE);
            preparer.seed(preparer.reserveFirst(grant, TTL), f.configuration.preparations(), CLOCK.instant());
            var before = f.snapshot();
            try (var reopened = MongoConfigurationDomain.open(f.settings, "cluster-a");
                 var cache = MongoCacheDomain.connect(f.cacheConnection, Duration.ofSeconds(5), "reopened-test")) {
                assertThat(f.preparer(reopened, cache).prepare(f.scope).phase()).isEqualTo(PipelinePreparation.Phase.COMPLETE);
                assertThat(reopened.intents().loadForActuation("flow")).isPresent();
            }
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, IoError code) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(code));
    }
    private static void withFixture(Scenario scenario) {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String configName = "app_first_cfg_" + suffix; String cacheName = "app_first_cache_" + suffix;
        try (var configClient = MongoClients.create(CONFIG.getReplicaSetUrl()); var cacheClient = MongoClients.create(CACHE.getReplicaSetUrl())) {
            var configDb = configClient.getDatabase(configName); var cacheDb = cacheClient.getDatabase(cacheName);
            var settings = new MongoConnectionSettings(CONFIG.getReplicaSetUrl(configName), null, Duration.ofSeconds(5));
            var cacheConnection = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, CACHE.getReplicaSetUrl(cacheName), cacheName, null);
            try (var configuration = MongoConfigurationBootstrap.prepare(settings, "cluster-a", "installation-a", "setup-a", 1);
                 var cache = MongoCacheDomain.connect(cacheConnection, Duration.ofSeconds(5), "app-preparation-test")) {
                var master = new SourceResource(StorageBinding.defaultSourceId(StorageBinding.Mode.OP), null, "mongodb", Map.of("uri", "mongodb://master.invalid/master"), null, null, null, null);
                configuration.bindings().commit(new StorageSetupScope("cluster-a", "installation-a", "setup-a", 1), StorageBinding.Mode.OP, cacheConnection, master);
                var business = configuration.bindings().cachePreparation().orElseThrow().businessScope();
                cache.prepareAuth(new CacheAuthScope(business.clusterId(), business.bindingVersion(), StorageBinding.Mode.OP, business.cacheIncarnation(), 1), "auth-a", ACTIVE);
                // Explicit business-schema fixture, not production business-root preparation evidence.
                for (var table : List.of(SystemCollections.CACHE_WORKLOAD_CLAIMS, SystemCollections.CACHE_PIPELINE_STATE, SystemCollections.CACHE_PIPELINE_OBSERVATION, SystemCollections.CACHE_OPERATOR_CLEANUP)) cacheDb.createCollection(table.collectionName());
                SystemCollections.CACHE_IDENTITY.on(cacheDb).updateOne(new Document("_id", "cluster"), new Document("$set", new Document("businessPhase", "READY")
                        .append("businessPreparationId", "business-a").append("claimAdmissionFence", 0L)));
                SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(cacheDb).insertOne(new Document("_id", "business").append("installedVersion", 1).append("phase", "READY")
                        .append("epoch", 1L).append("operationId", "business-a"));
                var pipeline = new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: flow\nsource: []\n");
                configuration.artifacts().create(pipeline);
                var scope = new PipelineIntentScope(business.clusterId(), business.bindingVersion(), business.cacheIncarnation(), "flow", 1);
                configuration.intents().compareAndSet(scope, 0, new DesiredState("flow", PipelineState.RUNNING, CanonicalHash.of(pipeline)));
                var node = cache.workloadClaims(business).acquire(new WorkloadClaimKey("cluster-a", WorkloadClaimType.NODE_SESSION, "node-a"), OWNER, 1, TTL).claim();
                scenario.run(new Fixture(configuration, cache, configDb, cacheDb, settings, cacheConnection, scope, WorkloadClaimFence.from(node)));
            } finally { configDb.drop(); cacheDb.drop(); }
        } catch (RuntimeException | Error failure) { throw failure; }
        catch (Exception failure) { throw new AssertionError(failure); }
    }
    private record Fixture(MongoConfigurationDomain configuration, MongoCacheDomain cache, MongoDatabase configDatabase,
            MongoDatabase cacheDatabase, MongoConnectionSettings settings, StorageBinding.Connection cacheConnection,
            PipelineIntentScope scope, WorkloadClaimFence node) {
        CacheBusinessScope business() { return new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation()); }
        PipelineFirstStatePreparer preparer(MongoConfigurationDomain configuration, MongoCacheDomain cache) {
            return new PipelineFirstStatePreparer(business(), configuration.preparations(), cache.pipelinePreparations(business(), ACTIVE),
                    () -> Optional.of(node), ACTIVE, CLOCK, TTL);
        }
        long version() { return configDatabase.getCollection("configuration_version").find().first().getLong("revision"); }
        Document pipelineFilter() { return new Document("_id", new Document("clusterId", scope.clusterId()).append("resourceType", "PIPELINE_ACTUATION").append("resourceId", scope.pipelineId())); }
        Document claim() { return cacheDatabase.getCollection("workload_claims").find(pipelineFilter()).first(); }
        Document state() { return cacheDatabase.getCollection("pipeline_state").find().first(); }
        Map<String, List<Document>> snapshot() {
            var out = new java.util.TreeMap<String, List<Document>>();
            for (String table : List.of("workload_claims", "pipeline_state", "cache_identity")) out.put(table, cacheDatabase.getCollection(table).find().sort(new Document("_id", 1)).into(new ArrayList<>()));
            return out;
        }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
