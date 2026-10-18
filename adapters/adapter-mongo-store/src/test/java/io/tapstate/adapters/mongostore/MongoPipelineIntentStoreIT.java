package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClients;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Latest-intent CAS and mapping/version atomicity, without opening a cache client. */
@RequiresDocker
class MongoPipelineIntentStoreIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
    private static final String HASH = CanonicalHash.of(pipeline());

    @Test
    void latestIntentAndCoordinatesSurviveReopenWithoutAnyCacheResource() {
        withConfiguration((domain, database, scope) -> {
            var store = domain.intents();
            var desired = intent(PipelineState.RUNNING, true, true, 7L);
            long before = revision(database);
            assertThat(store.compareAndSet(scope, 0, desired)).isTrue();
            assertThat(store.load("flow")).contains(new PipelineIntentSnapshot(scope, 1, desired));
            assertThat(revision(database)).isEqualTo(before + 1);
            try (var reopened = MongoConfigurationDomain.open(settings(database), "cluster-a")) {
                assertThat(reopened.intents().load("flow")).isEqualTo(store.load("flow"));
            }
            assertThat(database.listCollectionNames().into(new ArrayList<>()))
                    .contains("pipeline_intents").doesNotContain("pipeline_desired", "pipeline_state", "cache_identity", "workload_claims", "srs_meta");
        });
    }

    @Test
    void stopThenStartCoalescesIntoOneLatestValueWithTheOriginalOneTimeStamp() {
        withConfiguration((domain, database, scope) -> {
            var store = domain.intents();
            assertThat(store.compareAndSet(scope, 0, intent(PipelineState.STOPPED, true, false, null))).isTrue();
            assertThat(store.compareAndSet(scope, 1, intent(PipelineState.RUNNING, true, true, 12L))).isTrue();
            assertThat(store.load("flow")).get().satisfies(snapshot -> {
                assertThat(snapshot.intentRevision()).isEqualTo(2);
                assertThat(snapshot.scope()).isEqualTo(scope);
                assertThat(snapshot.desired().revision()).isEqualTo(HASH);
                assertThat(snapshot.desired().purgeState()).isTrue();
                assertThat(snapshot.desired().reassemble()).isTrue();
                assertThat(snapshot.desired().rebuiltAtStateEpoch()).isEqualTo(12L);
            });
            assertThat(SystemCollections.PIPELINE_INTENTS.on(database).countDocuments()).isOne();
        });
    }

    @Test
    void staleRevisionOrArtifactHashCannotOverwriteTheLatestIntentOrAdvanceVersions() {
        withConfiguration((domain, database, scope) -> {
            var store = domain.intents();
            var desired = intent(PipelineState.RUNNING, false, false, null);
            assertThat(store.compareAndSet(scope, 0, desired)).isTrue();
            long before = revision(database); Document mapping = mapping(database);
            assertThat(store.compareAndSet(scope, 0, intent(PipelineState.STOPPED, true, false, null))).isFalse();
            var staleHash = new DesiredState("flow", PipelineState.STOPPED, "old-artifact-hash", true);
            assertThat(store.compareAndSet(scope, 1, staleHash)).isFalse();
            assertThat(store.load("flow")).contains(new PipelineIntentSnapshot(scope, 1, desired));
            assertThat(revision(database)).isEqualTo(before);
            assertThat(mapping(database)).isEqualTo(mapping);
        });
    }

    @Test
    void twoIndependentClientsCanPublishOnlyOneSuccessorOfAnObservedRevision() {
        withConfiguration((domain, database, scope) -> {
            assertThat(domain.intents().compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null))).isTrue();
            long before = revision(database);
            try (var other = MongoConfigurationDomain.open(settings(database), "cluster-a"); var executor = Executors.newFixedThreadPool(2)) {
                var ready = new CountDownLatch(2); var start = new CountDownLatch(1);
                var a = executor.submit(() -> { ready.countDown(); assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
                    return domain.intents().compareAndSet(scope, 1, intent(PipelineState.STOPPED, true, false, null)); });
                var b = executor.submit(() -> { ready.countDown(); assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
                    return other.intents().compareAndSet(scope, 1, intent(PipelineState.PAUSED, false, false, null)); });
                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue(); start.countDown();
                assertThat(List.of(a.get(20, TimeUnit.SECONDS), b.get(20, TimeUnit.SECONDS))).containsExactlyInAnyOrder(true, false);
            }
            assertThat(domain.intents().load("flow")).get().extracting(PipelineIntentSnapshot::intentRevision).isEqualTo(2L);
            assertThat(revision(database)).isEqualTo(before + 1);
            assertThat(SystemCollections.PIPELINE_INTENTS.on(database).countDocuments()).isOne();
        });
    }

    @Test
    void removingAnAbsentIntentAlsoPreventsADelayedFirstWriterFromRevivingIt() {
        withConfiguration((domain, database, scope) -> {
            assertThat(domain.intents().remove(scope, 0)).isTrue();
            assertThat(domain.intents().compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null)))
                    .as("an earlier never-written observation cannot revive a removed intent").isFalse();
            assertThat(domain.intents().load("flow")).contains(new PipelineIntentSnapshot(scope, 1, null));
            assertThat(domain.intents().pipelineIds()).isEmpty();
        });
    }

    @Test
    void deletionRetainsASmallRevisionMarkerAndRejectsEarlierWritersAfterRecreation() {
        withConfiguration((domain, database, scope) -> {
            var store = domain.intents();
            assertThat(store.compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null))).isTrue();
            assertThat(store.remove(scope, 1)).isTrue();
            var deleted = store.load("flow").orElseThrow();
            assertThat(deleted.deleted()).isTrue(); assertThat(deleted.intentRevision()).isEqualTo(2);
            assertThat(store.pipelineIds()).isEmpty();
            assertThat(SystemCollections.PIPELINE_INTENTS.on(database).find().first())
                    .doesNotContainKeys("targetState", "revision", "assemblyRevision", "purgeState", "reassemble", "rebuiltAtStateEpoch");
            long before = revision(database);
            assertThat(store.remove(scope, 2)).isTrue(); assertThat(revision(database)).isEqualTo(before);
            assertThat(store.compareAndSet(scope, 0, intent(PipelineState.STOPPED, true, false, null))).isFalse();
            assertThat(store.compareAndSet(scope, 2, intent(PipelineState.RUNNING, false, false, null))).isTrue();
            assertThat(store.compareAndSet(scope, 1, intent(PipelineState.STOPPED, true, false, null))).isFalse();
            assertThat(store.load("flow")).get().extracting(PipelineIntentSnapshot::intentRevision).isEqualTo(3L);
        });
    }

    @Test
    void callerCannotChooseADifferentIncarnationForTheCurrentBinding() {
        withConfiguration((domain, database, scope) -> {
            var wrong = new PipelineIntentScope(scope.clusterId(), scope.bindingVersion(), UUID.randomUUID().toString(),
                    scope.pipelineId(), scope.stateGeneration());
            var originalMapping = mapping(database); long before = revision(database);
            assertThatThrownBy(() -> domain.intents().compareAndSet(wrong, 0, intent(PipelineState.RUNNING, false, false, null)))
                    .satisfies(MongoPipelineIntentStoreIT::fenced);
            assertThat(SystemCollections.PIPELINE_INTENTS.on(database).countDocuments()).isZero();
            assertThat(mapping(database)).isEqualTo(originalMapping);
            assertThat(revision(database)).isEqualTo(before);
        });
    }

    @Test
    void aStoredDifferentIncarnationCannotBeReadRetargetedOrRemovedByAnOrdinaryCommand() {
        withConfiguration((domain, database, scope) -> {
            var store = domain.intents();
            assertThat(store.compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null))).isTrue();
            SystemCollections.PIPELINE_INTENTS.on(database).updateOne(new Document("_id", "flow"),
                    new Document("$set", new Document("cacheIncarnation", UUID.randomUUID().toString())));
            var stored = SystemCollections.PIPELINE_INTENTS.on(database).find().first();
            var originalMapping = mapping(database); long before = revision(database);
            assertThat(store.pipelineIds()).containsExactly("flow");
            assertThatThrownBy(() -> store.load("flow")).satisfies(MongoPipelineIntentStoreIT::unreadable);
            assertThatThrownBy(() -> store.compareAndSet(scope, 1, intent(PipelineState.STOPPED, true, false, null)))
                    .satisfies(MongoPipelineIntentStoreIT::unreadable);
            assertThatThrownBy(() -> store.remove(scope, 1)).satisfies(MongoPipelineIntentStoreIT::unreadable);
            assertThat(SystemCollections.PIPELINE_INTENTS.on(database).find().first()).isEqualTo(stored);
            assertThat(mapping(database)).isEqualTo(originalMapping); assertThat(revision(database)).isEqualTo(before);
        });
    }

    @Test
    void anOrdinaryCommandCannotAdoptAnotherStoredGenerationAtTheSameRevision() {
        withConfiguration((domain, database, scope) -> {
            var store = domain.intents();
            assertThat(store.compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null))).isTrue();
            SystemCollections.OPERATOR_GENERATIONS.on(database).updateOne(new Document("_id", "flow"),
                    new Document("$set", new Document("stateGeneration", 2L)));
            var next = new PipelineIntentScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation(), "flow", 2);
            var before = SystemCollections.PIPELINE_INTENTS.on(database).find().first();
            long version = revision(database); var originalMapping = mapping(database);
            assertThatThrownBy(() -> store.compareAndSet(next, 1, intent(PipelineState.STOPPED, true, false, null)))
                    .satisfies(MongoPipelineIntentStoreIT::fenced);
            assertThatThrownBy(() -> store.remove(next, 1)).satisfies(MongoPipelineIntentStoreIT::fenced);
            assertThat(SystemCollections.PIPELINE_INTENTS.on(database).find().first()).isEqualTo(before);
            assertThat(mapping(database)).isEqualTo(originalMapping); assertThat(revision(database)).isEqualTo(version);
        });
    }

    @Test
    void missingRecoveryAuthorityCannotBeReconstructedFromAnExistingIntentOrCallerScope() {
        withConfiguration((domain, database, scope) -> {
            var store = domain.intents();
            assertThat(store.compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null))).isTrue();
            SystemCollections.STORAGE_RECOVERY.on(database).deleteMany(new Document());
            var before = SystemCollections.PIPELINE_INTENTS.on(database).find().first();
            var originalMapping = mapping(database); long version = revision(database);
            assertThatThrownBy(() -> store.load("flow")).satisfies(MongoPipelineIntentStoreIT::unreadable);
            assertThatThrownBy(() -> store.compareAndSet(scope, 1, intent(PipelineState.STOPPED, true, false, null)))
                    .satisfies(MongoPipelineIntentStoreIT::unreadable);
            assertThatThrownBy(() -> store.remove(scope, 1)).satisfies(MongoPipelineIntentStoreIT::unreadable);
            assertThat(SystemCollections.STORAGE_RECOVERY.on(database).countDocuments()).isZero();
            assertThat(SystemCollections.PIPELINE_INTENTS.on(database).find().first()).isEqualTo(before);
            assertThat(mapping(database)).isEqualTo(originalMapping); assertThat(revision(database)).isEqualTo(version);
        });
    }

    @Test
    void staleBindingGenerationAndNonActiveMappingAreNeverSilentlyRetargeted() {
        withConfiguration((domain, database, scope) -> {
            var store = domain.intents();
            for (var wrong : List.of(new PipelineIntentScope("cluster-b", 1, scope.cacheIncarnation(), "flow", 1),
                    new PipelineIntentScope("cluster-a", 2, scope.cacheIncarnation(), "flow", 1),
                    new PipelineIntentScope("cluster-a", 1, scope.cacheIncarnation(), "flow", 2))) {
                assertThatThrownBy(() -> store.compareAndSet(wrong, 0, intent(PipelineState.RUNNING, false, false, null)))
                        .satisfies(MongoPipelineIntentStoreIT::fenced);
            }
            SystemCollections.OPERATOR_GENERATIONS.on(database).updateOne(new Document("_id", "flow"),
                    new Document("$set", new Document("phase", "REBUILDING").append("operationId", "recovery-a")));
            assertThatThrownBy(() -> store.compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null)))
                    .satisfies(MongoPipelineIntentStoreIT::fenced);
            assertThat(store.load("flow")).isEmpty();
        });
    }

    @Test
    void postWriteLossOfOriginalAuthorityRollsBackIntentMappingAndGlobalVersion() {
        withConfiguration((domain, database, scope) -> {
            var checks = new AtomicInteger(); long before = revision(database); Document mapping = mapping(database);
            var guarded = domain.intents().guardedBy(() -> {
                if (checks.incrementAndGet() == 2) throw new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null);
            });
            assertThatThrownBy(() -> guarded.compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null)))
                    .satisfies(MongoPipelineIntentStoreIT::fenced);
            assertThat(domain.intents().load("flow")).isEmpty();
            assertThat(revision(database)).isEqualTo(before); assertThat(mapping(database)).isEqualTo(mapping);
        });
    }

    @Test
    void aRealTransactionRetryRechecksTheOriginalGuardInsteadOfCreatingANewContext() {
        for (boolean injectFailure : List.of(false, true)) {
            withConfiguration((domain, database, scope) -> {
                var failedUpdates = new AtomicInteger(); var startedUpdates = new AtomicInteger(); var checks = new AtomicInteger();
                var witness = new CommandListener() {
                    @Override public void commandStarted(CommandStartedEvent event) {
                        if (event.getCommandName().equals("update")) startedUpdates.incrementAndGet();
                    }
                    @Override public void commandFailed(CommandFailedEvent event) {
                        if (event.getCommandName().equals("update")) failedUpdates.incrementAndGet();
                    }
                };
                try (var admin = MongoClients.create(MONGO.getReplicaSetUrl());
                        var observed = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                                .addCommandListener(witness).build())) {
                    admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                            .append("mode", injectFailure ? new Document("times", 1) : "off")
                            .append("data", new Document("failCommands", List.of("update")).append("errorCode", 112)
                                    .append("errorLabels", List.of("TransientTransactionError"))));
                    try {
                        long before = revision(database);
                        String material = SystemCollections.CLUSTER_KEYS.on(database).find().first().getString("material");
                        var ciphers = SourceConfigCipherProvider.fixed(new SourceConfigCipher(java.util.Base64.getUrlDecoder().decode(material)));
                        var observedDatabase = observed.getDatabase(database.getName());
                        var originalBindings = new MongoStorageBindingStore(observed, observedDatabase, ciphers);
                        var guarded = new MongoPipelineIntentStore(observed, observedDatabase, "cluster-a", originalBindings).guardedBy(() -> {
                            checks.incrementAndGet();
                            if (failedUpdates.get() > 0) throw new TapstateException(IoError.PIPELINE_INTENT_FENCED, Map.of(), null);
                        });
                        if (injectFailure) {
                            assertThatThrownBy(() -> guarded.compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null)))
                                    .satisfies(MongoPipelineIntentStoreIT::fenced);
                            assertThat(failedUpdates.get()).isOne(); assertThat(startedUpdates.get()).isOne();
                            assertThat(checks.get()).isEqualTo(2); assertThat(revision(database)).isEqualTo(before);
                            assertThat(domain.intents().load("flow")).isEmpty();
                        } else {
                            assertThat(guarded.compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null))).isTrue();
                            assertThat(failedUpdates.get()).isZero(); assertThat(startedUpdates.get()).isEqualTo(2);
                            assertThat(checks.get()).isEqualTo(2); assertThat(revision(database)).isEqualTo(before + 1);
                            assertThat(domain.intents().load("flow")).isPresent();
                        }
                    } finally {
                        admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
                    }
                }
            });
        }
    }

    @Test
    void corruptIntentBodiesRemainInTheIdProjectionButAreNeverDefaultedOrOverwritten() {
        for (String field : List.of("reassemble", "rebuiltAtStateEpoch", "cacheIncarnation", "intentRevision")) {
            withConfiguration((domain, database, scope) -> {
                assertThat(domain.intents().compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null))).isTrue();
                SystemCollections.PIPELINE_INTENTS.on(database).updateOne(new Document(), new Document("$set", new Document(field, field.equals("cacheIncarnation") ? 7L : "raw-secret-sentinel")));
                long before = revision(database);
                assertThat(domain.intents().pipelineIds()).containsExactly("flow");
                assertThatThrownBy(() -> domain.intents().load("flow")).satisfies(MongoPipelineIntentStoreIT::unreadable);
                assertThatThrownBy(() -> domain.intents().compareAndSet(scope, 1, intent(PipelineState.STOPPED, true, false, null)))
                        .satisfies(MongoPipelineIntentStoreIT::unreadable);
                assertThat(revision(database)).isEqualTo(before);
            });
        }
    }

    @Test
    void missingMappingOrConfigurationVersionRefusesWithoutCreatingReplacementFacts() {
        for (var missing : List.of(SystemCollections.OPERATOR_GENERATIONS, SystemCollections.CONFIGURATION_VERSION)) {
            withConfiguration((domain, database, scope) -> {
                missing.on(database).deleteMany(new Document());
                assertThatThrownBy(() -> domain.intents().compareAndSet(scope, 0, intent(PipelineState.RUNNING, false, false, null)))
                        .satisfies(MongoPipelineIntentStoreIT::unreadable);
                assertThat(domain.intents().load("flow")).isEmpty(); assertThat(missing.on(database).countDocuments()).isZero();
            });
        }
    }

    private static DesiredState intent(PipelineState target, boolean purge, boolean reassemble, Long epoch) {
        return new DesiredState("flow", target, HASH, purge, "assembly-a", reassemble, epoch);
    }
    private static io.tapstate.core.model.Resource pipeline() { return new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: flow\nsource: []\n"); }
    private static long revision(MongoDatabase database) { return SystemCollections.CONFIGURATION_VERSION.on(database).find().first().getLong("revision"); }
    private static Document mapping(MongoDatabase database) { return SystemCollections.OPERATOR_GENERATIONS.on(database).find(new Document("_id", "flow")).first(); }
    private static void fenced(Throwable failure) { assertThat(failure).isInstanceOfSatisfying(TapstateException.class,
            error -> assertThat(error.code()).isEqualTo(IoError.PIPELINE_INTENT_FENCED)); }
    private static void unreadable(Throwable failure) { assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> {
        assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE); assertThat(error.getCause()).isNull();
        assertThat(error.getMessage()).doesNotContain("raw-secret-sentinel");
    }); }
    private static MongoConnectionSettings settings(MongoDatabase database) { return new MongoConnectionSettings(MONGO.getReplicaSetUrl(database.getName()), null, Duration.ofSeconds(5)); }
    private static void withConfiguration(Scenario scenario) {
        try (var admin = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = admin.getDatabase("config_intent_" + UUID.randomUUID().toString().replace("-", ""));
            try (var domain = MongoConfigurationBootstrap.prepare(settings(database), "cluster-a", "installation-a", "setup-a", 1)) {
                var cache = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, "mongodb://cache.invalid/cache", "cache", null);
                var master = new SourceResource(StorageBinding.defaultSourceId(StorageBinding.Mode.OP), null, "mongodb",
                        Map.of("uri", "mongodb://master.invalid/master"), null, null, null, null);
                domain.bindings().commit(new StorageSetupScope("cluster-a", "installation-a", "setup-a", 1), StorageBinding.Mode.OP, cache, master);
                domain.artifacts().create(pipeline());
                var cacheScope = domain.bindings().cachePreparation().orElseThrow().businessScope();
                var scope = new PipelineIntentScope(cacheScope.clusterId(), cacheScope.bindingVersion(), cacheScope.cacheIncarnation(), "flow", 1);
                scenario.run(domain, database, scope);
            } catch (Exception failure) { throw new AssertionError(failure); }
            finally { database.drop(); }
        }
    }
    @FunctionalInterface private interface Scenario { void run(MongoConfigurationDomain domain, MongoDatabase database, PipelineIntentScope scope) throws Exception; }
}
