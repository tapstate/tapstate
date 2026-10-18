package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.FirstPipelineCreation;
import io.tapstate.spi.store.FirstPipelineCreationResult;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineCachePreparations;
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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
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

/** Independent CONFIG and CACHE replica sets exercise the original first-creation authority. */
@RequiresDocker
class MongoCachePipelinePreparationsIT {
    @Container private static final MongoDBContainer CONFIG = mongo();
    @Container private static final MongoDBContainer CACHE = mongo();
    private static final Runnable ACTIVE = () -> {};
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final Instant AT = Instant.parse("2026-10-18T02:00:00Z");
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");
    private static MongoDBContainer mongo() { return new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1"); }

    @Test void onlyCommittedCachePreparationCanCompleteConfigAndAllowActuation() {
        withFixture(f -> {
            long revision = f.version();
            FirstPipelineCreation grant = f.issue();
            assertThat(f.configuration.intents().loadForActuation("flow")).isEmpty();
            var reservation = f.preparations().reserveFirst(grant, TTL);
            assertThat(reservation.claim().claimGeneration()).isEqualTo(1);
            assertThat(reservation.claim().executionGeneration()).isZero();
            assertThat(f.state()).isNull();
            assertThat(f.claim()).containsEntry("pipelinePreparation", marker(grant.preparation(), "RESERVED"));
            var receipt = f.preparations().seed(reservation, f.configuration.preparations(), AT);
            assertThat(f.state()).containsEntry("epoch", 0L).containsEntry("runtimeWriteFence", 1L).containsEntry("stateJson", "NEW");
            assertThat(f.claim()).containsEntry("pipelinePreparation", marker(grant.preparation(), "PREPARED"));
            assertThat(f.configuration.intents().loadForActuation("flow")).isEmpty();
            var complete = f.configuration.preparations().complete(receipt);
            assertThat(complete.phase()).isEqualTo(PipelinePreparation.Phase.COMPLETE);
            assertThat(f.configuration.intents().loadForActuation("flow")).isEqualTo(f.configuration.intents().load("flow"));
            assertThat(f.configuration.preparations().complete(receipt)).isEqualTo(complete);
            assertThat(f.version()).isEqualTo(revision);
            assertThat(f.configDatabase.listCollectionNames().into(new ArrayList<>())).doesNotContain("pipeline_state", "workload_claims");
            assertThat(f.cacheDatabase.listCollectionNames().into(new ArrayList<>())).doesNotContain("operator_generations", "pipeline_intents");
        });
    }

    @Test void invocationIsConsumedBeforeTheCacheGuardAndCannotBeRepeatedAfterReentry() {
        withFixture(f -> {
            var grant = f.issue();
            var calls = new AtomicInteger();
            var guarded = new MongoCachePipelinePreparations(f.cacheClient, f.cacheDatabase, f.business(), () -> {
                calls.incrementAndGet();
                assertCode(() -> f.preparations().reserveFirst(grant, TTL), IoError.PIPELINE_INTENT_FENCED);
            });
            guarded.reserveFirst(grant, TTL);
            assertThat(calls.get()).isGreaterThan(0);
            var before = f.snapshot();
            assertCode(() -> f.preparations().reserveFirst(grant, TTL), IoError.PIPELINE_INTENT_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void arbitraryGrantImplementationsCannotTurnStatusIntoInsertAuthority() {
        withFixture(f -> {
            var issued = f.issue().preparation(); var callbacks = new AtomicInteger();
            FirstPipelineCreation forged = new FirstPipelineCreation() {
                @Override public PipelinePreparation preparation() { callbacks.incrementAndGet(); return issued; }
                @Override public void consume() { callbacks.incrementAndGet(); }
                @Override public void requireActive() { callbacks.incrementAndGet(); }
            };
            var before = f.snapshot();
            assertCode(() -> f.preparations().reserveFirst(forged, TTL), IoError.PIPELINE_INTENT_FENCED);
            assertThat(callbacks).hasValue(0); assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void issuedAuthorityWithoutAnyCacheFactRequiresExplicitRebuildAfterReopen() {
        withFixture(f -> {
            var original = f.issue().preparation();
            try (var reopened = MongoConfigurationDomain.open(f.settings(), "cluster-a")) {
                assertThat(reopened.preparations().issueFirstCreate(f.scope, f.node)).isInstanceOf(FirstPipelineCreationResult.AlreadyIssued.class);
                var before = f.snapshot();
                assertCode(() -> f.preparations().resume(original, f.node, TTL), IoError.PIPELINE_STATE_REBUILD_REQUIRED);
                assertThat(f.snapshot()).isEqualTo(before);
            }
        });
    }

    @Test void retainedReservationResumesAndExpiredSameBootGetsTheNextTenure() {
        withFixture(f -> {
            var grant = f.issue(); var first = f.preparations().reserveFirst(grant, TTL);
            var live = f.preparations().resume(grant.preparation(), f.node, TTL);
            assertThat(live.claim()).isEqualTo(first.claim());
            f.claims().updateOne(pipelineFilter(f.scope), new Document("$set", new Document("leaseUntil", new Date(0))));
            var resumed = f.preparations().resume(grant.preparation(), f.node, TTL);
            assertThat(resumed.claim().claimGeneration()).isEqualTo(2);
            assertThat(resumed.claim().executionGeneration()).isZero();
            assertCode(() -> f.preparations().seed(first, f.configuration.preparations(), AT), IoError.WORKLOAD_CLAIM_FENCED);
            f.configuration.preparations().complete(f.preparations().seed(resumed, f.configuration.preparations(), AT));
        });
    }

    @Test void expiredReservationCannotCommitANextTenureThatIsAlreadyAtTheForbiddenBoundary() {
        withFixture(f -> {
            var grant = f.issue(); f.preparations().reserveFirst(grant, TTL);
            f.claims().updateOne(pipelineFilter(f.scope), new Document("$set", new Document("claimGeneration", Long.MAX_VALUE - 2)
                    .append("leaseUntil", new Date(0))));
            var before = f.snapshot();
            assertCode(() -> f.preparations().resume(grant.preparation(), f.node, TTL), IoError.PIPELINE_STATE_REBUILD_REQUIRED);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void anotherLiveOwnerCannotTakeReservationButMayTakeItsExpiredNextTenure() {
        withFixture(f -> {
            var grant = f.issue(); var first = f.preparations().reserveFirst(grant, TTL);
            var other = f.node(new WorkloadOwner("node-b", "boot-b"));
            var before = f.snapshot();
            assertCode(() -> f.preparations().resume(grant.preparation(), other, TTL), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
            f.claims().updateOne(pipelineFilter(f.scope), new Document("$set", new Document("leaseUntil", new Date(0))));
            var resumed = f.preparations().resume(grant.preparation(), other, TTL);
            assertThat(resumed.claim().owner()).isEqualTo(other.owner());
            assertThat(resumed.claim().claimGeneration()).isEqualTo(first.claim().claimGeneration() + 1);
        });
    }

    @Test void preparedQueryIsReadOnlyAndDoesNotRenewOrTakeOverEvenWhenExpired() {
        withFixture(f -> {
            var grant = f.issue(); var original = f.preparations().reserveFirst(grant, TTL);
            f.preparations().seed(original, f.configuration.preparations(), AT);
            f.claims().updateOne(pipelineFilter(f.scope), new Document("$set", new Document("leaseUntil", new Date(0))));
            var before = f.snapshot();
            var found = f.preparations().resume(grant.preparation(), f.node, TTL);
            assertThat(found.phase()).isEqualTo(PipelineCachePreparations.Phase.PREPARED);
            var receipt = f.preparations().seed(found, f.configuration.preparations(), AT.plusSeconds(1));
            assertThat(f.snapshot()).isEqualTo(before);
            f.configuration.preparations().complete(receipt);
        });
    }

    @Test void lossOfEitherOriginalFactOrBothNeverReseedsOrRestartsCounters() {
        for (int damage = 0; damage < 3; damage++) {
            int selected = damage;
            withFixture(f -> {
                var grant = f.issue(); var original = f.preparations().reserveFirst(grant, TTL);
                var receipt = f.preparations().seed(original, f.configuration.preparations(), AT);
                f.configuration.preparations().complete(receipt);
                if (selected != 1) f.claims().deleteOne(pipelineFilter(f.scope));
                if (selected != 0) SystemCollections.CACHE_PIPELINE_STATE.on(f.cacheDatabase).deleteMany(new Document());
                var before = f.snapshot();
                // Query the retained ISSUED identity, as a lost-response operation would do.
                assertCode(() -> f.preparations().resume(grant.preparation(), f.node, TTL), IoError.PIPELINE_STATE_REBUILD_REQUIRED);
                assertCode(() -> f.preparations().reserveFirst(grant, TTL), IoError.PIPELINE_INTENT_FENCED);
                assertThat(f.configuration.preparations().issueFirstCreate(f.scope, f.node)).isInstanceOf(FirstPipelineCreationResult.AlreadyIssued.class);
                assertThat(f.snapshot()).isEqualTo(before);
            });
        }
    }

    @Test void damagedNonceScopePhaseAndCheckpointCannotBeRepairedOrTurnedIntoReceipt() {
        withFixture(f -> {
            var grant = f.issue(); var original = f.preparations().reserveFirst(grant, TTL);
            f.preparations().seed(original, f.configuration.preparations(), AT);
            Document saved = f.claim();
            for (var mutation : List.of(new Document("pipelinePreparation.preparationId", UUID.randomUUID().toString()),
                    new Document("pipelinePreparation.phase", "secret-sentinel"), new Document("pipelinePreparation.extra", true),
                    new Document("pipelineScope.stateGeneration", 1))) {
                f.claims().updateOne(pipelineFilter(f.scope), new Document("$set", mutation));
                var before = f.snapshot();
                assertCode(() -> f.preparations().resume(grant.preparation(), f.node, TTL), IoError.PIPELINE_STATE_REBUILD_REQUIRED);
                assertThat(f.snapshot()).isEqualTo(before);
                f.claims().replaceOne(pipelineFilter(f.scope), saved);
            }
            var states = SystemCollections.CACHE_PIPELINE_STATE.on(f.cacheDatabase); Document state = f.state();
            for (var mutation : List.of(new Document("epoch", 0), new Document("runtimeWriteFence", 2L),
                    new Document("stateJson", "RUNNING"), new Document("scope.stateGeneration", 2L))) {
                states.updateOne(new Document(), new Document("$set", mutation));
                var before = f.snapshot();
                assertCode(() -> f.preparations().resume(grant.preparation(), f.node, TTL), IoError.PIPELINE_STATE_REBUILD_REQUIRED);
                assertThat(f.snapshot()).isEqualTo(before);
                states.replaceOne(new Document(), state);
            }
        });
    }

    @Test void freshConfigCheckRefusesSeedAfterMappingRetirement() {
        withFixture(f -> {
            var grant = f.issue(); var original = f.preparations().reserveFirst(grant, TTL);
            f.configDatabase.getCollection("operator_generations").updateOne(new Document("_id", "flow"),
                    new Document("$set", new Document("phase", "REBUILDING").append("operationId", "rebuild")));
            var before = f.snapshot();
            assertCode(() -> f.preparations().seed(original, f.configuration.preparations(), AT), IoError.PIPELINE_INTENT_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void nodeRevocationBeforeItsPreparationWritePreventsTheReservationCommit() {
        withFixture(f -> {
            var grant = f.issue(); var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1); var once = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) {
                    if (updates(e, "fencedPreparationWrites") && once.compareAndSet(false, true)) { paused.countDown(); await(proceed); }
                }
            };
            try (var observed = cacheClient(listener); var executor = Executors.newSingleThreadExecutor()) {
                var store = new MongoCachePipelinePreparations(observed, observed.getDatabase(f.cacheDatabase.getName()), f.business(), ACTIVE);
                var result = executor.submit(() -> store.reserveFirst(grant, TTL));
                try {
                    assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue();
                    f.claims().updateOne(new Document("_id", MongoWorkloadClaimStore.id(f.node.key())),
                            new Document("$set", new Document("claimGeneration", 2L).append("ownerBootId", "replacement")));
                } finally { proceed.countDown(); }
                assertFutureCode(() -> result.get(15, TimeUnit.SECONDS), IoError.WORKLOAD_CLAIM_FENCED);
                assertThat(f.claim()).isNull(); assertThat(f.state()).isNull();
                assertThat(SystemCollections.CACHE_IDENTITY.on(f.cacheDatabase).find().first().getLong("claimAdmissionFence")).isEqualTo(1L);
            }
        });
    }

    @Test void reservationCommittedBeforeNodeRevocationKeepsOnlyItsOriginalEvidence() {
        withFixture(f -> {
            var grant = f.issue(); var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1); var once = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) {
                    if (e.getCommandName().equals("insert") && e.getCommand().getString("insert").getValue().equals("workload_claims")
                            && once.compareAndSet(false, true)) { paused.countDown(); await(proceed); }
                }
            };
            try (var observed = cacheClient(listener); var executor = Executors.newFixedThreadPool(2)) {
                var store = new MongoCachePipelinePreparations(observed, observed.getDatabase(f.cacheDatabase.getName()), f.business(), ACTIVE);
                var result = executor.submit(() -> store.reserveFirst(grant, TTL));
                assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue();
                var revocation = executor.submit(() -> f.claims().updateOne(new Document("_id", MongoWorkloadClaimStore.id(f.node.key())),
                        new Document("$set", new Document("claimGeneration", 2L).append("ownerBootId", "replacement"))));
                proceed.countDown();
                var reservation = result.get(15, TimeUnit.SECONDS); revocation.get(15, TimeUnit.SECONDS);
                assertThat(reservation.claim().owner()).isEqualTo(OWNER);
                assertThat(f.claim()).isNotNull(); assertThat(f.state()).isNull();
                assertCode(() -> store.reserveFirst(grant, TTL), IoError.PIPELINE_INTENT_FENCED);
            } finally { proceed.countDown(); }
        });
    }

    @Test void aLateOriginalSeedCannotInsertAfterItsReservationHistoryIsLost() {
        withFixture(f -> {
            var grant = f.issue(); var original = f.preparations().reserveFirst(grant, TTL);
            var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1); var once = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) {
                    if (updates(e, "pipelinePreparation") && once.compareAndSet(false, true)) { paused.countDown(); await(proceed); }
                }
            };
            try (var observed = cacheClient(listener); var executor = Executors.newSingleThreadExecutor()) {
                var store = new MongoCachePipelinePreparations(observed, observed.getDatabase(f.cacheDatabase.getName()), f.business(), ACTIVE);
                var result = executor.submit(() -> store.seed(original, f.configuration.preparations(), AT));
                try {
                    assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue();
                    f.claims().deleteOne(pipelineFilter(f.scope));
                } finally { proceed.countDown(); }
                assertFutureCode(() -> result.get(15, TimeUnit.SECONDS), IoError.PIPELINE_STATE_REBUILD_REQUIRED);
                assertThat(f.claim()).isNull(); assertThat(f.state()).isNull();
                assertCode(() -> f.preparations().resume(grant.preparation(), f.node, TTL), IoError.PIPELINE_STATE_REBUILD_REQUIRED);
            }
        });
    }

    @Test void transientReservationRetryKeepsTheConsumedGrantAndFixedNonce() {
        withFixture(f -> {
            var grant = f.issue(); var failures = new AtomicInteger(); var nodeWrites = new AtomicInteger();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) { if (updates(e, "fencedPreparationWrites")) nodeWrites.incrementAndGet(); }
                @Override public void commandFailed(CommandFailedEvent e) { if (e.getCommandName().equals("update")) failures.incrementAndGet(); }
            };
            try (var observed = cacheClient(listener)) {
                failCommand(f.cacheClient, "update", new Document("errorCode", 112).append("errorLabels", List.of("TransientTransactionError")));
                try {
                    var store = new MongoCachePipelinePreparations(observed, observed.getDatabase(f.cacheDatabase.getName()), f.business(), ACTIVE);
                    store.reserveFirst(grant, TTL);
                } finally { clearFailpoint(f.cacheClient); }
                assertThat(failures).hasValue(1); assertThat(nodeWrites).hasValue(1);
                assertThat(f.claim()).containsEntry("claimGeneration", 1L).containsEntry("pipelinePreparation", marker(grant.preparation(), "RESERVED"));
                assertCode(() -> f.preparations().reserveFirst(grant, TTL), IoError.PIPELINE_INTENT_FENCED);
            }
        });
    }

    @Test void ambiguousCommitRetriesOnlyCommitAndNeverReentersFirstInsert() {
        withFixture(f -> {
            var grant = f.issue(); var inserts = new AtomicInteger(); var commits = new AtomicInteger();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) {
                    if (e.getCommandName().equals("insert")) inserts.incrementAndGet();
                    if (e.getCommandName().equals("commitTransaction")) commits.incrementAndGet();
                }
            };
            try (var observed = cacheClient(listener)) {
                failCommand(f.cacheClient, "commitTransaction", new Document("writeConcernError", new Document("code", 91).append("errmsg", "test commit reply lost"))
                        .append("errorLabels", List.of("UnknownTransactionCommitResult")));
                try {
                    var store = new MongoCachePipelinePreparations(observed, observed.getDatabase(f.cacheDatabase.getName()), f.business(), ACTIVE);
                    store.reserveFirst(grant, TTL);
                } finally { clearFailpoint(f.cacheClient); }
                assertThat(inserts).hasValue(1); assertThat(commits.get()).isGreaterThanOrEqualTo(2);
                assertThat(f.claim()).containsEntry("claimGeneration", 1L);
            }
        });
    }

    @Test void aClosedOriginalCacheDomainConsumesButNeverExecutesTheGrant() {
        withFixture(f -> {
            var grant = f.issue(); var captured = f.preparations();
            f.cacheDomain.close();
            var before = f.snapshot();
            assertCode(() -> captured.reserveFirst(grant, TTL), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> captured.reserveFirst(grant, TTL), IoError.PIPELINE_INTENT_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void originalGuardRevokedAfterStateInsertRollsBackStateAndPreparedMarker() {
        withFixture(f -> {
            var grant = f.issue(); var original = f.preparations().reserveFirst(grant, TTL);
            var active = new AtomicBoolean(true);
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) {
                    if (e.getCommandName().equals("insert") && e.getCommand().getString("insert").getValue().equals("pipeline_state")) active.set(false);
                }
            };
            var before = f.snapshot();
            try (var observed = cacheClient(listener)) {
                var store = new MongoCachePipelinePreparations(observed, observed.getDatabase(f.cacheDatabase.getName()), f.business(),
                        () -> { if (!active.get()) throw MongoCachePipelineAccess.fenced(); });
                assertCode(() -> store.seed(original, f.configuration.preparations(), AT), IoError.CACHE_BUSINESS_FENCED);
            }
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    private static Document marker(PipelinePreparation preparation, String phase) { return new Document("preparationId", preparation.preparationId()).append("phase", phase); }
    private static Document pipelineFilter(PipelineIntentScope scope) { return new Document("_id", MongoWorkloadClaimStore.id(new WorkloadClaimKey(scope.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, scope.pipelineId()))); }
    private static MongoClient cacheClient(CommandListener listener) { return MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(CACHE.getReplicaSetUrl())).addCommandListener(listener).build()); }
    private static boolean updates(CommandStartedEvent e, String field) { return e.getCommandName().equals("update") && e.getCommand().getArray("updates").toString().contains(field); }
    private static void await(CountDownLatch latch) {
        try { if (!latch.await(15, TimeUnit.SECONDS)) throw new AssertionError("test barrier did not open"); }
        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
    }
    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, IoError code) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(code));
    }
    private static void assertFutureCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, IoError code) {
        assertThatThrownBy(action).hasCauseInstanceOf(TapstateException.class).satisfies(error -> assertThat(((TapstateException) error.getCause()).code()).isEqualTo(code));
    }
    private static void failCommand(MongoClient client, String command, Document data) {
        data.append("failCommands", List.of(command));
        client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1)).append("data", data));
    }
    private static void clearFailpoint(MongoClient client) { client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
    private static void withFixture(Scenario scenario) {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String configName = "first_cfg_" + suffix; String cacheName = "first_cache_" + suffix;
        try (var configClient = MongoClients.create(CONFIG.getReplicaSetUrl()); var cacheClient = MongoClients.create(CACHE.getReplicaSetUrl())) {
            var cfg = configClient.getDatabase(configName); var cache = cacheClient.getDatabase(cacheName);
            var settings = new MongoConnectionSettings(CONFIG.getReplicaSetUrl(configName), null, Duration.ofSeconds(5));
            var cacheConnection = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, CACHE.getReplicaSetUrl(cacheName), cacheName, null);
            try (var configuration = MongoConfigurationBootstrap.prepare(settings, "cluster-a", "installation-a", "setup-a", 1);
                    var domain = MongoCacheDomain.connect(cacheConnection, Duration.ofSeconds(5), "first-preparation-test")) {
                var master = new SourceResource(StorageBinding.defaultSourceId(StorageBinding.Mode.OP), null, "mongodb", Map.of("uri", "mongodb://master.invalid/master"), null, null, null, null);
                configuration.bindings().commit(new StorageSetupScope("cluster-a", "installation-a", "setup-a", 1), StorageBinding.Mode.OP, cacheConnection, master);
                var business = configuration.bindings().cachePreparation().orElseThrow().businessScope();
                domain.prepareAuth(new CacheAuthScope(business.clusterId(), business.bindingVersion(), StorageBinding.Mode.OP, business.cacheIncarnation(), 1), "auth-a", ACTIVE);
                try (var session = cacheClient.startSession()) {
                    for (var table : List.of(SystemCollections.CACHE_WORKLOAD_CLAIMS, SystemCollections.CACHE_PIPELINE_STATE, SystemCollections.CACHE_PIPELINE_OBSERVATION)) table.prepareCollectionOn(cache, session);
                }
                SystemCollections.CACHE_IDENTITY.on(cache).updateOne(new Document("_id", "cluster"), new Document("$set", new Document("businessPhase", "READY")
                        .append("businessPreparationId", "business-a").append("claimAdmissionFence", 0L)));
                SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(cache).insertOne(new Document("_id", "business").append("installedVersion", 1).append("phase", "READY")
                        .append("epoch", 1L).append("operationId", "business-a"));
                var pipeline = new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: flow\nsource: []\n");
                configuration.artifacts().create(pipeline);
                var scope = new PipelineIntentScope(business.clusterId(), business.bindingVersion(), business.cacheIncarnation(), "flow", 1);
                configuration.intents().compareAndSet(scope, 0, new DesiredState("flow", PipelineState.RUNNING, CanonicalHash.of(pipeline), false));
                var nodeClaim = domain.workloadClaims(business).acquire(new WorkloadClaimKey("cluster-a", WorkloadClaimType.NODE_SESSION, OWNER.nodeId()), OWNER, 1, TTL).claim();
                scenario.run(new Fixture(configuration, cfg, cacheClient, cache, domain, scope, WorkloadClaimFence.from(nodeClaim)));
            } finally { cfg.drop(); cache.drop(); }
        } catch (RuntimeException | Error failure) { throw failure; }
        catch (Exception failure) { throw new AssertionError(failure); }
    }
    private record Fixture(MongoConfigurationDomain configuration, MongoDatabase configDatabase, MongoClient cacheClient,
            MongoDatabase cacheDatabase, MongoCacheDomain cacheDomain, PipelineIntentScope scope, WorkloadClaimFence node) {
        CacheBusinessScope business() { return new CacheBusinessScope(scope.clusterId(), scope.bindingVersion(), scope.cacheIncarnation()); }
        MongoConnectionSettings settings() { return new MongoConnectionSettings(CONFIG.getReplicaSetUrl(configDatabase.getName()), null, Duration.ofSeconds(5)); }
        PipelineCachePreparations preparations() { return cacheDomain.pipelinePreparations(business(), ACTIVE); }
        FirstPipelineCreation issue() { return ((FirstPipelineCreationResult.Issued) configuration.preparations().issueFirstCreate(scope, node)).creation(); }
        long version() { return configDatabase.getCollection("configuration_version").find().first().getLong("revision"); }
        MongoCollection<Document> claims() { return SystemCollections.CACHE_WORKLOAD_CLAIMS.on(cacheDatabase); }
        WorkloadClaimFence node(WorkloadOwner owner) { return WorkloadClaimFence.from(cacheDomain.workloadClaims(business())
                .acquire(new WorkloadClaimKey(scope.clusterId(), WorkloadClaimType.NODE_SESSION, owner.nodeId()), owner, 1, TTL).claim()); }
        Document claim() { return claims().find(pipelineFilter(scope)).first(); }
        Document state() { return SystemCollections.CACHE_PIPELINE_STATE.on(cacheDatabase).find(new Document("_id", scope.pipelineId())).first(); }
        Map<String, List<Document>> snapshot() {
            var out = new java.util.TreeMap<String, List<Document>>();
            for (var table : List.of(SystemCollections.CACHE_WORKLOAD_CLAIMS, SystemCollections.CACHE_PIPELINE_STATE, SystemCollections.CACHE_IDENTITY))
                out.put(table.collectionName(), table.on(cacheDatabase).find().sort(new Document("_id", 1)).into(new ArrayList<>()));
            return out;
        }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
