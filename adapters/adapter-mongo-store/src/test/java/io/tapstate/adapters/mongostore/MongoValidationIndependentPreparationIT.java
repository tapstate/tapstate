package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.validation.*;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import static io.tapstate.adapters.mongostore.MongoValidationPreparationFacts.*;
import static org.assertj.core.api.Assertions.*;

/** Two actual Mongo servers; explicit schema fixtures do not stand in for production upgrade acceptance. */
@RequiresDocker
class MongoValidationIndependentPreparationIT {
    @Container static final MongoDBContainer CONFIG = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @Container static final MongoDBContainer CACHE = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    static final String PIPELINE = "flow";
    static final Duration TTL = Duration.ofSeconds(30);

    @Test void neverStartedPipelineGetsIndependentBirthWithoutProductionStateLeaseOrModels() {
        try (var f = new Fixture()) {
            var original = f.productionFacts(); var preparation = f.config.issue(PIPELINE, f.scope);
            var proof = f.cache.prepare(preparation, f.nodeA); var complete = f.config.complete(preparation, proof);
            assertThat(complete.requireComplete().phase()).isEqualTo(Phase.COMPLETE);
            assertThat(f.productionFacts()).isEqualTo(original);
            assertThat(f.configuration.preparations().read(PIPELINE).orElseThrow().phase()).isEqualTo(PipelinePreparation.Phase.UNASSIGNED);
            assertThat(f.cacheDb.listCollectionNames()).doesNotContain("pipeline_state", "source_schemas", "direct_channel_offsets", "snapshot_orders");
            var key = ValidationClaims.key("cluster", PIPELINE);
            assertThat(f.rawClaims().read(key).orElseThrow().leased()).isFalse();
            assertThat(f.rawClaims().read(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE))).isEmpty();
        }
    }

    @Test void twoMembersIssueOneSharedSubjectAndLoserCannotInventFirstCreationRights() throws Exception {
        try (var f = new Fixture(); var workers = Executors.newFixedThreadPool(2)) {
            var gate = new CountDownLatch(1);
            var one = workers.submit(() -> { gate.await(); return f.config.issue(PIPELINE, f.scope); });
            var two = workers.submit(() -> { gate.await(); return f.config.issue(PIPELINE, f.scope); }); gate.countDown();
            var a = one.get(10, TimeUnit.SECONDS); var b = two.get(10, TimeUnit.SECONDS);
            assertThat(a.subject()).isEqualTo(b.subject());
            MongoValidationCachePreparations.Proof proof;
            try { proof = f.cache.prepare(a, f.nodeA); }
            catch (TapstateException notCreator) { proof = f.cache.prepare(b, f.nodeB); }
            f.config.complete(a, proof);
            f.cache.prepare(b, f.nodeB);
            assertThat(f.controls().countDocuments()).isOne();
            assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.cacheDb).countDocuments(new Document("resourceType", "VALIDATION"))).isOne();
        }
    }

    @Test void cancellationFirstPreventsARealBirthAlreadyPastItsConfigurationPrecheck() throws Exception {
        try (var f = new Fixture(); var worker = Executors.newSingleThreadExecutor()) {
            var permission = f.config.issue(PIPELINE, f.scope); var entered = new CountDownLatch(1); var release = new CountDownLatch(1); var once = new AtomicBoolean();
            try (var observed = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(CACHE.getReplicaSetUrl()))
                    .addCommandListener(new CommandListener() {
                        @Override public void commandStarted(CommandStartedEvent event) {
                            if (event.getCommandName().equals("aggregate") && event.getCommand().getString("aggregate").getValue().equals("workload_claims")
                                    && once.compareAndSet(false, true)) {
                                entered.countDown(); await(release);
                            }
                        }
                    }).build())) {
                var delayed = new MongoValidationCachePreparations(observed, observed.getDatabase(f.cacheDb.getName()), f.scope,
                        MongoStorageBindingStore.targetFingerprint(f.cacheConnection), () -> {});
                var pending = worker.submit(() -> delayed.prepare(permission, f.nodeA));
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                var closing = f.config.beginClose(PIPELINE, permission.subject().generation(), UUID.randomUUID());
                try {
                    var cancelled = f.cache.cancel(closing, f.nodeB);
                    assertThat(cancelled.requireClosed(permission.subject()).outcome()).isEqualTo(Outcome.CANCELLED);
                    assertThat(f.config.finishClose(closing, cancelled).phase()).isEqualTo(Phase.CLOSED);
                } finally { release.countDown(); }
                assertThatThrownBy(() -> pending.get(10, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class);
            }
            assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.cacheDb).countDocuments(new Document("resourceType", "VALIDATION"))).isZero();
            assertThat(f.control().getString("birthState")).isEqualTo("CANCELLED");
        }
    }

    @Test void birthFirstRequiresTheOriginalRetiredReceiptAndPreservesClaimHistory() {
        try (var f = new Fixture()) {
            var permission = f.config.issue(PIPELINE, f.scope); var proof = f.cache.prepare(permission, f.nodeA); f.config.complete(permission, proof);
            var key = ValidationClaims.key("cluster", PIPELINE);
            var claim = f.rawClaims().acquire(key, f.nodeA.owner(), 1, TTL).claim();
            var executing = f.rawClaims().advanceExecution(claim, 1, Set.of(f.nodeA.owner().nodeId())).orElseThrow();
            var closing = f.config.beginClose(PIPELINE, permission.subject().generation(), UUID.randomUUID());
            assertThatThrownBy(() -> f.config.finishClose(closing, proof)).isInstanceOf(TapstateException.class);
            var retired = f.cache.cancel(closing, f.nodeB);
            assertThat(retired.requireClosed(permission.subject()).outcome()).isEqualTo(Outcome.RETIRED);
            assertThat(f.config.finishClose(closing, retired).phase()).isEqualTo(Phase.CLOSED);
            var held = f.rawClaims().read(key).orElseThrow(); assertThat(held.leased()).isFalse();
            assertThat(held.claim().claimGeneration()).isEqualTo(executing.claimGeneration());
            assertThat(held.claim().executionGeneration()).isEqualTo(executing.executionGeneration());
        }
    }

    @Test void retryingIssuedWithoutTheOriginalFreshRightNeverCreatesMissingCacheFacts() {
        try (var f = new Fixture()) {
            var original = f.config.issue(PIPELINE, f.scope); var repeated = f.config.issue(PIPELINE, f.scope);
            assertThat(repeated.subject()).isEqualTo(original.subject());
            assertThatThrownBy(() -> f.cache.prepare(repeated, f.nodeB)).isInstanceOf(TapstateException.class);
            assertThat(f.controls().countDocuments()).isZero();
            f.cache.prepare(original, f.nodeA);
            assertThat(f.cache.prepare(repeated, f.nodeB).requirePrepared(original.subject()).outcome()).isEqualTo(Outcome.PREPARED);
        }
    }

    @Test void completedPreparationCannotRepairLostClaimOrControl() {
        try (var f = new Fixture()) {
            var permission = f.config.issue(PIPELINE, f.scope); f.config.complete(permission, f.cache.prepare(permission, f.nodeA));
            var key = ValidationClaims.key("cluster", PIPELINE);
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.cacheDb).deleteOne(new Document("_id", MongoWorkloadClaimStore.id(key)));
            assertThatThrownBy(() -> f.cache.prepare(f.config.issue(PIPELINE, f.scope), f.nodeB)).isInstanceOf(TapstateException.class);
            f.controls().deleteOne(new Document("_id", MongoValidationControlStore.controlId("cluster", PIPELINE)));
            assertThatThrownBy(() -> f.cache.prepare(permission, f.nodeA)).isInstanceOf(TapstateException.class);
            assertThat(f.controls().countDocuments()).isZero(); assertThat(f.rawClaims().read(key)).isEmpty();
        }
    }

    @Test void completedBirthLossCannotBeCancelledAsUnusedOrReopenedWithResetCounters() {
        try (var f = new Fixture()) {
            var permission = f.config.issue(PIPELINE, f.scope);
            f.config.complete(permission, f.cache.prepare(permission, f.nodeA));
            var key = ValidationClaims.key("cluster", PIPELINE);
            var claim = f.rawClaims().acquire(key, f.nodeA.owner(), 1, TTL).claim();
            f.rawClaims().advanceExecution(claim, 1, Set.of(f.nodeA.owner().nodeId())).orElseThrow();
            var closing = f.config.beginClose(PIPELINE, permission.subject().generation(), UUID.randomUUID());
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.cacheDb).deleteOne(new Document("_id", MongoWorkloadClaimStore.id(key)));
            f.controls().deleteOne(new Document("_id", MongoValidationControlStore.controlId("cluster", PIPELINE)));

            assertThatThrownBy(() -> f.cache.cancel(closing, f.nodeB)).isInstanceOf(TapstateException.class);
            assertThat(f.config.read(PIPELINE).phase()).isEqualTo(Phase.COMPLETE);
            assertThat(f.config.read(PIPELINE).closingOperationId()).isEqualTo(closing.operation());
            assertThatThrownBy(() -> f.config.restartClosed(PIPELINE, permission.subject().generation(), f.config.read(PIPELINE).revision()))
                    .isInstanceOf(TapstateException.class);
            assertThat(f.controls().countDocuments()).isZero();
            assertThat(f.rawClaims().read(key)).isEmpty();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"countingGeneration", "retainedRuns", "owner", "closed", "problemLimit", "revision", "admissionClosed", "slots"})
    void incompleteControlCannotCertifyPreparedOrCompleteConfiguration(String field) {
        try (var f = new Fixture()) {
            var permission = f.config.issue(PIPELINE, f.scope);
            var proof = f.cache.prepare(permission, f.nodeA);
            f.controls().updateOne(new Document("_id", f.control().get("_id")), new Document("$unset", new Document(field, "")));
            var damaged = f.control();
            assertThatThrownBy(() -> f.config.complete(permission, proof)).isInstanceOf(TapstateException.class);
            assertThat(f.config.read(PIPELINE).phase()).isEqualTo(Phase.ISSUED);
            assertThat(f.control()).isEqualTo(damaged);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("damagedControlFields")
    void malformedControlOrMissingPhysicalSlotCannotIssueAReceipt(String reason, Document changes) {
        try (var f = new Fixture()) {
            var permission = f.config.issue(PIPELINE, f.scope);
            var proof = f.cache.prepare(permission, f.nodeA);
            var damaged = f.control(); damaged.putAll(changes);
            f.controls().replaceOne(new Document("_id", damaged.get("_id")), damaged);
            assertThatThrownBy(() -> f.config.complete(permission, proof)).as(reason).isInstanceOf(TapstateException.class);
            assertThat(f.config.read(PIPELINE).phase()).isEqualTo(Phase.ISSUED);
            assertThat(f.control()).isEqualTo(damaged);
        }
    }

    static Stream<Arguments> damagedControlFields() {
        var owner = WorkloadClaimDocuments.stored(ValidationClaims.fence(new ValidationOwner("cluster", PIPELINE, "node-a", "boot-a", 1, 1, 1)));
        var runId = new Document("kind", "run").append("clusterId", "cluster").append("pipelineId", PIPELINE)
                .append("taskId", "task").append("mode", "FULL").append("runId", "run");
        return Stream.of(
                Arguments.of("negative generation", new Document("countingGeneration", -1L)),
                Arguments.of("wrong generation BSON type", new Document("countingGeneration", 0)),
                Arguments.of("negative retained count", new Document("retainedRuns", -1L)),
                Arguments.of("unbounded retained count", new Document("retainedRuns", MongoValidationControlStore.MAX_RETAINED_RUNS + 1)),
                Arguments.of("open empty generation", new Document("closed", false)),
                Arguments.of("first use with an owner", new Document("owner", owner)),
                Arguments.of("first use with problem stop", new Document("problemLimit", true)),
                Arguments.of("first use with retained history", new Document("retainedRuns", 1L)),
                Arguments.of("noncanonical close identity", new Document("closingOperationId", "invalid").append("admissionClosed", true)),
                Arguments.of("slot count exceeds retained", new Document("countingGeneration", 1L).append("owner", owner)
                        .append("closed", false).append("slots", new Document("dGFzaw:FULL", runId))),
                Arguments.of("slot key disagrees with physical identity", new Document("countingGeneration", 1L).append("owner", owner)
                        .append("retainedRuns", 1L).append("closed", false).append("slots", new Document("wrong:FULL", runId))),
                Arguments.of("missing physical run", new Document("countingGeneration", 1L).append("owner", owner)
                        .append("retainedRuns", 1L).append("closed", false).append("slots", new Document("dGFzaw:FULL", runId))));
    }

    @Test void neverBornCancellationCannotCertifyNonzeroOwnerHistory() {
        try (var f = new Fixture()) {
            var permission = f.config.issue(PIPELINE, f.scope);
            var closing = f.config.beginClose(PIPELINE, permission.subject().generation(), UUID.randomUUID());
            var proof = f.cache.cancel(closing, f.nodeA);
            var owner = new ValidationOwner("cluster", PIPELINE, "node-a", "boot-a", 1, 1, 1);
            f.controls().updateOne(new Document("_id", f.control().get("_id")), new Document("$set", new Document("countingGeneration", 1L)
                    .append("owner", WorkloadClaimDocuments.stored(ValidationClaims.fence(owner)))));
            assertThatThrownBy(() -> f.config.finishClose(closing, proof)).isInstanceOf(TapstateException.class);
            assertThat(f.config.read(PIPELINE).phase()).isEqualTo(Phase.ISSUED);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "negative", "wrong-type", "exhausted"})
    void claimWithoutAValidWriteWitnessCannotCertifyPreparation(String damage) {
        try (var f = new Fixture()) {
            var permission = f.config.issue(PIPELINE, f.scope);
            var proof = f.cache.prepare(permission, f.nodeA);
            Object value = switch (damage) { case "negative" -> -1L; case "wrong-type" -> 0; default -> Long.MAX_VALUE; };
            var update = damage.equals("missing") ? new Document("$unset", new Document("fencedValidationWrites", ""))
                    : new Document("$set", new Document("fencedValidationWrites", value));
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.cacheDb).updateOne(
                    new Document("_id", MongoWorkloadClaimStore.id(ValidationClaims.key("cluster", PIPELINE))), update);
            assertThatThrownBy(() -> f.config.complete(permission, proof)).isInstanceOf(TapstateException.class);
            assertThat(f.config.read(PIPELINE).phase()).isEqualTo(Phase.ISSUED);
        }
    }

    @Test void openActiveLedgerCannotCertifyAProblemLimitLatch() {
        try (var f = new Fixture()) {
            var permission = f.config.issue(PIPELINE, f.scope);
            var proof = f.cache.prepare(permission, f.nodeA);
            var key = ValidationClaims.key("cluster", PIPELINE);
            var claim = f.rawClaims().acquire(key, f.nodeA.owner(), 1, TTL).claim();
            claim = f.rawClaims().advanceExecution(claim, 1, Set.of(f.nodeA.owner().nodeId())).orElseThrow();
            var run = new ValidationControlStore.Run(new ValidationRunIdentity(ValidationClaims.owner(PIPELINE, claim), "task", "run",
                    ValidationMode.FULL, "spec", 1, "rules"), 1);
            f.controls().insertOne(new Document("_id", MongoValidationControlStore.runId(run)).append("formatVersion", 2)
                    .append("scope", MongoValidationControlStore.scope(f.scope)).append("validationSubject", encode(permission.subject()))
                    .append("identity", MongoValidationControlStore.identity(run)).append("state", "STARTING").append("retired", false));
            f.controls().updateOne(new Document("_id", f.control().get("_id")), new Document("$set", new Document("countingGeneration", 1L)
                    .append("owner", WorkloadClaimDocuments.stored(run.fence())).append("retainedRuns", 1L).append("closed", false)
                    .append("slots", new Document("dGFzaw:FULL", MongoValidationControlStore.runId(run)))));
            assertThat(proof.requirePrepared(permission.subject()).outcome()).isEqualTo(Outcome.PREPARED);
            f.controls().updateOne(new Document("_id", f.control().get("_id")), new Document("$set", new Document("problemLimit", true)));
            assertThatThrownBy(() -> f.config.complete(permission, proof)).isInstanceOf(TapstateException.class);
            assertThat(f.config.read(PIPELINE).phase()).isEqualTo(Phase.ISSUED);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"claimGeneration", "executionGeneration"})
    void claimCountersCannotRegressBelowTheRetainedOwnerHistory(String counter) {
        try (var f = new Fixture()) {
            var permission = f.config.issue(PIPELINE, f.scope);
            f.config.complete(permission, f.cache.prepare(permission, f.nodeA));
            var key = ValidationClaims.key("cluster", PIPELINE);
            var claim = f.rawClaims().acquire(key, f.nodeA.owner(), 1, TTL).claim();
            claim = f.rawClaims().advanceExecution(claim, 1, Set.of(f.nodeA.owner().nodeId())).orElseThrow();
            claim = f.rawClaims().advanceExecution(claim, 1, Set.of(f.nodeA.owner().nodeId())).orElseThrow();
            f.controls().updateOne(new Document("_id", f.control().get("_id")), new Document("$set", new Document("countingGeneration", 1L)
                    .append("owner", WorkloadClaimDocuments.stored(ValidationClaims.fence(ValidationClaims.owner(PIPELINE, claim))))));
            var closing = f.config.beginClose(PIPELINE, permission.subject().generation(), UUID.randomUUID());
            var claims = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.cacheDb);
            claims.updateOne(new Document("_id", MongoWorkloadClaimStore.id(key)), new Document("$inc", new Document(counter, -1L)));
            var original = f.control();
            assertThatThrownBy(() -> f.cache.cancel(closing, f.nodeB)).isInstanceOf(TapstateException.class);
            assertThat(f.config.read(PIPELINE).phase()).isEqualTo(Phase.COMPLETE);
            assertThat(f.control()).isEqualTo(original);
        }
    }

    @Test void lawfulSameOwnerTopologyRefreshKeepsHistoricalClosureAvailable() {
        try (var f = new Fixture()) {
            var permission = f.config.issue(PIPELINE, f.scope);
            f.config.complete(permission, f.cache.prepare(permission, f.nodeA));
            var key = ValidationClaims.key("cluster", PIPELINE);
            var claim = f.rawClaims().acquire(key, f.nodeA.owner(), 1, TTL).claim();
            claim = f.rawClaims().advanceExecution(claim, 1, Set.of(f.nodeA.owner().nodeId())).orElseThrow();
            f.controls().updateOne(new Document("_id", f.control().get("_id")), new Document("$set", new Document("countingGeneration", 1L)
                    .append("owner", WorkloadClaimDocuments.stored(ValidationClaims.fence(ValidationClaims.owner(PIPELINE, claim))))));
            var refreshed = f.rawClaims().acquire(key, f.nodeA.owner(), 2, TTL).claim();
            assertThat(refreshed.claimGeneration()).isEqualTo(claim.claimGeneration());
            assertThat(refreshed.executionGeneration()).isEqualTo(claim.executionGeneration());
            assertThat(refreshed.topologyRevision()).isEqualTo(2);
            var closing = f.config.beginClose(PIPELINE, permission.subject().generation(), UUID.randomUUID());
            assertThat(f.config.finishClose(closing, f.cache.cancel(closing, f.nodeB)).phase()).isEqualTo(Phase.CLOSED);
        }
    }

    @Test void closedPredecessorReplacementKeepsCountersAndFencesOldCallbacks() {
        try (var f = new Fixture()) {
            var old = f.config.issue(PIPELINE, f.scope); f.config.complete(old, f.cache.prepare(old, f.nodeA));
            var key = ValidationClaims.key("cluster", PIPELINE); var first = f.rawClaims().acquire(key, f.nodeA.owner(), 1, TTL).claim();
            first = f.rawClaims().advanceExecution(first, 1, Set.of(f.nodeA.owner().nodeId())).orElseThrow();
            var closing = f.config.beginClose(PIPELINE, old.subject().generation(), UUID.randomUUID());
            var closed = f.config.finishClose(closing, f.cache.cancel(closing, f.nodeB));
            f.config.restartClosed(PIPELINE, closed.generation(), closed.revision());
            var fresh = f.config.issue(PIPELINE, f.scope); f.config.complete(fresh, f.cache.prepare(fresh, f.nodeB));
            var second = f.rawClaims().acquire(key, f.nodeB.owner(), 1, TTL).claim();
            second = f.rawClaims().advanceExecution(second, 1, Set.of(f.nodeB.owner().nodeId())).orElseThrow();
            assertThat(second.claimGeneration()).isGreaterThan(first.claimGeneration());
            assertThat(second.executionGeneration()).isGreaterThan(first.executionGeneration());
            var before = f.control();
            assertThatThrownBy(() -> f.cache.prepare(old, f.nodeA)).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.cache.cancel(closing, f.nodeA)).isInstanceOf(TapstateException.class);
            assertThat(f.control()).isEqualTo(before);
        }
    }

    @Test void missingMarkerIsUnknownAndReadOrIssueNeverRecreatesIt() {
        try (var f = new Fixture()) {
            SystemCollections.OPERATOR_GENERATIONS.on(f.configDb).updateOne(new Document("_id", PIPELINE), new Document("$unset", new Document("validationPreparation", "")));
            var before = f.mapping();
            assertThatThrownBy(() -> f.config.issue(PIPELINE, f.scope)).isInstanceOf(TapstateException.class);
            assertThat(f.mapping()).isEqualTo(before); assertThat(f.controls().countDocuments()).isZero();
        }
    }

    private static void await(CountDownLatch latch) {
        try { if (!latch.await(10, TimeUnit.SECONDS)) throw new AssertionError("test barrier did not open"); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
    }
    private static final class Fixture implements AutoCloseable {
        final MongoClient configClient = MongoClients.create(CONFIG.getReplicaSetUrl()), cacheClient = MongoClients.create(CACHE.getReplicaSetUrl());
        final String suffix = UUID.randomUUID().toString().replace("-", "");
        final MongoDatabase configDb = configClient.getDatabase("validation_cfg_" + suffix), cacheDb = cacheClient.getDatabase("validation_cache_" + suffix);
        final StorageBinding.Connection cacheConnection = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, CACHE.getReplicaSetUrl(cacheDb.getName()), cacheDb.getName(), null);
        final MongoConfigurationDomain configuration;
        final MongoCacheDomain cacheDomain;
        final MongoValidationPreparations config;
        final MongoValidationCachePreparations cache;
        final CacheBusinessScope scope;
        final WorkloadClaimFence nodeA, nodeB;
        Fixture() {
            configuration = MongoConfigurationBootstrap.prepare(new MongoConnectionSettings(CONFIG.getReplicaSetUrl(configDb.getName()), null, Duration.ofSeconds(5)), "cluster", "installation", "setup", 1);
            var master = new SourceResource("views", null, "mongodb", Map.of("uri", "mongodb://business.invalid/master"), null, null, null, null);
            configuration.bindings().commit(new StorageSetupScope("cluster", "installation", "setup", 1), StorageBinding.Mode.OP, cacheConnection, master);
            configuration.artifacts().create(new SourceResource("input", null, "mongodb", Map.of("uri", "mongodb://business.invalid/source"), null, null, null, null));
            configuration.artifacts().create(new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: flow\nsource: [input]\n"));
            scope = configuration.bindings().cachePreparation().orElseThrow().businessScope();
            // Explicit supported-schema fixture authority; ordinary read/issue must not add this field.
            SystemCollections.OPERATOR_GENERATIONS.on(configDb).updateOne(new Document("_id", PIPELINE), new Document("$set", new Document("validationPreparation", initial())));
            try (var session = configClient.startSession()) {
                for (var table : List.of(SystemCollections.VALIDATION_TASKS, SystemCollections.VALIDATION_REPORTS, SystemCollections.VALIDATION_DIFFERENCES)) table.prepareCollectionOn(configDb, session);
            }
            cacheDomain = MongoCacheDomain.connect(cacheConnection, Duration.ofSeconds(5), "validation-independent-fixture");
            cacheDomain.prepareAuth(new CacheAuthScope("cluster", 1, StorageBinding.Mode.OP, scope.cacheIncarnation(), 1), "auth", () -> {});
            try (var session = cacheClient.startSession()) {
                for (var table : List.of(SystemCollections.CACHE_WORKLOAD_CLAIMS, SystemCollections.VALIDATION_RUNS)) table.prepareCollectionOn(cacheDb, session);
            }
            SystemCollections.CACHE_IDENTITY.on(cacheDb).updateOne(new Document("_id", "cluster"), new Document("$set", new Document("businessPhase", "READY")
                    .append("businessPreparationId", "business").append("claimAdmissionFence", 0L)));
            SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(cacheDb).insertOne(new Document("_id", "business").append("installedVersion", 1).append("phase", "READY").append("epoch", 1L).append("operationId", "business"));
            nodeA = node("a"); nodeB = node("b");
            config = configuration.validationPreparations(); cache = cacheDomain.validationPreparations(scope);
        }
        WorkloadClaimFence node(String name) {
            var owner = new WorkloadOwner("node-" + name, "boot-" + name);
            return WorkloadClaimFence.from(cacheDomain.workloadClaims(scope).acquire(new WorkloadClaimKey("cluster", WorkloadClaimType.NODE_SESSION, owner.nodeId()), owner, 1, TTL).claim());
        }
        Document mapping() { return SystemCollections.OPERATOR_GENERATIONS.on(configDb).find(new Document("_id", PIPELINE)).first(); }
        Document productionFacts() { var row = new Document(mapping()); row.remove("validationPreparation"); return row; }
        com.mongodb.client.MongoCollection<Document> controls() { return SystemCollections.VALIDATION_RUNS.on(cacheDb); }
        Document control() { return controls().find(new Document("_id", MongoValidationControlStore.controlId("cluster", PIPELINE))).first(); }
        MongoWorkloadClaimStore rawClaims() { return new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(cacheDb)); }
        @Override public void close() { try { configDb.drop(); cacheDb.drop(); } finally { configuration.close(); cacheDomain.close(); configClient.close(); cacheClient.close(); } }
    }
}
