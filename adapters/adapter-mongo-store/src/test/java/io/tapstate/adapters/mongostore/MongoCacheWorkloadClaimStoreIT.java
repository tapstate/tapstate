package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandFailedEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Fixed-scope transactions over explicit test business facts, not production business preparation. */
@RequiresDocker
class MongoCacheWorkloadClaimStoreIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
    private static final CacheAuthScope AUTH = new CacheAuthScope("cluster-a", 1, StorageBinding.Mode.OP, "incarnation-a", 1);
    private static final CacheBusinessScope SCOPE = new CacheBusinessScope("cluster-a", 1, "incarnation-a");
    private static final PipelineIntentScope PIPELINE = new PipelineIntentScope("cluster-a", 1, "incarnation-a", "orders", 1);
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final Runnable ACTIVE = () -> {};

    @Test void normalTransitionsKeepOneAdmissionAndIgnoreIndependentAuthenticationReset() {
        withBusiness(f -> {
            var claims = f.domain().workloadClaims(SCOPE);
            var original = claims.acquirePipeline(PIPELINE, OWNER, 1, TTL).claim();
            var run = claims.advanceExecution(original, 1, Set.of("node-a")).orElseThrow();
            var renewed = claims.renew(run, TTL).orElseThrow();
            var failed = claims.recordExecutionFailure(renewed, false).orElseThrow();
            assertThat(claims.read(KEY).orElseThrow().claim()).isEqualTo(failed);
            assertThat(claims.readAll(List.of(KEY))).containsKey(KEY);
            assertThat(identity(f.database()).getLong("claimAdmissionFence")).isEqualTo(1);
            f.domain().advanceAuthGeneration(AUTH, 2, "auth-reset", ACTIVE);
            assertThat(claims.read(KEY).orElseThrow().claim()).isEqualTo(failed);
            assertThat(claims.release(failed)).isTrue();
            var next = claims.acquirePipeline(PIPELINE, OWNER, 1, TTL).claim();
            assertThat(next.claimGeneration()).isEqualTo(original.claimGeneration() + 1);
            assertThat(next.executionGeneration()).isEqualTo(run.executionGeneration());
            assertThat(claims.release(failed)).isFalse();
            assertThatThrownBy(() -> claims.acquire(new WorkloadClaimKey("another", WorkloadClaimType.CAPTURE, "tail"), OWNER, 1, TTL))
                    .satisfies(MongoCacheWorkloadClaimStoreIT::fenced);
            assertThat(identity(f.database()).getLong("claimAdmissionFence")).isEqualTo(2);
        });
    }

    @Test void separateClientsRacingAnAbsentKeyCommitOneTenureWithoutPoisoningTheRefusal() {
        withBusiness(f -> {
            try (var second = MongoCacheDomain.connect(f.binding(), Duration.ofSeconds(5), "claims-second");
                 var workers = Executors.newFixedThreadPool(2)) {
                var one = f.domain().workloadClaims(SCOPE); var two = second.workloadClaims(SCOPE);
                var start = new CountDownLatch(1);
                var a = workers.submit(() -> { start.await(); return one.acquirePipeline(PIPELINE, OWNER, 1, TTL); });
                var b = workers.submit(() -> { start.await(); return two.acquirePipeline(PIPELINE, new WorkloadOwner("node-b", "boot-b"), 1, TTL); });
                start.countDown();
                var results = List.of(a.get(15, TimeUnit.SECONDS), b.get(15, TimeUnit.SECONDS));
                assertThat(results).filteredOn(io.tapstate.spi.store.WorkloadClaimAttempt::acquired).hasSize(1);
                assertThat(results.getFirst().claim()).isEqualTo(results.getLast().claim());
                assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).countDocuments()).isEqualTo(1);
                assertThat(identity(f.database()).getLong("claimAdmissionFence")).isEqualTo(2);
            }
        });
    }

    @Test void retirementExpiresEveryOriginalClaimPreservesHistoryAndLeavesAuthenticationReady() {
        withBusiness(f -> {
            var claims = f.domain().workloadClaims(SCOPE);
            var pipeline = claims.advanceExecution(claims.acquirePipeline(PIPELINE, OWNER, 1, TTL).claim(), 1, Set.of("node-a")).orElseThrow();
            var nodeKey = new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.NODE_SESSION, "node-a");
            var captureKey = new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.CAPTURE, "tail-a");
            claims.acquire(nodeKey, OWNER, 0, TTL); claims.acquire(captureKey, OWNER, 1, TTL);
            var before = claimBodies(f.database());
            f.domain().retireBusiness(SCOPE, "retire-a", ACTIVE);
            assertThat(claimBodies(f.database())).isEqualTo(before);
            var raw = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            for (var key : List.of(KEY, nodeKey, captureKey)) assertThat(raw.read(key).orElseThrow().leased()).isFalse();
            assertThatThrownBy(() -> claims.renew(pipeline, TTL)).satisfies(MongoCacheWorkloadClaimStoreIT::fenced);
            assertThatThrownBy(() -> claims.acquirePipeline(PIPELINE, OWNER, 1, TTL)).satisfies(MongoCacheWorkloadClaimStoreIT::fenced);
            assertThatThrownBy(() -> claims.advanceExecution(pipeline, 1, Set.of("node-a"))).satisfies(MongoCacheWorkloadClaimStoreIT::fenced);
            assertThat(f.domain().loadAuthScope(AUTH)).isEqualTo(AUTH);
            var retired = snapshot(f.database());
            f.domain().retireBusiness(SCOPE, "retire-a", ACTIVE);
            assertThat(snapshot(f.database())).isEqualTo(retired);
            assertThatThrownBy(() -> f.domain().retireBusiness(SCOPE, "another-operation", ACTIVE)).satisfies(MongoCacheWorkloadClaimStoreIT::fenced);
            assertThatThrownBy(() -> f.domain().loadBusinessScope(new CacheBusinessScope("cluster-a", 1, "another-incarnation")))
                    .satisfies(MongoCacheWorkloadClaimStoreIT::fenced);
        });
    }

    @Test void anAdmissionPausedAfterReadingReadyCannotEscapeACompletedRetirement() {
        withBusiness(f -> {
            var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1); var once = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getCommandName().equals("update") && event.getCommand().getString("update").getValue().equals("cache_identity")
                            && once.compareAndSet(false, true)) { paused.countDown(); await(proceed); }
                }
            };
            try (var observed = client(listener); var workers = Executors.newSingleThreadExecutor()) {
                var store = new MongoCacheWorkloadClaimStore(observed, observed.getDatabase(f.database().getName()), SCOPE, ACTIVE);
                var result = workers.submit(() -> store.acquirePipeline(PIPELINE, OWNER, 1, TTL));
                try {
                    assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue();
                    f.domain().retireBusiness(SCOPE, "retire-a", ACTIVE);
                } finally { proceed.countDown(); }
                assertThatThrownBy(() -> result.get(15, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class)
                        .satisfies(error -> fenced(error.getCause()));
                assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).countDocuments()).isZero();
                assertThat(identity(f.database())).containsEntry("businessPhase", "RETIRED").containsEntry("claimAdmissionFence", 0L);
            }
        });
    }

    @Test void anAbortedCallerTransactionRollsBackRetirementAndAllClaimExpirations() {
        withBusiness(f -> {
            f.domain().workloadClaims(SCOPE).acquirePipeline(PIPELINE, OWNER, 1, TTL);
            var before = snapshot(f.database());
            try (var session = f.admin().startSession()) {
                assertThatThrownBy(() -> session.withTransaction(() -> {
                    new MongoCacheBusinessIdentity(f.database(), SCOPE).retire(session, "retire-a");
                    assertThat(SystemCollections.CACHE_IDENTITY.on(f.database()).find(session).first())
                            .containsEntry("businessPhase", "RETIRED");
                    var reading = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()))
                            .inSession(session).read(KEY).orElseThrow();
                    assertThat(reading.leased()).isFalse();
                    throw new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null);
                }, com.mongodb.TransactionOptions.builder().readConcern(com.mongodb.ReadConcern.SNAPSHOT)
                        .writeConcern(com.mongodb.WriteConcern.MAJORITY.withJournal(true)).build()))
                        .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(StoreError.INITIALIZATION_REFUSED));
            }
            assertThat(snapshot(f.database())).isEqualTo(before);
            assertThat(f.domain().workloadClaims(SCOPE).read(KEY).orElseThrow().leased()).isTrue();
        });
    }

    @Test void missingAuthorityCollectionAndClosedViewsRefuseWithoutImplicitCreation() {
        withBusiness(f -> {
            var claims = f.domain().workloadClaims(SCOPE);
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).drop();
            assertThatThrownBy(() -> claims.acquirePipeline(PIPELINE, OWNER, 1, TTL)).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            assertThat(f.database().listCollectionNames().into(new ArrayList<>())).doesNotContain("workload_claims");
            assertThat(identity(f.database()).getLong("claimAdmissionFence")).isZero();
            f.domain().close();
            assertThatThrownBy(() -> claims.read(KEY)).satisfies(MongoCacheWorkloadClaimStoreIT::fenced);
        });
    }

    @Test void transientFailureRetriesTheSameScopeAndRechecksTheOriginalGuard() {
        withBusiness(f -> {
            var active = new AtomicBoolean(true); var failed = new AtomicInteger(); var modifications = new AtomicInteger();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getCommandName().equals("findAndModify")) modifications.incrementAndGet();
                }
                @Override public void commandFailed(CommandFailedEvent event) {
                    if (event.getCommandName().equals("findAndModify")) { failed.incrementAndGet(); active.set(false); }
                }
            };
            try (var observed = client(listener)) {
                var claims = new MongoCacheWorkloadClaimStore(observed, observed.getDatabase(f.database().getName()), SCOPE, () -> {
                    if (!active.get()) throw MongoCacheBusinessIdentity.businessFenced();
                });
                f.admin().getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                        .append("data", new Document("failCommands", List.of("findAndModify")).append("errorCode", 112)
                                .append("errorLabels", List.of("TransientTransactionError"))));
                try { assertThatThrownBy(() -> claims.acquirePipeline(PIPELINE, OWNER, 1, TTL)).satisfies(MongoCacheWorkloadClaimStoreIT::fenced); }
                finally { f.admin().getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
                assertThat(failed).hasValue(1); assertThat(modifications).hasValue(1);
                assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).countDocuments()).isZero();
                assertThat(identity(f.database()).getLong("claimAdmissionFence")).isZero();
            }
        });
    }

    @Test void admissionExhaustionRefusesWithoutOverflowAndStillAllowsRetirement() {
        withBusiness(f -> {
            var claims = f.domain().workloadClaims(SCOPE);
            SystemCollections.CACHE_IDENTITY.on(f.database()).updateOne(new Document(),
                    new Document("$set", new Document("claimAdmissionFence", Long.MAX_VALUE - 1)));
            assertThatThrownBy(() -> claims.acquirePipeline(PIPELINE, OWNER, 1, TTL)).satisfies(MongoCacheWorkloadClaimStoreIT::fenced);
            assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).countDocuments()).isZero();
            assertThat(identity(f.database()).getLong("claimAdmissionFence")).isEqualTo(Long.MAX_VALUE - 1);
            f.domain().retireBusiness(SCOPE, "retire-a", ACTIVE);
            f.domain().retireBusiness(SCOPE, "retire-a", ACTIVE);
            assertThat(identity(f.database())).containsEntry("businessPhase", "RETIRED");
        });
    }

    @Test void aPipelineCannotAcquireByBareIdWithoutItsOriginalStateScope() {
        withBusiness(f -> {
            var before = snapshot(f.database());
            assertThatThrownBy(() -> f.domain().workloadClaims(SCOPE).acquire(KEY, OWNER, 1, TTL))
                    .satisfies(MongoCacheWorkloadClaimStoreIT::fenced);
            assertThat(snapshot(f.database())).isEqualTo(before);
        });
    }

    @Test void staleScopeCapturedBeforeReplacementCannotAcquireAFreshPipelineClaim() {
        withBusiness(f -> {
            var next = new PipelineIntentScope("cluster-a", 1, "incarnation-a", "orders", 2);
            SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).updateOne(new Document("_id", "orders"), new Document("$set", new Document("scope", header(next))));
            var before = snapshot(f.database());
            assertThatThrownBy(() -> f.domain().workloadClaims(SCOPE).acquirePipeline(PIPELINE, OWNER, 1, TTL))
                    .satisfies(MongoCacheWorkloadClaimStoreIT::fenced);
            assertThat(snapshot(f.database())).isEqualTo(before);
            assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).countDocuments()).isZero();
            var current = f.domain().workloadClaims(SCOPE).acquirePipeline(next, OWNER, 1, TTL);
            assertThat(current.acquired()).isTrue();
            assertThat(identity(f.database()).getLong("claimAdmissionFence")).isEqualTo(1);
        });
    }

    @Test void anAdmissionPausedAfterReadingOldStateCannotEscapeItsReplacement() {
        withBusiness(f -> {
            var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1); var once = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getCommandName().equals("update") && event.getCommand().getString("update").getValue().equals("pipeline_state")
                            && once.compareAndSet(false, true)) { paused.countDown(); await(proceed); }
                }
            };
            var next = new PipelineIntentScope("cluster-a", 1, "incarnation-a", "orders", 2);
            try (var observed = client(listener); var workers = Executors.newSingleThreadExecutor()) {
                var store = new MongoCacheWorkloadClaimStore(observed, observed.getDatabase(f.database().getName()), SCOPE, ACTIVE);
                var result = workers.submit(() -> store.acquirePipeline(PIPELINE, OWNER, 1, TTL));
                try {
                    assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue();
                    // Replace only the state row: admission must conflict on this Pipeline, not a global switch write.
                    SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).updateOne(new Document("_id", "orders"), new Document("$set", new Document("scope", header(next))));
                } finally { proceed.countDown(); }
                assertThatThrownBy(() -> result.get(15, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class)
                        .satisfies(error -> fenced(error.getCause()));
                assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).countDocuments()).isZero();
                assertThat(SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).find().first()).containsEntry("scope", header(next))
                        .containsEntry("runtimeWriteFence", 0L);
                assertThat(identity(f.database()).getLong("claimAdmissionFence")).isZero();
            }
        });
    }

    @Test void completedAdmissionIsIncludedInAControlledReplacementAndKeepsClaimHistory() {
        withBusiness(f -> {
            var claims = f.domain().workloadClaims(SCOPE);
            var first = claims.acquirePipeline(PIPELINE, OWNER, 1, TTL).claim();
            var next = new PipelineIntentScope("cluster-a", 1, "incarnation-a", "orders", 2);
            try (var session = f.admin().startSession()) {
                session.withTransaction(() -> {
                    SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).updateOne(session, new Document("_id", "orders"), new Document("$set", new Document("scope", header(next))));
                    SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).updateOne(session,
                            WorkloadClaimDocuments.live(io.tapstate.spi.store.WorkloadClaimFence.from(first)),
                            List.of(new Document("$set", new Document("leaseUntil", "$$NOW"))));
                    return null;
                }, com.mongodb.TransactionOptions.builder().readConcern(com.mongodb.ReadConcern.SNAPSHOT)
                        .writeConcern(com.mongodb.WriteConcern.MAJORITY.withJournal(true)).build());
            }
            assertThat(claims.renew(first, TTL)).isEmpty();
            var second = claims.acquirePipeline(next, OWNER, 1, TTL).claim();
            assertThat(second.claimGeneration()).isEqualTo(first.claimGeneration() + 1);
            assertThat(claims.release(first)).isFalse();
            assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).countDocuments()).isEqualTo(1);
        });
    }

    @Test void missingKnownCheckpointRefusesAdmissionWithoutRepairOrCounterCommit() {
        withBusiness(f -> {
            SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).deleteOne(new Document("_id", "orders"));
            var before = snapshot(f.database());
            assertThatThrownBy(() -> f.domain().workloadClaims(SCOPE).acquirePipeline(PIPELINE, OWNER, 1, TTL))
                    .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            assertThat(snapshot(f.database())).isEqualTo(before);
        });
    }

    @Test void wrongBusinessCoordinatesNeverEnterPipelineAdmission() {
        withBusiness(f -> {
            var before = snapshot(f.database());
            for (var wrong : List.of(new PipelineIntentScope("another", 1, "incarnation-a", "orders", 1),
                    new PipelineIntentScope("cluster-a", 2, "incarnation-a", "orders", 1),
                    new PipelineIntentScope("cluster-a", 1, "another-incarnation", "orders", 1)))
                assertThatThrownBy(() -> f.domain().workloadClaims(SCOPE).acquirePipeline(wrong, OWNER, 1, TTL))
                        .satisfies(MongoCacheWorkloadClaimStoreIT::fenced);
            assertThat(snapshot(f.database())).isEqualTo(before);
        });
    }

    @Test void exhaustedPipelineWriteFenceRefusesAdmissionWithoutOverflow() {
        withBusiness(f -> {
            var claims = f.domain().workloadClaims(SCOPE);
            var original = claims.acquirePipeline(PIPELINE, OWNER, 1, TTL).claim();
            assertThat(claims.release(original)).isTrue();
            SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).updateOne(new Document("_id", "orders"),
                    new Document("$set", new Document("runtimeWriteFence", Long.MAX_VALUE - 1)));
            var before = snapshot(f.database());
            assertThatThrownBy(() -> f.domain().workloadClaims(SCOPE).acquirePipeline(PIPELINE, OWNER, 1, TTL))
                    .satisfies(MongoCacheWorkloadClaimStoreIT::fenced);
            assertThat(snapshot(f.database())).isEqualTo(before);
        });
    }

    @Test void aStoreWithoutScopeAdmissionCannotFallBackToItsBareIdAcquire() {
        withBusiness(f -> {
            var before = snapshot(f.database());
            var raw = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            assertThatThrownBy(() -> raw.acquirePipeline(PIPELINE, OWNER, 1, TTL)).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
            assertThatThrownBy(() -> raw.readPipeline(PIPELINE)).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
            assertThat(snapshot(f.database())).isEqualTo(before);
        });
    }

    @Test void fractionalClaimGenerationIsUnreadableInsteadOfBeingTruncated() {
        withBusiness(f -> {
            var claims = f.domain().workloadClaims(SCOPE); claims.acquirePipeline(PIPELINE, OWNER, 1, TTL);
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("claimGeneration", 1.75d)));
            var before = snapshot(f.database());
            assertThatThrownBy(() -> claims.read(KEY)).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            assertThat(snapshot(f.database())).isEqualTo(before);
        });
    }

    @Test void aKnownMissingGenerationCannotBeReconstructedBySameOwnerAcquisition() {
        withBusiness(f -> {
            var claims = f.domain().workloadClaims(SCOPE); claims.acquirePipeline(PIPELINE, OWNER, 1, TTL);
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).updateOne(new Document(), new Document("$unset", new Document("claimGeneration", 1)));
            var before = snapshot(f.database());
            assertThatThrownBy(() -> claims.acquirePipeline(PIPELINE, OWNER, 1, TTL)).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            assertThat(snapshot(f.database())).isEqualTo(before);
        });
    }

    @Test void aMalformedClaimTypeUsesACodedDiagnosticWithoutItsRawValueOrCause() {
        withBusiness(f -> {
            var claims = f.domain().workloadClaims(SCOPE); claims.acquirePipeline(PIPELINE, OWNER, 1, TTL);
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("resourceType", "private-sentinel-type")));
            var before = snapshot(f.database());
            assertThatThrownBy(() -> claims.read(KEY)).isInstanceOfSatisfying(TapstateException.class, error -> {
                assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                assertThat(error.getCause()).isNull(); assertThat(error.getMessage()).doesNotContain("private-sentinel-type");
            });
            assertThat(snapshot(f.database())).isEqualTo(before);
        });
    }

    @Test void lossOfAKnownPipelineClaimCannotRestartItsGenerationCounters() {
        withBusiness(f -> {
            var claims = f.domain().workloadClaims(SCOPE);
            var original = claims.acquirePipeline(PIPELINE, OWNER, 1, TTL).claim();
            claims.advanceExecution(original, 1, Set.of("node-a")).orElseThrow();
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).deleteMany(new Document());
            var before = snapshot(f.database());
            assertThatThrownBy(() -> claims.acquirePipeline(PIPELINE, OWNER, 1, TTL)).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            assertThat(snapshot(f.database())).isEqualTo(before);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"contextExecutionGeneration", "executionClaimGeneration", "executionNodeIds", "failureClaimGeneration", "failureAfterMemberLoss"})
    void publishedExecutionContextCannotBeDefaultedOrRepairedWhenAFieldIsLost(String field) {
        withBusiness(f -> {
            var claims = f.domain().workloadClaims(SCOPE);
            var original = claims.acquirePipeline(PIPELINE, OWNER, 1, TTL).claim();
            var run = claims.advanceExecution(original, 1, Set.of("node-a")).orElseThrow();
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).updateOne(new Document(), new Document("$unset", new Document(field, 1)));
            var before = snapshot(f.database());
            List<org.assertj.core.api.ThrowableAssert.ThrowingCallable> operations = List.of(
                    () -> claims.read(KEY), () -> claims.readAll(List.of(KEY)), () -> claims.renew(run, TTL),
                    () -> claims.release(run), () -> claims.advanceExecution(run, 1, Set.of("node-a")),
                    () -> claims.recordExecutionFailure(run, false), () -> claims.acquirePipeline(PIPELINE, OWNER, 1, TTL));
            for (var operation : operations) {
                assertThatThrownBy(operation).isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE); assertThat(error.getCause()).isNull();
                });
                assertThat(snapshot(f.database())).isEqualTo(before);
            }
        });
    }

    @Test void primitiveTypesAndCanonicalClaimKeysCannotBeCoercedIntoValidFacts() {
        withBusiness(f -> {
            var claims = f.domain().workloadClaims(SCOPE);
            var first = claims.acquirePipeline(PIPELINE, OWNER, 1, TTL).claim();
            claims.advanceExecution(first, 1, Set.of("node-a")).orElseThrow();
            var table = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database());
            var original = table.find().first();
            for (var mutation : List.of(new Document("claimGeneration", 1), new Document("executionGeneration", 1),
                    new Document("topologyRevision", -1L), new Document("leaseUntil", "private-sentinel-time"),
                    new Document("ownerBootId", null), new Document("resourceId", "another"),
                    new Document("executionNodeIds", List.of("node-a", "node-a")), new Document("contextExecutionGeneration", 1.25d),
                    new Document("failureAfterMemberLoss", "bad"))) {
                table.updateOne(new Document(), new Document("$set", mutation));
                var before = snapshot(f.database());
                for (org.assertj.core.api.ThrowableAssert.ThrowingCallable read : List.<org.assertj.core.api.ThrowableAssert.ThrowingCallable>of(
                        () -> claims.read(KEY), () -> claims.readAll(List.of(KEY))))
                    assertThatThrownBy(read).isInstanceOfSatisfying(TapstateException.class, error -> {
                        assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE); assertThat(error.getCause()).isNull();
                    });
                assertThat(snapshot(f.database())).isEqualTo(before);
                table.replaceOne(new Document(), original);
            }
        });
    }

    @Test void completeExplicitUnknownContextIsSafeButPartialInitialContextIsNotDefaulted() {
        withBusiness(f -> {
            var claims = f.domain().workloadClaims(SCOPE);
            var first = claims.acquirePipeline(PIPELINE, OWNER, 1, TTL).claim();
            assertThat(first.executionGeneration()).isZero();
            assertThat(claims.read(KEY).orElseThrow().claim()).isEqualTo(first);
            var table = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database());
            var initial = table.find().first();
            table.updateOne(new Document(), new Document("$set", new Document("contextExecutionGeneration", 0L)));
            assertThatThrownBy(() -> claims.read(KEY)).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            table.replaceOne(new Document(), initial);
            claims.advanceExecution(first, 1, Set.of("node-a")).orElseThrow();
            table.updateOne(new Document(), new Document("$set", new Document("contextExecutionGeneration", 0L).append("executionClaimGeneration", 0L)
                    .append("executionNodeIds", List.of()).append("failureClaimGeneration", 0L).append("failureAfterMemberLoss", false)));
            var unknown = claims.read(KEY).orElseThrow().claim();
            assertThat(unknown.contextExecutionGeneration()).isZero();
            assertThat(claims.recordExecutionFailure(unknown, false)).isEmpty();
            var next = claims.advanceExecution(unknown, 1, Set.of("node-a")).orElseThrow();
            assertThat(next.contextExecutionGeneration()).isEqualTo(next.executionGeneration());
        });
    }

    @Test void pipelineAdmissionDoesNotDependOnTheRebuildableObservationCollection() {
        withBusiness(f -> {
            SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database()).drop();
            var claim = f.domain().workloadClaims(SCOPE).acquirePipeline(PIPELINE, OWNER, 1, TTL);
            assertThat(claim.acquired()).isTrue();
            assertThat(f.domain().pipelineStates(PIPELINE).read("orders")).isPresent();
            assertThatThrownBy(() -> f.domain().pipelineObservations(PIPELINE).read("orders"))
                    .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            assertThat(f.database().listCollectionNames().into(new ArrayList<>())).doesNotContain("pipeline_observation");
        });
    }

    @Test void pipelineScopeSurvivesTheOriginalTenureAndAuthorizationReadsAreReadOnly() {
        withBusiness(f -> {
            var claims = f.domain().workloadClaims(SCOPE);
            var empty = snapshot(f.database());
            assertThat(claims.readPipeline(PIPELINE)).isEmpty();
            assertThat(snapshot(f.database())).isEqualTo(empty);
            var first = claims.acquirePipeline(PIPELINE, OWNER, 1, TTL).claim();
            assertThat(claimScope(f.database())).isEqualTo(header(PIPELINE));
            var run = claims.advanceExecution(first, 1, Set.of("node-a")).orElseThrow();
            var failed = claims.recordExecutionFailure(run, false).orElseThrow();
            var renewed = claims.renew(failed, TTL).orElseThrow();
            var before = snapshot(f.database());
            assertThat(claims.readPipeline(PIPELINE).orElseThrow().claim()).isEqualTo(renewed);
            assertThat(claims.readPipeline(PIPELINE).orElseThrow().leased()).isTrue();
            assertThat(snapshot(f.database())).isEqualTo(before);
            assertThat(claims.release(renewed)).isTrue();
            assertThat(claims.readPipeline(PIPELINE).orElseThrow().leased()).isFalse();
            assertThat(claimScope(f.database())).isEqualTo(header(PIPELINE));
        });
    }

    @Test void aNewStateScopeTakesANewTenureAndDoesNotInheritItsOldRunContext() {
        withBusiness(f -> {
            var claims = f.domain().workloadClaims(SCOPE);
            var run = claims.advanceExecution(claims.acquirePipeline(PIPELINE, OWNER, 1, TTL).claim(), 1, Set.of("node-a")).orElseThrow();
            var failed = claims.recordExecutionFailure(run, false).orElseThrow();
            assertThat(claims.release(failed)).isTrue();
            var next = nextScope();
            SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).updateOne(new Document(), new Document("$set", new Document("scope", header(next))));
            var taken = claims.acquirePipeline(next, OWNER, 1, TTL).claim();
            assertThat(taken.claimGeneration()).isEqualTo(run.claimGeneration() + 1);
            assertThat(taken.executionGeneration()).isEqualTo(run.executionGeneration());
            assertThat(taken.contextExecutionGeneration()).isZero();
            assertThat(taken.executionClaimGeneration()).isZero(); assertThat(taken.executionNodeIds()).isEmpty();
            assertThat(taken.failureClaimGeneration()).isZero(); assertThat(taken.failureAfterMemberLoss()).isFalse();
            assertThat(claimScope(f.database())).isEqualTo(header(next));
            assertThat(claims.readPipeline(next).orElseThrow().claim()).isEqualTo(taken);
            assertThatThrownBy(() -> claims.readPipeline(PIPELINE)).satisfies(MongoCacheWorkloadClaimStoreIT::fenced);
            assertThat(claims.release(failed)).isFalse();
            var advanced = claims.advanceExecution(taken, 1, Set.of("node-a")).orElseThrow();
            assertThat(advanced.contextExecutionGeneration()).isEqualTo(advanced.executionGeneration());
            assertThat(advanced.executionClaimGeneration()).isEqualTo(taken.claimGeneration());
            assertThat(claimScope(f.database())).isEqualTo(header(next));
        });
    }

    @Test void anUnretiredLiveTenureCannotMoveToAnotherPhysicalScope() {
        withBusiness(f -> {
            var claims = f.domain().workloadClaims(SCOPE);
            var held = claims.acquirePipeline(PIPELINE, OWNER, 1, TTL).claim();
            var next = nextScope();
            SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).updateOne(new Document(), new Document("$set", new Document("scope", header(next))));
            var before = snapshot(f.database());
            assertThatThrownBy(() -> claims.acquirePipeline(next, OWNER, 1, TTL)).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
            assertThatThrownBy(() -> claims.renew(held, TTL)).satisfies(MongoCacheWorkloadClaimStoreIT::fenced);
            assertThatThrownBy(() -> claims.advanceExecution(held, 1, Set.of("node-a"))).satisfies(MongoCacheWorkloadClaimStoreIT::fenced);
            assertThat(snapshot(f.database())).isEqualTo(before);
            assertThat(claimScope(f.database())).isEqualTo(header(PIPELINE));
            assertThat(claims.release(held)).isTrue();
            assertThat(claims.acquirePipeline(next, OWNER, 1, TTL).claim().claimGeneration()).isEqualTo(held.claimGeneration() + 1);
        });
    }

    @Test void aKnownMissingScopeCannotBeAdoptedOrRepairedByAnyOrdinaryClaimOperation() {
        withBusiness(f -> {
            var claims = f.domain().workloadClaims(SCOPE);
            var held = claims.acquirePipeline(PIPELINE, OWNER, 1, TTL).claim();
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).updateOne(new Document(), new Document("$unset", new Document("pipelineScope", 1)));
            var before = snapshot(f.database());
            List<org.assertj.core.api.ThrowableAssert.ThrowingCallable> operations = List.of(
                    () -> claims.read(KEY), () -> claims.readAll(List.of(KEY)), () -> claims.readPipeline(PIPELINE),
                    () -> claims.renew(held, TTL), () -> claims.release(held), () -> claims.advanceExecution(held, 1, Set.of("node-a")),
                    () -> claims.recordExecutionFailure(held, false), () -> claims.acquirePipeline(PIPELINE, OWNER, 1, TTL));
            for (var operation : operations) {
                assertThatThrownBy(operation).isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE); assertThat(error.getCause()).isNull();
                });
                assertThat(snapshot(f.database())).isEqualTo(before);
            }
        });
    }

    @Test void scopeTypesCoordinatesAndUnknownFieldsCannotBeDefaulted() {
        withBusiness(f -> {
            var claims = f.domain().workloadClaims(SCOPE); claims.acquirePipeline(PIPELINE, OWNER, 1, TTL);
            var original = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).find().first();
            for (var change : List.of(new Document("pipelineScope.bindingVersion", 1), new Document("pipelineScope.stateGeneration", 1),
                    new Document("pipelineScope.clusterId", "another"), new Document("pipelineScope.cacheIncarnation", "private-sentinel-incarnation"),
                    new Document("pipelineScope.pipelineId", "another"), new Document("pipelineScope.extra", "private-sentinel-field"))) {
                SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).updateOne(new Document(), new Document("$set", change));
                var before = snapshot(f.database());
                assertThatThrownBy(() -> claims.readPipeline(PIPELINE)).isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE); assertThat(error.getCause()).isNull();
                    assertThat(error.getMessage()).doesNotContain("private-sentinel");
                });
                assertThat(snapshot(f.database())).isEqualTo(before);
                SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).replaceOne(new Document(), original);
            }
        });
    }

    @Test void fixedScopeAuthorizationReadsDoNotDependOnTheProjectionCollection() {
        withBusiness(f -> {
            var claims = f.domain().workloadClaims(SCOPE);
            var held = claims.acquirePipeline(PIPELINE, OWNER, 1, TTL).claim();
            SystemCollections.CACHE_PIPELINE_OBSERVATION.on(f.database()).drop();
            var before = snapshot(f.database());
            assertThat(claims.readPipeline(PIPELINE).orElseThrow().claim()).isEqualTo(held);
            assertThat(snapshot(f.database())).isEqualTo(before);
            assertThat(f.database().listCollectionNames().into(new ArrayList<>())).doesNotContain("pipeline_observation");
        });
    }

    @Test void matchingNumericTokensCannotMakeAnOldPhysicalScopeCurrentAgain() {
        withBusiness(f -> {
            var claims = f.domain().workloadClaims(SCOPE);
            var run = claims.advanceExecution(claims.acquirePipeline(PIPELINE, OWNER, 1, TTL).claim(), 1, Set.of("node-a")).orElseThrow();
            assertThat(claims.release(run)).isTrue();
            var next = nextScope();
            SystemCollections.CACHE_PIPELINE_STATE.on(f.database()).updateOne(new Document(), new Document("$set", new Document("scope", header(next))));
            claims.acquirePipeline(next, OWNER, 1, TTL);
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).updateOne(new Document(), new Document("$set",
                    new Document("claimGeneration", run.claimGeneration()).append("executionGeneration", run.executionGeneration())));
            var sameNumbers = claims.readPipeline(next).orElseThrow().claim();
            assertThat(sameNumbers.claimGeneration()).isEqualTo(run.claimGeneration());
            assertThat(sameNumbers.executionGeneration()).isEqualTo(run.executionGeneration());
            assertThatThrownBy(() -> claims.readPipeline(PIPELINE)).satisfies(MongoCacheWorkloadClaimStoreIT::fenced);
        });
    }

    @Test void aLaterRetainedScopeCannotBeOverwrittenByAnOlderAdmission() {
        withBusiness(f -> {
            var claims = f.domain().workloadClaims(SCOPE); claims.acquirePipeline(PIPELINE, OWNER, 1, TTL);
            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("pipelineScope", header(nextScope()))));
            var before = snapshot(f.database());
            assertThatThrownBy(() -> claims.acquirePipeline(PIPELINE, OWNER, 1, TTL)).satisfies(MongoCacheWorkloadClaimStoreIT::fenced);
            assertThatThrownBy(() -> claims.readPipeline(PIPELINE)).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
            assertThat(snapshot(f.database())).isEqualTo(before);
        });
    }

    private static PipelineIntentScope nextScope() { return new PipelineIntentScope("cluster-a", 1, "incarnation-a", "orders", 2); }
    private static Document claimScope(MongoDatabase database) { return SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find().first().get("pipelineScope", Document.class); }

    private static Document header(PipelineIntentScope scope) {
        return new Document("clusterId", scope.clusterId()).append("bindingVersion", scope.bindingVersion()).append("cacheIncarnation", scope.cacheIncarnation())
                .append("pipelineId", scope.pipelineId()).append("stateGeneration", scope.stateGeneration());
    }
    private static MongoClient client(CommandListener listener) {
        return MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                .addCommandListener(listener).build());
    }
    private static void await(CountDownLatch latch) {
        try { if (!latch.await(15, TimeUnit.SECONDS)) throw new AssertionError("test barrier did not open"); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
    }
    private static Document identity(MongoDatabase database) { return SystemCollections.CACHE_IDENTITY.on(database).find().first(); }
    private static List<Document> claimBodies(MongoDatabase database) {
        var rows = SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find().sort(new Document("_id", 1)).into(new ArrayList<>());
        rows.forEach(row -> row.remove("leaseUntil")); return rows;
    }
    private static Map<String, List<Document>> snapshot(MongoDatabase database) {
        var facts = new java.util.TreeMap<String, List<Document>>();
        for (var table : List.of(SystemCollections.CACHE_IDENTITY, SystemCollections.CACHE_SCHEMA_MIGRATIONS, SystemCollections.CACHE_WORKLOAD_CLAIMS,
                SystemCollections.CACHE_PIPELINE_STATE, SystemCollections.CACHE_PIPELINE_OBSERVATION))
            facts.put(table.collectionName(), table.on(database).find().sort(new Document("_id", 1)).into(new ArrayList<>()));
        return facts;
    }
    private static void fenced(Throwable failure) { assertThat(failure).isInstanceOfSatisfying(TapstateException.class,
            error -> assertThat(error.code()).isEqualTo(IoError.CACHE_BUSINESS_FENCED)); }
    private static void withBusiness(Scenario scenario) {
        String databaseName = "cache_claims_" + UUID.randomUUID().toString().replace("-", "");
        var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, MONGO.getReplicaSetUrl(databaseName), databaseName, null);
        try (var admin = MongoClients.create(MONGO.getReplicaSetUrl());
             var domain = MongoCacheDomain.connect(binding, Duration.ofSeconds(5), "cache-claims-test")) {
            var database = admin.getDatabase(databaseName);
            try {
                domain.prepareAuth(AUTH, "auth-a", ACTIVE);
                try (var session = admin.startSession()) {
                    for (var table : List.of(SystemCollections.CACHE_WORKLOAD_CLAIMS, SystemCollections.CACHE_PIPELINE_STATE,
                            SystemCollections.CACHE_PIPELINE_OBSERVATION)) table.prepareCollectionOn(database, session);
                }
                SystemCollections.CACHE_PIPELINE_STATE.on(database).insertOne(new Document("_id", "orders").append("scope", header(PIPELINE))
                        .append("stateJson", "NEW").append("epoch", 0L).append("touchMillis", 0L).append("runtimeWriteFence", 0L));
                SystemCollections.CACHE_IDENTITY.on(database).updateOne(new Document("_id", "cluster"), new Document("$set",
                        new Document("businessPhase", "READY").append("businessPreparationId", "business-a").append("claimAdmissionFence", 0L)));
                SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).insertOne(new Document("_id", "business").append("installedVersion", 1)
                        .append("phase", "READY").append("epoch", 1L).append("operationId", "business-a"));
                scenario.run(new Fixture(admin, database, domain, binding));
            } finally { database.drop(); }
        } catch (RuntimeException | Error failure) { throw failure; }
        catch (Exception failure) { throw new AssertionError(failure); }
    }
    private record Fixture(MongoClient admin, MongoDatabase database, MongoCacheDomain domain, StorageBinding.Connection binding) {}
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
