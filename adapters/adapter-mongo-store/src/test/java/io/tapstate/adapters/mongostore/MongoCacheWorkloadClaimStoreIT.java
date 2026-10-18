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
import io.tapstate.spi.store.StorageBinding;
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
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final Runnable ACTIVE = () -> {};

    @Test void normalTransitionsKeepOneAdmissionAndIgnoreIndependentAuthenticationReset() {
        withBusiness(f -> {
            var claims = f.domain().workloadClaims(SCOPE);
            var original = claims.acquire(KEY, OWNER, 1, TTL).claim();
            var run = claims.advanceExecution(original, 1, Set.of("node-a")).orElseThrow();
            var renewed = claims.renew(run, TTL).orElseThrow();
            var failed = claims.recordExecutionFailure(renewed, false).orElseThrow();
            assertThat(claims.read(KEY).orElseThrow().claim()).isEqualTo(failed);
            assertThat(claims.readAll(List.of(KEY))).containsKey(KEY);
            assertThat(identity(f.database()).getLong("claimAdmissionFence")).isEqualTo(1);
            f.domain().advanceAuthGeneration(AUTH, 2, "auth-reset", ACTIVE);
            assertThat(claims.read(KEY).orElseThrow().claim()).isEqualTo(failed);
            assertThat(claims.release(failed)).isTrue();
            var next = claims.acquire(KEY, OWNER, 1, TTL).claim();
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
                var a = workers.submit(() -> { start.await(); return one.acquire(KEY, OWNER, 1, TTL); });
                var b = workers.submit(() -> { start.await(); return two.acquire(KEY, new WorkloadOwner("node-b", "boot-b"), 1, TTL); });
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
            var pipeline = claims.advanceExecution(claims.acquire(KEY, OWNER, 1, TTL).claim(), 1, Set.of("node-a")).orElseThrow();
            var nodeKey = new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.NODE_SESSION, "node-a");
            var captureKey = new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.CAPTURE, "tail-a");
            claims.acquire(nodeKey, OWNER, 0, TTL); claims.acquire(captureKey, OWNER, 1, TTL);
            var before = claimBodies(f.database());
            f.domain().retireBusiness(SCOPE, "retire-a", ACTIVE);
            assertThat(claimBodies(f.database())).isEqualTo(before);
            var raw = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            for (var key : List.of(KEY, nodeKey, captureKey)) assertThat(raw.read(key).orElseThrow().leased()).isFalse();
            assertThatThrownBy(() -> claims.renew(pipeline, TTL)).satisfies(MongoCacheWorkloadClaimStoreIT::fenced);
            assertThatThrownBy(() -> claims.acquire(KEY, OWNER, 1, TTL)).satisfies(MongoCacheWorkloadClaimStoreIT::fenced);
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
                var result = workers.submit(() -> store.acquire(KEY, OWNER, 1, TTL));
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
            f.domain().workloadClaims(SCOPE).acquire(KEY, OWNER, 1, TTL);
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
            assertThatThrownBy(() -> claims.acquire(KEY, OWNER, 1, TTL)).isInstanceOfSatisfying(TapstateException.class,
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
                try { assertThatThrownBy(() -> claims.acquire(KEY, OWNER, 1, TTL)).satisfies(MongoCacheWorkloadClaimStoreIT::fenced); }
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
            assertThatThrownBy(() -> claims.acquire(KEY, OWNER, 1, TTL)).satisfies(MongoCacheWorkloadClaimStoreIT::fenced);
            assertThat(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()).countDocuments()).isZero();
            assertThat(identity(f.database()).getLong("claimAdmissionFence")).isEqualTo(Long.MAX_VALUE - 1);
            f.domain().retireBusiness(SCOPE, "retire-a", ACTIVE);
            f.domain().retireBusiness(SCOPE, "retire-a", ACTIVE);
            assertThat(identity(f.database())).containsEntry("businessPhase", "RETIRED");
        });
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
        for (var table : List.of(SystemCollections.CACHE_IDENTITY, SystemCollections.CACHE_SCHEMA_MIGRATIONS, SystemCollections.CACHE_WORKLOAD_CLAIMS))
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
                try (var session = admin.startSession()) { SystemCollections.CACHE_WORKLOAD_CLAIMS.prepareCollectionOn(database, session); }
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
