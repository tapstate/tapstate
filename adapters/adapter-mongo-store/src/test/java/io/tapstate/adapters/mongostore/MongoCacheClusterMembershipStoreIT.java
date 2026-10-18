package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.ClusterMembershipStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.WorkloadClaim;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Explicit initial-layout fixtures witness membership history without claiming production bootstrap. */
@RequiresDocker
class MongoCacheClusterMembershipStoreIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
    private static final CacheBusinessScope SCOPE = new CacheBusinessScope("cluster-a", 1, "cache-a");
    private static final CacheAuthScope AUTH = new CacheAuthScope("cluster-a", 1, StorageBinding.Mode.OP, "cache-a", 1);
    private static final WorkloadClaimKey NODE = new WorkloadClaimKey("cluster-a", WorkloadClaimType.NODE_SESSION, "node-a");
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");
    private static final Runnable ACTIVE = () -> {};

    @Test void onlyKnownGenesisIsEmptyAndReadOnlyViewsCannotTurnItIntoACommittedMembership() {
        withFixture(f -> {
            var reader = f.domain.clusterMembership(SCOPE); assertThat(reader.read("cluster-a")).isEmpty(); var before = f.snapshot();
            assertCode(() -> reader.createIfAbsent("cluster-a", Set.of("node-a")), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> reader.compareAndSet("cluster-a", 1, Set.of("node-a")), IoError.CACHE_BUSINESS_FENCED);
            assertThat(f.snapshot()).isEqualTo(before); assertThat(f.member()).containsEntry("phase", "GENESIS").containsEntry("revision", 0L);
        });
    }

    @Test void firstCreationReturnsOneWinnerAndSubsequentCasKeepsTheOriginalExpectedRevision() {
        withFixture(f -> {
            var writer = f.writer(); var first = writer.createIfAbsent("cluster-a", Set.of("node-a"));
            assertThat(first.revision()).isEqualTo(1); assertThat(first.activeNodeIds()).containsExactly("node-a");
            assertThat(writer.createIfAbsent("cluster-a", Set.of("different-proposal"))).isEqualTo(first);
            var next = writer.compareAndSet("cluster-a", 1, Set.of("node-a", "node-b")).orElseThrow();
            assertThat(next.revision()).isEqualTo(2); assertThat(writer.compareAndSet("cluster-a", 1, Set.of("late-proposal"))).isEmpty();
            assertThat(writer.read("cluster-a")).contains(next); assertThat(f.members().countDocuments()).isEqualTo(1);
            assertThat(f.claimRow()).containsEntry("fencedMembershipWrites", 4L); assertThat(f.root()).containsEntry("claimAdmissionFence", 5L);
        });
    }

    @Test void twoIndependentClientsCreateTheSameRevisionOneWithoutCombiningConflictingProposals() {
        withFixture(f -> {
            var start = new CountDownLatch(1);
            try (var second = MongoCacheDomain.connect(f.binding, Duration.ofSeconds(5), "second"); var workers = Executors.newFixedThreadPool(2)) {
                var other = second.clusterMembership(SCOPE, WorkloadClaimFence.from(f.node), ACTIVE);
                var a = workers.submit(() -> { await(start); return f.writer().createIfAbsent("cluster-a", Set.of("node-a")); });
                var b = workers.submit(() -> { await(start); return other.createIfAbsent("cluster-a", Set.of("node-a", "node-b")); }); start.countDown();
                var one = a.get(15, TimeUnit.SECONDS); var two = b.get(15, TimeUnit.SECONDS);
                assertThat(one).isEqualTo(two); assertThat(one.revision()).isEqualTo(1);
                assertThat(one.activeNodeIds()).isIn(Set.of("node-a"), Set.of("node-a", "node-b"));
                assertThat(f.members().countDocuments()).isEqualTo(1); assertThat(f.claimRow()).containsEntry("fencedMembershipWrites", 2L);
            }
        });
    }

    @Test void missingGenesisOrCommittedHistoryIsNeverRecreatedByAnyOrdinaryPort() {
        for (boolean committed : List.of(false, true)) withFixture(f -> {
            if (committed) f.writer().createIfAbsent("cluster-a", Set.of("node-a"));
            f.members().deleteOne(new Document()); var before = f.snapshot();
            assertUnreadable(() -> f.domain.clusterMembership(SCOPE).read("cluster-a"));
            assertUnreadable(() -> f.writer().createIfAbsent("cluster-a", Set.of("node-a")));
            assertUnreadable(() -> f.writer().compareAndSet("cluster-a", 1, Set.of("node-a")));
            assertThat(f.snapshot()).isEqualTo(before); assertThat(f.members().countDocuments()).isZero();
        });
    }

    @Test void anExpiredOrReplacedNodeCannotUseItsOldViewToWriteMembership() {
        withFixture(f -> {
            var old = f.writer(); var claims = f.domain.workloadClaims(SCOPE); claims.release(f.node);
            var next = claims.acquire(NODE, new WorkloadOwner("node-a", "boot-b"), 0, Duration.ofSeconds(30)).claim();
            var before = f.snapshot(); assertCode(() -> old.createIfAbsent("cluster-a", Set.of("node-a")), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
            assertThat(f.domain.clusterMembership(SCOPE, WorkloadClaimFence.from(next), ACTIVE).createIfAbsent("cluster-a", Set.of("node-a")).revision()).isEqualTo(1);
        });
    }

    @Test void wrongClusterRolesOrNodeIdentityCannotOpenAWriteView() {
        withFixture(f -> {
            var before = f.snapshot(); var original = WorkloadClaimFence.from(f.node);
            for (var other : List.of(new WorkloadClaimFence(new WorkloadClaimKey("cluster-a", WorkloadClaimType.CAPTURE, "node-a"), OWNER, 1, 0, 0),
                    new WorkloadClaimFence(new WorkloadClaimKey("other", WorkloadClaimType.NODE_SESSION, "node-a"), OWNER, 1, 0, 0),
                    new WorkloadClaimFence(new WorkloadClaimKey("cluster-a", WorkloadClaimType.NODE_SESSION, "other"), OWNER, 1, 0, 0)))
                assertCode(() -> f.domain.clusterMembership(SCOPE, other, ACTIVE), IoError.WORKLOAD_CLAIM_FENCED);
            assertCode(() -> f.domain.clusterMembership(SCOPE, original, ACTIVE).read("other"), IoError.CACHE_BUSINESS_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void candidateNodesAreCopiedBeforeAnyGuardCallbackCanChangeThem() {
        withFixture(f -> {
            var nodes = new java.util.HashSet<>(Set.of("node-a")); var once = new AtomicBoolean();
            var writer = f.domain.clusterMembership(SCOPE, WorkloadClaimFence.from(f.node), () -> {
                if (once.compareAndSet(false, true)) nodes.add("late-node");
            });
            assertThat(writer.createIfAbsent("cluster-a", nodes).activeNodeIds()).containsExactly("node-a"); assertThat(nodes).contains("late-node");
        });
    }

    @Test void losingTheOriginalGuardAfterMembershipReplacementRollsBackRootNodeAndPayload() {
        withFixture(f -> {
            var before = f.snapshot(); var live = new AtomicBoolean(true); var listener = new CommandListener() { @Override public void commandStarted(CommandStartedEvent e) {
                if (membershipReplace(e)) live.set(false);
            } };
            try (var observed = client(listener)) {
                assertCode(() -> f.writer(observed, () -> { if (!live.get()) throw MongoCacheBusinessIdentity.businessFenced(); })
                        .createIfAbsent("cluster-a", Set.of("node-a")), IoError.CACHE_BUSINESS_FENCED);
            }
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void aRetirementCommittedBeforeRootAdmissionRefusesTheOldMembershipCreator() {
        withFixture(f -> {
            var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1); var once = new AtomicBoolean();
            var listener = new CommandListener() { @Override public void commandStarted(CommandStartedEvent e) {
                if (updates(e, "cache_identity") && once.compareAndSet(false, true)) { paused.countDown(); await(proceed); }
            } };
            try (var observed = client(listener); var worker = Executors.newSingleThreadExecutor()) {
                var result = worker.submit(() -> f.writer(observed, ACTIVE).createIfAbsent("cluster-a", Set.of("node-a")));
                try { assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue(); f.domain.retireBusiness(SCOPE, "retire-membership", ACTIVE); }
                finally { proceed.countDown(); }
                assertThatThrownBy(() -> result.get(15, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class);
                assertThat(f.member()).containsEntry("phase", "GENESIS"); assertThat(f.root()).containsEntry("claimAdmissionFence", 1L);
            }
        });
    }

    @Test void anAdmittedMembershipCommitCanPrecedeTheRetirementThatThenFencesItsView() {
        withFixture(f -> {
            var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1); var once = new AtomicBoolean();
            var listener = new CommandListener() { @Override public void commandStarted(CommandStartedEvent e) {
                if (membershipReplace(e) && once.compareAndSet(false, true)) { paused.countDown(); await(proceed); }
            } };
            try (var observed = client(listener); var workers = Executors.newFixedThreadPool(2)) {
                var result = workers.submit(() -> f.writer(observed, ACTIVE).createIfAbsent("cluster-a", Set.of("node-a")));
                try {
                    assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue(); var retiring = workers.submit(() -> f.domain.retireBusiness(SCOPE, "retire-membership", ACTIVE));
                    proceed.countDown(); assertThat(result.get(15, TimeUnit.SECONDS).revision()).isEqualTo(1); retiring.get(15, TimeUnit.SECONDS);
                } finally { proceed.countDown(); }
                assertThat(f.member()).containsEntry("phase", "COMMITTED").containsEntry("revision", 1L);
                assertCode(() -> f.domain.clusterMembership(SCOPE).read("cluster-a"), IoError.CACHE_BUSINESS_FENCED);
            }
        });
    }

    @Test void aTransientRetryCannotAdoptTheNewRevisionCommittedByAnotherClient() {
        withFixture(f -> {
            f.writer().createIfAbsent("cluster-a", Set.of("node-a")); var failed = new AtomicBoolean(); var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1); var once = new AtomicBoolean();
            var listener = new CommandListener() { @Override public void commandFailed(CommandFailedEvent e) { if (e.getCommandName().equals("findAndModify")) failed.set(true); } };
            try (var observed = client(listener); var worker = Executors.newSingleThreadExecutor()) {
                var writer = f.writer(observed, () -> { if (failed.get() && once.compareAndSet(false, true)) { paused.countDown(); await(proceed); } });
                f.client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                        .append("data", new Document("failCommands", List.of("findAndModify")).append("errorCode", 112).append("errorLabels", List.of("TransientTransactionError"))));
                try {
                    var result = worker.submit(() -> writer.compareAndSet("cluster-a", 1, Set.of("late-proposal")));
                    try { assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue(); assertThat(f.writer().compareAndSet("cluster-a", 1, Set.of("node-a", "node-b"))).isPresent(); }
                    finally { proceed.countDown(); }
                    assertThat(result.get(15, TimeUnit.SECONDS)).isEmpty(); assertThat(f.writer().read("cluster-a").orElseThrow().activeNodeIds()).containsExactlyInAnyOrder("node-a", "node-b");
                } finally { proceed.countDown(); f.client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
            }
        });
    }

    @Test void malformedHistoryAndSaturatedWitnessesDoNotDefaultOrOverflow() {
        withFixture(f -> {
            var writer = f.writer(); writer.createIfAbsent("cluster-a", Set.of("node-a", "node-b")); var saved = f.member();
            for (var mutation : List.of(new Document("revision", 1), new Document("revision", 1.5d), new Document("cacheScope.extra", "private-sentinel"),
                    new Document("activeNodeIds", List.of("node-a", "node-a")), new Document("phase", "private-sentinel"))) {
                f.members().updateOne(new Document(), new Document("$set", mutation)); assertUnreadable(() -> writer.read("cluster-a")); f.members().replaceOne(new Document(), saved);
            }
            f.members().updateOne(new Document(), new Document("$set", new Document("revision", Long.MAX_VALUE - 1))); var before = f.snapshot();
            assertCode(() -> writer.compareAndSet("cluster-a", Long.MAX_VALUE - 1, Set.of("node-a", "node-b")), IoError.WORKLOAD_CLAIM_FENCED); assertThat(f.snapshot()).isEqualTo(before);
            for (Object count : List.of(1, Double.POSITIVE_INFINITY, Long.MAX_VALUE - 1)) {
                f.claims().updateOne(new Document(), new Document("$set", new Document("fencedMembershipWrites", count))); before = f.snapshot();
                assertCode(() -> writer.createIfAbsent("cluster-a", Set.of("node-a")), IoError.WORKLOAD_CLAIM_FENCED); assertThat(f.snapshot()).isEqualTo(before);
            }
        });
    }

    @Test void authOnlyResetKeepsMembershipButClosedAndMissingStructuresDoNotRepairHistory() {
        withFixture(f -> {
            var writer = f.writer(); var expected = writer.createIfAbsent("cluster-a", Set.of("node-a")); f.domain.advanceAuthGeneration(AUTH, 2, "auth-reset", ACTIVE);
            assertThat(writer.read("cluster-a")).contains(expected);
            f.members().drop(); assertUnreadable(() -> writer.read("cluster-a")); assertUnreadable(() -> writer.createIfAbsent("cluster-a", Set.of("node-a")));
            assertThat(f.database.listCollectionNames().into(new ArrayList<>())).doesNotContain("cluster_membership");
            f.domain.close(); assertCode(() -> writer.read("cluster-a"), IoError.CACHE_BUSINESS_FENCED);
        });
    }

    @Test void aRetainedRetirementFactCannotBeReinterpretedAsAnUnusedBusinessDomain() {
        withAuthFixture((client, database, domain, binding) -> {
            try (var session = client.startSession()) { session.withTransaction(() -> {
                SystemCollections.CACHE_CLUSTER_MEMBERSHIP.prepareCollectionOn(database, session); return null;
            }, TransactionOptions.builder().readConcern(ReadConcern.LOCAL).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build()); }
            SystemCollections.CACHE_IDENTITY.on(database).updateOne(new Document("_id", "cluster"), new Document("$set", new Document("businessRetirementId", "retired-operation")));
            Document before = SystemCollections.CACHE_IDENTITY.on(database).find().first();
            try (var session = client.startSession()) {
                assertUnreadable(() -> session.withTransaction(() -> { MongoCacheClusterMembershipStore.prepareInitial(session, database, SCOPE); return null; }));
            }
            assertThat(SystemCollections.CACHE_CLUSTER_MEMBERSHIP.on(database).countDocuments()).isZero();
            assertThat(SystemCollections.CACHE_IDENTITY.on(database).find().first()).isEqualTo(before);
        });
    }

    @Test void initialMarkerAndReadyFixtureRollBackTogetherAndReadyNeverAllowsReallocatingGenesis() {
        withAuthFixture((client, database, domain, binding) -> {
            var stopped = new IllegalStateException("fixture stops after initial marker");
            assertThatThrownBy(() -> prepareBusiness(client, database, () -> { throw stopped; })).isSameAs(stopped);
            assertThat(SystemCollections.CACHE_IDENTITY.on(database).find().first()).containsEntry("businessPhase", "ABSENT");
            assertThat(database.listCollectionNames().into(new ArrayList<>())).doesNotContain("cluster_membership", "workload_claims");
            prepareBusiness(client, database, ACTIVE);
            SystemCollections.CACHE_CLUSTER_MEMBERSHIP.on(database).deleteOne(new Document());
            try (var session = client.startSession()) {
                assertUnreadable(() -> session.withTransaction(() -> { MongoCacheClusterMembershipStore.prepareInitial(session, database, SCOPE); return null; }));
            }
            assertThat(SystemCollections.CACHE_CLUSTER_MEMBERSHIP.on(database).countDocuments()).isZero();
        });
    }

    private static boolean membershipReplace(CommandStartedEvent e) { return e.getCommandName().equals("findAndModify") && e.getCommand().getString("findAndModify").getValue().equals("cluster_membership"); }
    private static boolean updates(CommandStartedEvent e, String table) { return e.getCommandName().equals("update") && e.getCommand().getString("update").getValue().equals(table); }
    private static MongoClient client(CommandListener listener) { return MongoClients.create(MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(listener).build()); }
    private static void await(CountDownLatch latch) { try { if (!latch.await(15, TimeUnit.SECONDS)) throw new AssertionError("test barrier did not open"); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); } }
    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, IoError code) { assertThatThrownBy(action)
            .isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(code)); }
    private static void assertUnreadable(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) { assertThatThrownBy(action)
            .isInstanceOfSatisfying(TapstateException.class, e -> { assertThat(e.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                assertThat(e.getCause()).isNull(); assertThat(e.args().toString()).doesNotContain("private-sentinel"); }); }
    private static void prepareBusiness(MongoClient client, MongoDatabase database, Runnable afterMarker) {
        try (var session = client.startSession()) { session.withTransaction(() -> {
            SystemCollections.CACHE_WORKLOAD_CLAIMS.prepareCollectionOn(database, session); SystemCollections.CACHE_CLUSTER_MEMBERSHIP.prepareCollectionOn(database, session);
            MongoCacheClusterMembershipStore.prepareInitial(session, database, SCOPE); afterMarker.run();
            SystemCollections.CACHE_IDENTITY.on(database).updateOne(session, new Document("_id", "cluster"), new Document("$set", new Document("businessPhase", "READY")
                    .append("businessPreparationId", "business-a").append("claimAdmissionFence", 0L)));
            SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).insertOne(session, new Document("_id", "business").append("installedVersion", 1)
                    .append("phase", "READY").append("epoch", 1L).append("operationId", "business-a")); return null;
        }, TransactionOptions.builder().readConcern(ReadConcern.LOCAL).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build()); }
    }
    private static void withFixture(Scenario scenario) {
        withAuthFixture((client, database, domain, binding) -> {
            prepareBusiness(client, database, ACTIVE); var claim = domain.workloadClaims(SCOPE).acquire(NODE, OWNER, 0, Duration.ofSeconds(30)).claim();
            scenario.run(new Fixture(client, database, domain, binding, claim));
        });
    }
    private static void withAuthFixture(AuthScenario scenario) {
        String name = "cache_membership_" + UUID.randomUUID().toString().replace("-", "");
        var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, MONGO.getReplicaSetUrl(name), name, null);
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl()); var domain = MongoCacheDomain.connect(binding, Duration.ofSeconds(5), "membership-test")) {
            var database = client.getDatabase(name);
            try { domain.prepareAuth(AUTH, "auth-a", ACTIVE); scenario.run(client, database, domain, binding); }
            finally { database.drop(); }
        } catch (RuntimeException | Error e) { throw e; }
        catch (Exception e) { throw new AssertionError(e); }
    }
    private record Fixture(MongoClient client, MongoDatabase database, MongoCacheDomain domain, StorageBinding.Connection binding, WorkloadClaim node) {
        ClusterMembershipStore writer() { return domain.clusterMembership(SCOPE, WorkloadClaimFence.from(node), ACTIVE); }
        ClusterMembershipStore writer(MongoClient original, Runnable guard) { return new MongoCacheClusterMembershipStore(original,
                original.getDatabase(database.getName()), SCOPE, WorkloadClaimFence.from(node), guard); }
        com.mongodb.client.MongoCollection<Document> members() { return SystemCollections.CACHE_CLUSTER_MEMBERSHIP.on(database); }
        com.mongodb.client.MongoCollection<Document> claims() { return SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database); }
        Document member() { return members().find().first(); }
        Document root() { return SystemCollections.CACHE_IDENTITY.on(database).find().first(); }
        Document claimRow() { return claims().find().first(); }
        Map<String, List<Document>> snapshot() {
            var out = new java.util.TreeMap<String, List<Document>>();
            for (var table : List.of(SystemCollections.CACHE_CLUSTER_MEMBERSHIP, SystemCollections.CACHE_WORKLOAD_CLAIMS, SystemCollections.CACHE_IDENTITY))
                out.put(table.collectionName(), table.on(database).find().sort(new Document("_id", 1)).into(new ArrayList<>()));
            return out;
        }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
    @FunctionalInterface private interface AuthScenario { void run(MongoClient client, MongoDatabase database, MongoCacheDomain domain, StorageBinding.Connection binding) throws Exception; }
}
