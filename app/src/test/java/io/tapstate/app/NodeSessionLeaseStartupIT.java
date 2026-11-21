package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationBootstrap;
import io.tapstate.adapters.mongostore.MongoConnection;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.DeadlineWorkloadClaimStore;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimRenewalException;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real server leases adopted after startup work, without restarting a member or changing its TTL. */
@RequiresDocker
class NodeSessionLeaseStartupIT {
    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    @BeforeAll
    static void recordOwnedFixture() {
        System.out.printf("Owned lease Mongo fixture [container_id=%s, session_id=%s, jvm_pid=%d]%n",
                MONGO.getContainerId(), MONGO.getContainerInfo().getConfig().getLabels().get("org.testcontainers.sessionId"),
                ProcessHandle.current().pid());
    }

    @Test
    void aLateAdoptedLiveClaimIsRenewedBeforeItsOriginalProofExpires() throws Exception {
        try (Fixture fixture = new Fixture()) {
            Duration ttl = Duration.ofSeconds(8);
            Duration interval = Duration.ofSeconds(6);
            Acquired acquired = fixture.acquire("boot-live", ttl);
            long proofDeadline = acquired.askedAt + ttl.toNanos();
            long adoptionAt = proofDeadline - Duration.ofSeconds(3).toNanos();
            TimeUnit.NANOSECONDS.sleep(Math.max(0, adoptionAt - System.nanoTime()));
            WorkloadClaimReading before = fixture.claims.read(acquired.claim.key()).orElseThrow();
            assertThat(before.leased()).isTrue();
            assertThat(before.leaseRemaining()).isPositive().isLessThan(interval);
            assertThat(proofDeadline - System.nanoTime()).isPositive().isLessThan(interval.toNanos());
            TrackingClaims tracked = new TrackingClaims(fixture.claims);
            AtomicInteger stopped = new AtomicInteger();
            try (NodeSessionLease lease = new NodeSessionLease(tracked, acquired.claim, acquired.askedAt,
                    ttl, interval, stopped::incrementAndGet)) {
                boolean renewed = tracked.renewed.await(
                        Math.max(0, proofDeadline - System.nanoTime()), TimeUnit.NANOSECONDS);
                assertThat(renewed).as("a still-live late-adopted claim must renew before its original proof expires").isTrue();
                WorkloadClaimReading after = fixture.claims.read(acquired.claim.key()).orElseThrow();
                assertThat(after.leased()).isTrue();
                assertThat(after.claim().leaseUntil()).isAfter(acquired.claim.leaseUntil());
                assertThat(after.claim().owner()).isEqualTo(acquired.claim.owner());
                assertThat(after.claim().claimGeneration()).isEqualTo(acquired.claim.claimGeneration());
                assertThat(stopped).hasValue(0);
            }
        }
    }

    @Test
    void aClaimExpiredBeforeAdoptionCannotBeResurrected() throws Exception {
        try (Fixture fixture = new Fixture()) {
            Duration ttl = Duration.ofMillis(200);
            Acquired acquired = fixture.acquire("boot-expired", ttl);
            WorkloadClaimReading before = fixture.awaitRemainingBelow(acquired.claim, Duration.ZERO);
            assertThat(before.leased()).isFalse();
            TrackingClaims tracked = new TrackingClaims(fixture.claims);
            CountDownLatch stopped = new CountDownLatch(1);
            try (NodeSessionLease lease = new NodeSessionLease(tracked, acquired.claim, acquired.askedAt,
                    ttl, Duration.ofMillis(50), stopped::countDown)) {
                assertThat(stopped.await(1, TimeUnit.SECONDS)).isTrue();
                assertThat(tracked.calls).hasValue(0);
                WorkloadClaimReading after = fixture.claims.read(acquired.claim.key()).orElseThrow();
                assertThat(after.leased()).isFalse();
                assertThat(after.claim()).isEqualTo(before.claim());
            }
        }
    }

    @Test
    void aReplacementOwnerRemainsUntouchedByTheOldBootsFirstRenewal() throws Exception {
        try (Fixture fixture = new Fixture()) {
            Duration ttl = Duration.ofSeconds(2);
            Acquired old = fixture.acquire("boot-old", ttl);
            assertThat(fixture.claims.release(old.claim)).isTrue();
            Acquired replacement = fixture.acquire("boot-replacement", ttl);
            assertThat(replacement.claim.claimGeneration()).isGreaterThan(old.claim.claimGeneration());
            TrackingClaims tracked = new TrackingClaims(fixture.claims);
            CountDownLatch stopped = new CountDownLatch(1);
            try (NodeSessionLease lease = new NodeSessionLease(tracked, old.claim, old.askedAt,
                    ttl, Duration.ofMillis(100), stopped::countDown)) {
                assertThat(stopped.await(1, TimeUnit.SECONDS)).isTrue();
                assertThat(tracked.calls).hasValue(1);
            }
            WorkloadClaimReading after = fixture.claims.read(replacement.claim.key()).orElseThrow();
            assertThat(after.leased()).isTrue();
            assertThat(after.claim()).isEqualTo(replacement.claim);
        }
    }

    @Test
    void aLateSuccessfulRenewalLeavesTimeForTheNextMongoAttempt() throws Exception {
        try (var fixture = new CacheFixture()) {
            Duration ttl = Duration.ofSeconds(5);
            var initial = fixture.acquire(ttl);
            fixture.delay("commitTransaction", new Document("times", 1), 3600);
            var losses = new AtomicInteger();
            try (var lease = new NodeSessionLease(fixture.observed, initial.claim, initial.askedAt,
                    ttl, Duration.ofSeconds(2), losses::incrementAndGet)) {
                await(() -> fixture.successfulWrites.get() >= 2, Duration.ofSeconds(5));
                assertThat(lease.currentFence()).contains(WorkloadClaimFence.from(initial.claim));
                assertThat(losses).hasValue(0);
            }
        }
    }

    @Test
    void aTimedOutMongoRenewalCanRecoverBeforeTheLastConfirmedDeadline() throws Exception {
        try (var fixture = new CacheFixture()) {
            Duration ttl = Duration.ofSeconds(11);
            var initial = fixture.acquire(ttl);
            var losses = new AtomicInteger();
            try (var lease = new NodeSessionLease(fixture.observed, initial.claim, initial.askedAt,
                    ttl, Duration.ofSeconds(2), losses::incrementAndGet)) {
                await(() -> fixture.successfulWrites.get() == 1, Duration.ofSeconds(2));
                fixture.delay("findAndModify", new Document("times", 1), 6500);
                long setupAt = System.nanoTime();
                await(() -> fixture.failedWrites.get() >= 1, Duration.ofSeconds(8));
                assertThat(Duration.ofNanos(System.nanoTime() - setupAt)).isLessThan(Duration.ofMillis(7300));
                assertThat(lease.currentFence()).contains(WorkloadClaimFence.from(initial.claim));
                await(() -> fixture.successfulReads.get() >= 1 && fixture.successfulWrites.get() >= 2,
                        Duration.ofSeconds(4));
                assertThat(lease.currentFence()).contains(WorkloadClaimFence.from(initial.claim));
                assertThat(losses).hasValue(0);
            }
        }
    }

    @Test
    void anUnacknowledgedMongoCommitPublishesOnlyTheExtendedOriginalLease() throws Exception {
        try (var fixture = new CacheFixture()) {
            Duration ttl = Duration.ofSeconds(15);
            var initial = fixture.acquire(ttl);
            var losses = new AtomicInteger();
            try (var lease = new NodeSessionLease(fixture.observed, initial.claim, initial.askedAt,
                    ttl, Duration.ofSeconds(4), losses::incrementAndGet)) {
                await(() -> fixture.successfulWrites.get() == 1, Duration.ofSeconds(2));
                var original = fixture.claims.read(initial.claim.key()).orElseThrow().claim();
                int beforeReads = fixture.successfulReads.get();
                fixture.failCommitAcknowledgement();
                await(() -> fixture.failedWrites.get() >= 1, Duration.ofSeconds(10));
                fixture.clearFailure();
                await(() -> fixture.successfulReads.get() > beforeReads, Duration.ofSeconds(4));
                var after = fixture.claims.read(original.key()).orElseThrow();
                assertThat(after.leased()).isTrue();
                assertThat(after.claim().leaseUntil()).isAfter(original.leaseUntil());
                assertThat(WorkloadClaimFence.from(after.claim())).isEqualTo(WorkloadClaimFence.from(original));
                assertThat(fixture.successfulWrites).hasValue(1);
                assertThat(lease.currentFence()).contains(WorkloadClaimFence.from(original));
                assertThat(losses).hasValue(0);
            }
        }
    }

    @Test
    void confirmationReadsTheCommittedLeaseWhileTheOriginalWriterStillOwnsItsWorker() throws Exception {
        try (var fixture = new CacheFixture(); var caller = Executors.newSingleThreadExecutor()) {
            var original = fixture.acquire(Duration.ofSeconds(15)).claim;
            var committed = new CountDownLatch(1);
            var releaseWriter = new CountDownLatch(1);
            var key = original.key();
            var response = caller.submit(() -> fixture.claims.withNodeSessionDeadline(
                    key, System.nanoTime() + Duration.ofSeconds(2).toNanos(), () -> {}, () -> {
                        var renewed = fixture.claims.renew(original, Duration.ofSeconds(30));
                        committed.countDown();
                        try {
                            if (!releaseWriter.await(10, TimeUnit.SECONDS))
                                throw new AssertionError("writer was not released");
                        } catch (InterruptedException stopped) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(stopped);
                        }
                        return renewed;
                    }));
            try {
                assertThat(committed.await(1, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> response.get(4, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(io.tapstate.core.common.TapstateException.class);
                var reading = fixture.claims.confirmNodeSession(key,
                        System.nanoTime() + Duration.ofSeconds(2).toNanos(), () -> {}).orElseThrow();
                assertThat(reading.leased()).isTrue();
                assertThat(reading.claim().leaseUntil()).isAfter(original.leaseUntil());
                assertThat(WorkloadClaimFence.from(reading.claim())).isEqualTo(WorkloadClaimFence.from(original));
                assertThatThrownBy(() -> fixture.claims.withNodeSessionDeadline(
                        key, System.nanoTime() + Duration.ofSeconds(1).toNanos(), () -> {}, () -> null))
                        .isInstanceOfSatisfying(WorkloadClaimRenewalException.class,
                                busy -> assertThat(busy.reason()).isEqualTo(WorkloadClaimRenewalException.Reason.NOT_STARTED));
            } finally {
                releaseWriter.countDown();
            }
        }
    }

    @Test
    void continuedMongoDelayCannotExtendTheOriginalProofOrRestoreTheOldFence() throws Exception {
        try (var fixture = new CacheFixture()) {
            Duration ttl = Duration.ofSeconds(4);
            var initial = fixture.acquire(ttl);
            long expires = initial.askedAt + ttl.toNanos();
            fixture.delay("findAndModify", "alwaysOn", 5600);
            var losses = new AtomicInteger();
            try (var lease = new NodeSessionLease(fixture.observed, initial.claim, initial.askedAt,
                    ttl, Duration.ofSeconds(1), losses::incrementAndGet)) {
                assertThat(lease.currentFence()).contains(WorkloadClaimFence.from(initial.claim));
                await(() -> losses.get() != 0, Duration.ofSeconds(6));
                assertThat(System.nanoTime()).isGreaterThanOrEqualTo(expires);
                fixture.clearFailure();
                fixture.claims.awaitQuiescence(System.nanoTime() + Duration.ofSeconds(7).toNanos());
                assertThat(lease.currentFence()).isEmpty();
                assertThat(losses).hasValue(1);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void confirmationCannotAcceptAReplacementOwnerOrRetiredCache(boolean retire) throws Exception {
        try (var fixture = new CacheFixture()) {
            Duration ttl = Duration.ofSeconds(15);
            var initial = fixture.acquire(ttl);
            var losses = new AtomicInteger();
            try (var lease = new NodeSessionLease(fixture.observed, initial.claim, initial.askedAt,
                    ttl, Duration.ofSeconds(2), losses::incrementAndGet)) {
                await(() -> fixture.successfulWrites.get() == 1, Duration.ofSeconds(2));
                long entered = fixture.delay("findAndModify", new Document("times", 1), 6500);
                fixture.awaitInjection(entered);
                WorkloadClaim replacement = null;
                if (retire) {
                    fixture.cache.retireBusiness(fixture.scope, "node-renewal-retirement-test", () -> {});
                } else {
                    assertThat(fixture.claims.release(initial.claim)).isTrue();
                    replacement = fixture.claims.acquire(initial.claim.key(), new WorkloadOwner("node-a", "boot-b"),
                            initial.claim.topologyRevision(), ttl).claim();
                    assertThat(replacement.claimGeneration()).isGreaterThan(initial.claim.claimGeneration());
                }
                await(() -> losses.get() != 0, Duration.ofSeconds(9));
                assertThat(lease.currentFence()).isEmpty();
                assertThat(losses).hasValue(1);
                assertThat(fixture.successfulWrites).hasValue(1);
                if (replacement != null) {
                    assertThat(fixture.claims.read(initial.claim.key()).orElseThrow().claim()).isEqualTo(replacement);
                }
            }
        }
    }

    private static void await(BooleanSupplier reached, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!reached.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(10);
        assertThat(reached.getAsBoolean()).as("expected node-session evidence within %s", timeout).isTrue();
    }

    private static final class CacheFixture implements AutoCloseable {
        final String suffix = UUID.randomUUID().toString().replace("-", "");
        final String cluster = "node-session-recovery";
        final com.mongodb.client.MongoClient admin = MongoClients.create(MONGO.getReplicaSetUrl());
        final String configName = "node_config_" + suffix;
        final String cacheName = "node_cache_" + suffix;
        final StorageBinding.Connection endpoint = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                MONGO.getReplicaSetUrl(cacheName), cacheName, null);
        final MongoCacheDomain cache = MongoCacheDomain.connect(endpoint, Duration.ofSeconds(5), suffix);
        final CacheBusinessScope scope;
        final DeadlineWorkloadClaimStore claims;
        final WorkloadClaimStore observed;
        final AtomicInteger successfulWrites = new AtomicInteger();
        final AtomicInteger failedWrites = new AtomicInteger();
        final AtomicInteger successfulReads = new AtomicInteger();

        CacheFixture() {
            var configSettings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(configName), null, Duration.ofSeconds(5));
            try (var config = MongoConfigurationBootstrap.prepare(configSettings, cluster, "installation", "setup", 1)) {
                var binding = config.bindings().commitWithFirstCache(new StorageSetupScope(cluster, "installation", "setup", 1),
                        StorageBinding.Mode.OP, endpoint, new SourceResource("views", null, "mongodb",
                                Map.of("uri", "mongodb://business.invalid/target"), null, null, null, null));
                cache.prepareFirstAuth(binding.firstCreation().orElseThrow());
                cache.prepareInitialBusiness(binding.preparation(), () -> {});
                scope = binding.preparation().businessScope();
                claims = (DeadlineWorkloadClaimStore) cache.workloadClaims(scope);
            }
            observed = ObservedWorkloadClaims.observe(claims, (write, failure) -> {
                if (write) {
                    if (failure == null) successfulWrites.incrementAndGet();
                    else failedWrites.incrementAndGet();
                } else if (failure == null) successfulReads.incrementAndGet();
            });
        }

        Acquired acquire(Duration ttl) {
            long askedAt = System.nanoTime();
            var key = new WorkloadClaimKey(cluster, WorkloadClaimType.NODE_SESSION, "node-a");
            return new Acquired(claims.acquire(key, new WorkloadOwner("node-a", "boot-a"), 1, ttl).claim(), askedAt);
        }

        long delay(String command, Object mode, int delayMs) {
            return setFailure(command, mode, new Document("blockConnection", true).append("blockTimeMS", delayMs));
        }

        void awaitInjection(long count) {
            admin.getDatabase("admin").runCommand(new Document("waitForFailPoint", "failCommand")
                    .append("timesEntered", count + 1).append("maxTimeMS", 3000));
        }

        void failCommitAcknowledgement() {
            setFailure("commitTransaction", "alwaysOn", new Document("blockConnection", true)
                    .append("blockTimeMS", 175)
                    .append("writeConcernError", new Document("code", 64).append("errmsg", "test commit reply lost")));
        }

        private long setFailure(String command, Object mode, Document options) {
            var result = admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", mode)
                    .append("data", options.append("failCommands", List.of(command))
                            .append("appName", MongoConnection.processTag(suffix))));
            return ((Number) result.get("count")).longValue();
        }

        void clearFailure() {
            admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
        }

        @Override public void close() {
            try {
                clearFailure();
                claims.awaitQuiescence(System.nanoTime() + Duration.ofSeconds(7).toNanos());
                cache.close();
            } finally {
                admin.close();
            }
        }
    }

    private record Acquired(WorkloadClaim claim, long askedAt) {}

    private static final class TrackingClaims implements WorkloadClaimStore {
        private final WorkloadClaimStore delegate;
        private final AtomicInteger calls = new AtomicInteger();
        private final CountDownLatch renewed = new CountDownLatch(1);

        private TrackingClaims(WorkloadClaimStore delegate) { this.delegate = delegate; }
        @Override public java.util.Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
            calls.incrementAndGet();
            var result = delegate.renew(expected, ttl);
            if (result.isPresent()) renewed.countDown();
            return result;
        }
        @Override public io.tapstate.spi.store.WorkloadClaimAttempt acquire(WorkloadClaimKey key,
                WorkloadOwner owner, long topologyRevision, Duration ttl) {
            return delegate.acquire(key, owner, topologyRevision, ttl);
        }
        @Override public boolean release(WorkloadClaim expected) { return delegate.release(expected); }
        @Override public java.util.Optional<WorkloadClaim> advanceExecution(WorkloadClaim expected,
                long topologyRevision, java.util.Set<String> executionNodeIds) {
            return delegate.advanceExecution(expected, topologyRevision, executionNodeIds);
        }
        @Override public java.util.Optional<WorkloadClaim> recordExecutionFailure(WorkloadClaim expected,
                boolean afterMemberLoss) { return delegate.recordExecutionFailure(expected, afterMemberLoss); }
        @Override public java.util.Optional<WorkloadClaimReading> read(WorkloadClaimKey key) { return delegate.read(key); }
    }

    private static final class Fixture implements AutoCloseable {
        private final MongoConnection connection;
        private final WorkloadClaimStore claims;

        private Fixture() {
            String database = "lease_adoption_" + UUID.randomUUID().toString().replace("-", "");
            connection = new MongoConnection(new MongoConnectionSettings(MONGO.getReplicaSetUrl(database),
                    null, Duration.ofSeconds(5), "lease-adoption-fixture"));
            try {
                connection.verify();
                claims = new MongoStorePort(connection, database + "_state").workloadClaims();
            } catch (RuntimeException | Error failure) {
                connection.close();
                throw failure;
            }
        }

        private Acquired acquire(String boot, Duration ttl) {
            long askedAt = System.nanoTime();
            WorkloadClaim claim = claims.acquire(new WorkloadClaimKey("lease-adoption", WorkloadClaimType.NODE_SESSION, "node-a"),
                    new WorkloadOwner("node-a", boot), 0, ttl).claim();
            return new Acquired(claim, askedAt);
        }

        private WorkloadClaimReading awaitRemainingBelow(WorkloadClaim claim, Duration remaining) throws InterruptedException {
            long deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
            WorkloadClaimReading reading;
            do {
                reading = claims.read(claim.key()).orElseThrow();
                if (reading.leaseRemaining().compareTo(remaining) <= 0) return reading;
                Thread.sleep(10);
            } while (System.nanoTime() < deadline);
            throw new AssertionError("the owned server lease did not reach its startup barrier");
        }

        @Override
        public void close() { connection.close(); }
    }
}
