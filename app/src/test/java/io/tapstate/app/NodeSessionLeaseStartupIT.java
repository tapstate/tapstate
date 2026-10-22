package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConnection;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Real server leases adopted after startup work, without restarting a member or changing its TTL. */
@RequiresDocker
class NodeSessionLeaseStartupIT {
    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

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
