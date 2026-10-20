package io.tapstate.app;

import io.tapstate.adapters.mongostore.MongoConnection;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.adapters.mongostore.SourceConfigKeyringSession;
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
            fixture.keyring.acknowledge(acquired.claim, ttl);
            long proofDeadline = acquired.askedAt + ttl.toNanos();
            long adoptionAt = proofDeadline - Duration.ofSeconds(3).toNanos();
            TimeUnit.NANOSECONDS.sleep(Math.max(0, adoptionAt - System.nanoTime()));
            WorkloadClaimReading before = fixture.claims.read(acquired.claim.key()).orElseThrow();
            assertThat(before.leased()).isTrue();
            assertThat(before.leaseRemaining()).isPositive().isLessThan(interval);
            assertThat(proofDeadline - System.nanoTime()).isPositive().isLessThan(interval.toNanos());
            TrackingKeyring keyring = new TrackingKeyring(fixture.keyring);
            AtomicInteger stopped = new AtomicInteger();
            try (NodeSessionLease lease = new NodeSessionLease(fixture.claims, acquired.claim, acquired.askedAt,
                    ttl, interval, keyring, stopped::incrementAndGet)) {
                boolean renewed = keyring.acknowledged.await(
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
            TrackingKeyring keyring = new TrackingKeyring(fixture.keyring);
            CountDownLatch stopped = new CountDownLatch(1);
            try (NodeSessionLease lease = new NodeSessionLease(fixture.claims, acquired.claim, acquired.askedAt,
                    ttl, Duration.ofMillis(50), keyring, stopped::countDown)) {
                assertThat(stopped.await(1, TimeUnit.SECONDS)).isTrue();
                assertThat(keyring.calls).hasValue(0);
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
            TrackingKeyring keyring = new TrackingKeyring(fixture.keyring);
            CountDownLatch stopped = new CountDownLatch(1);
            try (NodeSessionLease lease = new NodeSessionLease(fixture.claims, old.claim, old.askedAt,
                    ttl, Duration.ofMillis(100), keyring, stopped::countDown)) {
                assertThat(stopped.await(1, TimeUnit.SECONDS)).isTrue();
                assertThat(keyring.calls).hasValue(0);
            }
            WorkloadClaimReading after = fixture.claims.read(replacement.claim.key()).orElseThrow();
            assertThat(after.leased()).isTrue();
            assertThat(after.claim()).isEqualTo(replacement.claim);
        }
    }

    private record Acquired(WorkloadClaim claim, long askedAt) {}

    private static final class TrackingKeyring implements SourceConfigKeyringSession {
        private final SourceConfigKeyringSession delegate;
        private final AtomicInteger calls = new AtomicInteger();
        private final CountDownLatch acknowledged = new CountDownLatch(1);

        private TrackingKeyring(SourceConfigKeyringSession delegate) { this.delegate = delegate; }
        @Override
        public void acknowledge(WorkloadClaim session, Duration ttl) {
            delegate.acknowledge(session, ttl);
            calls.incrementAndGet();
            acknowledged.countDown();
        }
        @Override
        public void release(WorkloadClaim session) { delegate.release(session); }
    }

    private static final class Fixture implements AutoCloseable {
        private final MongoConnection connection;
        private final WorkloadClaimStore claims;
        private final SourceConfigKeyringSession keyring;

        private Fixture() {
            String database = "lease_adoption_" + UUID.randomUUID().toString().replace("-", "");
            connection = new MongoConnection(new MongoConnectionSettings(MONGO.getReplicaSetUrl(database),
                    null, Duration.ofSeconds(5), "lease-adoption-fixture"));
            try {
                connection.verify();
                claims = new MongoStorePort(connection, database + "_state").workloadClaims();
                keyring = connection.sourceConfigKeyring();
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
