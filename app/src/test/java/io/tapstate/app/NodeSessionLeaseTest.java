package io.tapstate.app;

import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class NodeSessionLeaseTest {

    private static final WorkloadClaim CLAIM = new WorkloadClaim(
            new WorkloadClaimKey("cluster-a", WorkloadClaimType.NODE_SESSION, "node-a"),
            new WorkloadOwner("node-a", "boot-1"), 1, 0, 0, Instant.now().plusSeconds(30));

    @Test
    void aLiveNodeSessionIsRenewedAndReleasedBeforeTheMemberIsDestroyed() throws Exception {
        RecordingStore store = new RecordingStore(true);
        NodeSessionLease lease = new NodeSessionLease(
                store, CLAIM, Duration.ofSeconds(1), Duration.ofMillis(10), () -> { });

        assertThat(store.renewed.await(1, TimeUnit.SECONDS)).isTrue();
        lease.close();

        assertThat(store.released.get()).isTrue();
    }

    @Test
    void losingTheExactClaimStopsTheMemberRatherThanRenewingAsTheNewGeneration() throws Exception {
        RecordingStore store = new RecordingStore(false);
        CountDownLatch memberStopped = new CountDownLatch(1);
        NodeSessionLease lease = new NodeSessionLease(
                store, CLAIM, Duration.ofSeconds(1), Duration.ofMillis(10), memberStopped::countDown);

        assertThat(memberStopped.await(1, TimeUnit.SECONDS)).isTrue();
        lease.close();

        assertThat(store.released.get()).isFalse();
    }

    private static final class RecordingStore implements WorkloadClaimStore {

        /** Nothing here reads the lease; it answers with a live one so a read is not a lapsed claim. */
        private static final Duration LIVE_LEASE = Duration.ofSeconds(30);
        private final boolean renews;
        private final CountDownLatch renewed = new CountDownLatch(1);
        private final AtomicBoolean released = new AtomicBoolean();

        private RecordingStore(boolean renews) {
            this.renews = renews;
        }

        @Override
        public WorkloadClaimAttempt acquire(
                WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
            renewed.countDown();
            return renews ? Optional.of(expected) : Optional.empty();
        }

        @Override
        public boolean release(WorkloadClaim expected) {
            released.set(true);
            return true;
        }

        @Override
        public Optional<WorkloadClaim> advanceExecution(WorkloadClaim expected, long topologyRevision) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<WorkloadClaimReading> read(WorkloadClaimKey key) {
            return Optional.of(new WorkloadClaimReading(CLAIM, LIVE_LEASE));
        }
    }
}
