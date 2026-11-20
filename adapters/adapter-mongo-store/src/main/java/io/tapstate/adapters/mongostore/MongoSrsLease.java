package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaimFence;
import org.bson.Document;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** Reuses a structural and model proof for one storage operation. */
final class MongoSrsLease {
    static final String VALIDATION_PROPERTY = "tapstate.srs.validation";
    static final String LEASE_PROPERTY = "tapstate.srs.lease.max-millis";
    static final long DEFAULT_LEASE_MILLIS = 10_000L;
    static final AtomicLong LEASE_OPENS = new AtomicLong();
    static final AtomicLong LEASE_LOSSES = new AtomicLong();
    private static final MongoSrsLease PER_TRANSACTION = new MongoSrsLease(false, 0L);

    static final class Lease {
        private final long deadlineNanos;
        private Lease(long deadlineNanos) { this.deadlineNanos = deadlineNanos; }
    }

    private final boolean leased;
    private final long maximumNanos;
    private final AtomicReference<Lease> current = new AtomicReference<>(ended());

    private MongoSrsLease(boolean leased, long maximumNanos) { this.leased = leased; this.maximumNanos = maximumNanos; }

    /** The mode and bound the system properties select, read when a capture's stores are assembled. */
    static MongoSrsLease selected() {
        boolean perTransaction = "transaction".equals(System.getProperty(VALIDATION_PROPERTY));
        long maximum = Math.max(1L, Long.getLong(LEASE_PROPERTY, DEFAULT_LEASE_MILLIS));
        return new MongoSrsLease(!perTransaction, TimeUnit.MILLISECONDS.toNanos(maximum));
    }

    /** Validation on every transaction, for stores assembled outside a capture: nothing ever stands. */
    static MongoSrsLease perTransaction() { return PER_TRANSACTION; }

    /** The lease seen when the operation started. */
    Lease observe() { return current.get(); }

    boolean enabled() { return leased; }

    boolean stands(Lease lease) { return leased && current.get() == lease && System.nanoTime() - lease.deadlineNanos < 0; }

    /**
     * Publishes the lease a proof established: it began at {@code startedNanos} while {@code before} stood, and
     * the claim had {@code claimRemainingMillis} of lease left on the store's clock. Published only over the
     * exact lease the proof started from, so a drop at any point of the proof wins and the next write proves
     * again; a proof that ran out while validating serves only the write it was proved for. Answers the lease
     * now standing, or null when none was published.
     */
    Lease proved(Lease before, long startedNanos, long claimRemainingMillis) {
        if (!leased) return null;
        long held = Math.min(maximumNanos, TimeUnit.MILLISECONDS.toNanos(Math.max(0L, claimRemainingMillis)));
        var lease = new Lease(startedNanos + held);
        if (System.nanoTime() - lease.deadlineNanos >= 0 || !current.compareAndSet(before, lease)) return null;
        LEASE_OPENS.incrementAndGet();
        return lease;
    }

    void drop() {
        if (!leased) return;
        Lease was = current.getAndSet(ended());
        if (System.nanoTime() - was.deadlineNanos < 0) LEASE_LOSSES.incrementAndGet();
    }

    /** A fresh, already expired marker: no two ends ever look like the same lease to a proof in flight. */
    private static Lease ended() { return new Lease(System.nanoTime()); }

    /**
     * How long the live claim {@code fence} names still runs, in milliseconds on the store's own clock, read
     * inside the proof's transaction. A read proof also checks the original owner and generation here before
     * it may publish a lease.
     */
    static long remainingMillis(MongoCollection<Document> claims, ClientSession session, WorkloadClaimFence fence) {
        Document remaining = claims.find(session, WorkloadClaimDocuments.live(fence))
                .projection(new Document("_id", 0).append("remaining", new Document("$subtract", List.of("$leaseUntil", "$$NOW")))).first();
        if (remaining == null && MongoWorkloadClaimStore.canUseNodeLease(fence)) {
            return new MongoWorkloadClaimStore(claims).inStrictSession(session).read(fence.key())
                    .filter(reading -> reading.leased() && fence.equals(WorkloadClaimFence.from(reading.claim())))
                    .map(reading -> reading.leaseRemaining().toMillis()).orElseThrow(
                            () -> new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null));
        }
        if (remaining == null || !(remaining.get("remaining") instanceof Number millis)) {
            throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
        }
        return Math.max(0L, millis.longValue());
    }
}
