package io.tapstate.adapters.mongostore;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.WorkloadClaimFence;
import org.bson.Document;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * How often one assembly's SRS stores prove identity, structure and model consistency on their fenced
 * operations: once per lease (the default), or on every transaction (the rollback switch).
 *
 * <p>Each facet (the log's fenced append, the chain's checkpoint, the consumer's read verification, the
 * consumer's progress writes) proves its own lease: the first fenced transaction after the facet's lease ran
 * out carries today's full validation, and the lease then stands from when that transaction began for the
 * shorter of the claim's remaining lease, as the store counted it inside the proof, and the configured
 * maximum. Within a lease, a fenced write is one transaction holding the claim compare-and-set and its
 * payload, and a read verification is a check against what the proof read. Any failed fenced transaction
 * ends that facet's lease; every chain change the owner makes itself (a model publication, a new epoch, a
 * table request, a serving selection, a model reclaim that removed a model) and every change a consumer makes to its own
 * membership or snapshot ends all of them, so the next operation proves against the changed state. Writes
 * stay fenced whatever stands here: every commit still compares and sets the live claim. What a lease bounds
 * is how long a store trusts the structure, identity and model relationships it last checked; a change made
 * underneath a running lease by something that does not hold the claim is seen at the next proof.
 */
final class MongoSrsLease {
    /**
     * Selects how often the capture stores validate. {@code transaction} restores per-transaction validation
     * for one release, as the rollback switch; any other value, and no value, selects the lease.
     */
    static final String VALIDATION_PROPERTY = "tapstate.srs.validation";
    /** The longest one lease stands before the next fenced write proves again, in milliseconds. */
    static final String LEASE_PROPERTY = "tapstate.srs.lease.max-millis";
    static final long DEFAULT_LEASE_MILLIS = 10_000L;
    /** Lease proofs and ended leases across every capture in this process, for measurement only. */
    static final AtomicLong LEASE_OPENS = new AtomicLong();
    static final AtomicLong LEASE_LOSSES = new AtomicLong();
    private static final MongoSrsLease PER_TRANSACTION = new MongoSrsLease(false, 0L);

    /**
     * The fenced operations with a lease of their own, each proving exactly what it skips: a capture's append
     * and checkpoint, a consumer's read verification and its progress writes.
     */
    enum Facet { APPEND, CHECKPOINT, READ, PROGRESS }

    /** One lease: until when it stands on this process's monotonic clock. Identity matters, value does not. */
    static final class Lease {
        private final long deadlineNanos;
        private Lease(long deadlineNanos) { this.deadlineNanos = deadlineNanos; }
    }

    private final boolean leased;
    private final long maximumNanos;
    private final Map<Facet, AtomicReference<Lease>> slots = new EnumMap<>(Facet.class);

    private MongoSrsLease(boolean leased, long maximumNanos) {
        this.leased = leased; this.maximumNanos = maximumNanos;
        for (Facet facet : Facet.values()) slots.put(facet, new AtomicReference<>(ended()));
    }

    /** The mode and bound the system properties select, read when a capture's stores are assembled. */
    static MongoSrsLease selected() {
        boolean perTransaction = "transaction".equals(System.getProperty(VALIDATION_PROPERTY));
        long maximum = Math.max(1L, Long.getLong(LEASE_PROPERTY, DEFAULT_LEASE_MILLIS));
        return new MongoSrsLease(!perTransaction, TimeUnit.MILLISECONDS.toNanos(maximum));
    }

    /** Validation on every transaction, for stores assembled outside a capture or a consumer: nothing ever stands. */
    static MongoSrsLease perTransaction() { return PER_TRANSACTION; }

    /** Whether any lease can stand here at all; a per-transaction store never reads what a lease would need. */
    boolean leasing() { return leased; }

    /** The facet's lease as a fenced transaction begins; it decides whether that transaction proves. */
    Lease observe(Facet facet) { return slot(facet).get(); }

    boolean stands(Lease lease) { return leased && System.nanoTime() - lease.deadlineNanos < 0; }

    /**
     * Publishes the lease a proof established: it began at {@code startedNanos} while {@code before} stood, and
     * the claim had {@code claimRemainingMillis} of lease left on the store's clock. Published only over the
     * exact lease the proof started from, so a drop at any point of the proof wins and the next write proves
     * again; a proof that ran out while validating serves only the write it was proved for. Answers the lease
     * now standing, or null when none was published.
     */
    Lease proved(Facet facet, Lease before, long startedNanos, long claimRemainingMillis) {
        if (!leased) return null;
        long held = Math.min(maximumNanos, TimeUnit.MILLISECONDS.toNanos(Math.max(0L, claimRemainingMillis)));
        var lease = new Lease(startedNanos + held);
        if (System.nanoTime() - lease.deadlineNanos >= 0 || !slot(facet).compareAndSet(before, lease)) return null;
        LEASE_OPENS.incrementAndGet();
        return lease;
    }

    /** Ends one facet's lease, after a failed fenced transaction. Cheap, idempotent, no IO. */
    void drop(Facet facet) { end(slot(facet)); }

    /** Ends every lease, after a change by the owner itself. Cheap, idempotent, safe from any thread. */
    void drop() { for (var slot : slots.values()) end(slot); }

    private void end(AtomicReference<Lease> slot) {
        Lease was = slot.getAndSet(ended());
        if (leased && System.nanoTime() - was.deadlineNanos < 0) LEASE_LOSSES.incrementAndGet();
    }

    private AtomicReference<Lease> slot(Facet facet) { return slots.get(facet); }

    /** A fresh, already expired marker: no two ends ever look like the same lease to a proof in flight. */
    private static Lease ended() { return new Lease(System.nanoTime()); }

    /**
     * How long the live claim {@code fence} names still runs, in milliseconds on the store's own clock, read
     * inside the proof's transaction; the caller counts it down on its monotonic clock without ever reading a
     * deadline. Read from the claim as the fence names it, never by its key alone: a claim another owner holds,
     * or one that ran out, bounds nothing here, and a proof that only reads is fenced by it like a write.
     */
    static long remainingMillis(MongoCollection<Document> claims, ClientSession session, WorkloadClaimFence fence) {
        Document remaining = claims.find(session, WorkloadClaimDocuments.live(fence))
                .projection(new Document("_id", 0).append("remaining", new Document("$subtract", List.of("$leaseUntil", "$$NOW")))).first();
        if (remaining == null || !(remaining.get("remaining") instanceof Number millis)) {
            throw new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
        }
        return Math.max(0L, millis.longValue());
    }
}
