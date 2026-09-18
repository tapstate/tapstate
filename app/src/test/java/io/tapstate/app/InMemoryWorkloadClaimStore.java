package io.tapstate.app;

import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadOwner;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * In-memory {@link WorkloadClaimStore} double holding the durable store's contract, so a test over it
 * witnesses the same rules the coordination store enforces:
 *
 * <ul>
 *   <li>a lease is compared against <em>this store's</em> clock, never the caller's, and only
 *       {@link #elapse} moves it — so a test says when a lease ran out instead of sleeping for it;
 *   <li>a renew, a release and an execution advance match the exact owner and both generations, not the
 *       whole claim: the lease moves under every one of them, so comparing it would refuse them all;
 *   <li>a takeover advances the claim generation and carries the execution generation across unchanged,
 *       and a release expires the lease without deleting the record or lowering either generation.
 * </ul>
 *
 * <p>Synchronized because members share one of these the way they share one store: an unsynchronized map
 * loses a write from another member's thread, which reads exactly like ownership being handed over twice.
 */
final class InMemoryWorkloadClaimStore implements WorkloadClaimStore {

    private final Map<WorkloadClaimKey, WorkloadClaim> claims = new LinkedHashMap<>();
    private Instant now = Instant.parse("2026-09-18T00:00:00Z");

    /** Moves the store's own clock on, the way waiting would move the coordination store's. */
    synchronized void elapse(Duration elapsed) {
        now = now.plus(elapsed);
    }

    @Override
    public synchronized WorkloadClaimAttempt acquire(
            WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(owner, "owner");
        WorkloadClaim current = claims.get(key);
        if (current != null && !current.owner().equals(owner) && current.leaseUntil().isAfter(now)) {
            return WorkloadClaimAttempt.refused(current);
        }
        long claimGeneration = current == null ? 1
                : current.owner().equals(owner) ? current.claimGeneration() : current.claimGeneration() + 1;
        long executionGeneration = current == null ? 0 : current.executionGeneration();
        WorkloadClaim acquired = new WorkloadClaim(
                key, owner, claimGeneration, executionGeneration, topologyRevision, now.plus(ttl));
        claims.put(key, acquired);
        return WorkloadClaimAttempt.acquired(acquired);
    }

    @Override
    public synchronized Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
        WorkloadClaim current = live(expected, expected.topologyRevision());
        if (current == null) {
            return Optional.empty();
        }
        return Optional.of(store(new WorkloadClaim(
                current.key(), current.owner(), current.claimGeneration(), current.executionGeneration(),
                current.topologyRevision(), now.plus(ttl))));
    }

    @Override
    public synchronized boolean release(WorkloadClaim expected) {
        WorkloadClaim current = claims.get(expected.key());
        if (!matches(current, expected)) {
            return false;
        }
        store(new WorkloadClaim(
                current.key(), current.owner(), current.claimGeneration(), current.executionGeneration(),
                current.topologyRevision(), now));
        return true;
    }

    @Override
    public synchronized Optional<WorkloadClaim> advanceExecution(WorkloadClaim expected, long topologyRevision) {
        WorkloadClaim current = live(expected, topologyRevision);
        if (current == null) {
            return Optional.empty();
        }
        return Optional.of(store(new WorkloadClaim(
                current.key(), current.owner(), current.claimGeneration(), current.executionGeneration() + 1,
                current.topologyRevision(), current.leaseUntil())));
    }

    @Override
    public synchronized Optional<WorkloadClaimReading> read(WorkloadClaimKey key) {
        return Optional.ofNullable(claims.get(key))
                .map(claim -> new WorkloadClaimReading(claim, Duration.between(now, claim.leaseUntil())));
    }

    private WorkloadClaim store(WorkloadClaim claim) {
        claims.put(claim.key(), claim);
        return claim;
    }

    /** The stored claim when it is this exact owner's, at this topology revision, and still leased. */
    private WorkloadClaim live(WorkloadClaim expected, long topologyRevision) {
        WorkloadClaim current = claims.get(expected.key());
        if (!matches(current, expected)
                || current.topologyRevision() != topologyRevision
                || !current.leaseUntil().isAfter(now)) {
            return null;
        }
        return current;
    }

    private static boolean matches(WorkloadClaim current, WorkloadClaim expected) {
        Objects.requireNonNull(expected, "expected");
        return current != null
                && current.owner().equals(expected.owner())
                && current.claimGeneration() == expected.claimGeneration()
                && current.executionGeneration() == expected.executionGeneration();
    }
}
