package io.tapstate.spi.store;

/**
 * A state store that validates its identity, claim and structure once per lease rather than on every
 * call, and must be told when the claim reading that lease was proved under is no longer good.
 *
 * <p>The owner of the claim reading (the member's own refresh of the pipeline's claim) calls
 * {@link #dropLease()} when a refresh fails, the claim it names moves to other generations, or the reading
 * runs out. The store then proves a new lease before its next read or write. A write proof requires the
 * live claim, so a lost claim fails writes closed. A read proof checks identity and structure only, not the
 * claim's liveness, so reads may continue after the claim is lost or taken over; callers must not rely on
 * this to fence reads. Dropping is cheap, idempotent and safe from any thread; it never performs IO.
 *
 * <p>Writes stay fenced on their own whatever this is told: every commit still compares and sets the live
 * claim. What this bounds is how long a store keeps trusting the structure it checked on a lease whose
 * claim reading has already gone.
 */
public interface LeasedStateStore {

    /** Forgets the current lease, so the next operation proves a new one first. */
    void dropLease();
}
