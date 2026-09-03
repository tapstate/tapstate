package io.tapstate.spi.store;

import java.time.Instant;
import java.util.Optional;

/**
 * Persistence port for revocable opaque user sessions. The store owns only durable state and atomic
 * mutations; the control layer owns token shape, secret verification, issuer binding and TTL policy.
 */
public interface SessionStore {

    /** Persists a newly issued session record, keyed by its public session id. */
    void save(SessionRecord record);

    /** Returns the stored session for diagnostics and administration, or empty when it does not exist. */
    Optional<SessionRecord> find(String sessionId);

    /**
     * In one compare-and-update operation verifies every bearer and lifetime condition, then refreshes
     * last-used and idle expiry, capped at the stored absolute expiry. Empty means any condition failed.
     */
    Optional<SessionRecord> exchange(
            String sessionId, String secretHash, String issuer, Instant now, Instant idleExpiresAt);

    /**
     * Atomically validates the bearer, issuer and lifetime and marks the session revoked. The revoked flag
     * is deliberately not a precondition, making a repeated logout with the same valid credential succeed.
     */
    boolean revoke(String sessionId, String secretHash, String issuer, Instant now);
}
