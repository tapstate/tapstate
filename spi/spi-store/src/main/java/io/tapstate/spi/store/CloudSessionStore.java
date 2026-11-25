package io.tapstate.spi.store;

import java.time.Instant;
import java.util.Optional;

/** Local managed-session persistence, distinct from the on-prem session format and lifetime rules. */
public interface CloudSessionStore {

    /**
     * Creates at most one local session for an issued JWT id. An existing session or revocation marker
     * refuses creation atomically, including when invalidation arrived before the login write.
     */
    boolean create(CloudSessionRecord record);

    /** Reads a login record for diagnostics/tests, but never treats a revocation-only marker as a login. */
    Optional<CloudSessionRecord> find(CloudSessionIdentity identity, String jwtId);

    /** Atomically checks context/secret/revocation/idle expiry and refreshes the local idle deadline. */
    Optional<CloudSessionRecord> authenticate(
            CloudSessionIdentity identity, String jwtId, String secretHash, Instant now, Instant idleExpiresAt);

    /** Revokes a matching local cookie session; a repeated call with the same secret is idempotent. */
    boolean logout(CloudSessionIdentity identity, String jwtId, String secretHash, Instant now);

    /**
     * Idempotently revokes this JWT id in this exact deployment context, retaining a deny marker even
     * when the session has not been created yet. Callback authentication belongs to the caller.
     */
    void invalidate(CloudSessionIdentity identity, String jwtId, Instant now);
}
