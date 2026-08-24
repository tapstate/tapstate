package io.tapstate.spi.store;

import java.time.Instant;

/**
 * One persisted opaque user session. The public id is the lookup handle; the bearer secret is never
 * stored, only its hash. The principal, scope and issuer are server-side truth, so a session file cannot
 * widen its own capability or move itself to another cluster by editing local metadata.
 */
public record SessionRecord(
        String sessionId,
        String secretHash,
        String principal,
        String scope,
        String issuer,
        boolean revoked,
        Instant createdAt,
        Instant lastUsedAt,
        Instant idleExpiresAt,
        Instant absoluteExpiresAt) {

    public SessionRecord {
        requireText(sessionId, "sessionId");
        requireText(secretHash, "secretHash");
        requireText(principal, "principal");
        requireText(scope, "scope");
        requireText(issuer, "issuer");
        requireInstant(createdAt, "createdAt");
        requireInstant(lastUsedAt, "lastUsedAt");
        requireInstant(idleExpiresAt, "idleExpiresAt");
        requireInstant(absoluteExpiresAt, "absoluteExpiresAt");
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("session record " + field + " must be non-blank");
        }
    }

    private static void requireInstant(Instant value, String field) {
        if (value == null) {
            throw new IllegalArgumentException("session record " + field + " must be set");
        }
    }
}
