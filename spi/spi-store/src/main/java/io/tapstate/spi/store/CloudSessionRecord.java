package io.tapstate.spi.store;

import java.time.Instant;
import java.util.Objects;

/** Minimal local login facts; neither a raw Cloud JWT nor user profile/password/role records are stored. */
public record CloudSessionRecord(
        CloudSessionIdentity identity,
        String jwtId,
        String secretHash,
        String userId,
        String scope,
        boolean revoked,
        Instant createdAt,
        Instant lastUsedAt,
        Instant idleExpiresAt) {

    public CloudSessionRecord {
        Objects.requireNonNull(identity, "identity");
        requireText(jwtId, "jwtId");
        requireText(secretHash, "secretHash");
        requireText(userId, "userId");
        requireText(scope, "scope");
        Objects.requireNonNull(createdAt, "createdAt");
        Objects.requireNonNull(lastUsedAt, "lastUsedAt");
        Objects.requireNonNull(idleExpiresAt, "idleExpiresAt");
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Cloud session record " + name + " must be non-blank");
        }
    }

    @Override
    public String toString() {
        return "CloudSessionRecord[identity=" + identity + ", jwtId=" + jwtId + ", userId=" + userId
                + ", scope=" + scope + ", revoked=" + revoked + ", idleExpiresAt=" + idleExpiresAt
                + ", secretHash=<redacted>]";
    }
}
