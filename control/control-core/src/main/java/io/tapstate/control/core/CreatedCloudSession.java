package io.tapstate.control.core;

import java.time.Instant;
import java.util.Objects;

/** The one-time local cookie credential, never a Cloud JWT or a persisted plaintext secret. */
public record CreatedCloudSession(String token, Instant idleExpiresAt) {
    public CreatedCloudSession {
        if (token == null || token.isBlank()) {
            throw new IllegalArgumentException("Cloud session token must be non-blank");
        }
        Objects.requireNonNull(idleExpiresAt, "idleExpiresAt");
    }

    @Override
    public String toString() {
        return "CreatedCloudSession[token=<redacted>, idleExpiresAt=" + idleExpiresAt + "]";
    }
}
