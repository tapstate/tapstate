package io.tapstate.control.core;

import java.time.Instant;
import java.util.Objects;

/** The one-time user-session credential returned at login, plus its server-enforced expiry bounds. */
public record CreatedSession(String token, Instant idleExpiresAt, Instant absoluteExpiresAt) {

    public CreatedSession {
        if (token == null || token.isBlank()) {
            throw new IllegalArgumentException("created session token must be non-blank");
        }
        idleExpiresAt = Objects.requireNonNull(idleExpiresAt, "idleExpiresAt");
        absoluteExpiresAt = Objects.requireNonNull(absoluteExpiresAt, "absoluteExpiresAt");
    }
}
