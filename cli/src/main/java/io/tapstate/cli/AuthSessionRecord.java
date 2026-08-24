package io.tapstate.cli;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** The secret-bearing auth cache payload; access tokens are intentionally absent. */
record AuthSessionRecord(
        int version,
        UUID authRef,
        UUID contextId,
        String issuer,
        String principal,
        List<String> scopes,
        String sessionToken,
        Instant createdAt,
        Instant idleExpiresAt,
        Instant absoluteExpiresAt) {

    static final int CURRENT_VERSION = 1;

    AuthSessionRecord {
        authRef = Objects.requireNonNull(authRef, "authRef");
        contextId = Objects.requireNonNull(contextId, "contextId");
        issuer = required(issuer, "issuer");
        principal = required(principal, "principal");
        scopes = List.copyOf(scopes);
        if (scopes.isEmpty() || scopes.stream().anyMatch(scope -> scope == null || scope.isBlank())) {
            throw new IllegalArgumentException("scopes must contain non-empty values");
        }
        sessionToken = required(sessionToken, "sessionToken");
        if (!LoginOutcome.isValidSessionToken(sessionToken)) {
            throw new IllegalArgumentException("sessionToken has an invalid opaque-session shape");
        }
        createdAt = Objects.requireNonNull(createdAt, "createdAt");
        idleExpiresAt = Objects.requireNonNull(idleExpiresAt, "idleExpiresAt");
        absoluteExpiresAt = Objects.requireNonNull(absoluteExpiresAt, "absoluteExpiresAt");
        if (idleExpiresAt.isBefore(createdAt) || absoluteExpiresAt.isBefore(idleExpiresAt)) {
            throw new IllegalArgumentException("session expiry timestamps must be ordered");
        }
    }

    @Override
    public String toString() {
        return "AuthSessionRecord[version=" + version
                + ", authRef=" + authRef
                + ", contextId=" + contextId
                + ", issuer=" + issuer
                + ", principal=" + principal
                + ", scopes=" + scopes
                + ", sessionToken=<redacted>"
                + ", createdAt=" + createdAt
                + ", idleExpiresAt=" + idleExpiresAt
                + ", absoluteExpiresAt=" + absoluteExpiresAt + ']';
    }

    private static String required(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " must be non-empty");
        }
        return value;
    }
}
