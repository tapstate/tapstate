package io.tapstate.control.core;

import java.time.Instant;
import java.util.Objects;

/** A freshly minted short-lived process access token and the server-side identity it represents. */
public record AccessTokenGrant(String token, Instant expiresAt, String principal, Scope scope) {

    public AccessTokenGrant {
        requireText(token, "token");
        requireText(principal, "principal");
        expiresAt = Objects.requireNonNull(expiresAt, "expiresAt");
        scope = Objects.requireNonNull(scope, "scope");
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("access token grant " + field + " must be non-blank");
        }
    }
}
