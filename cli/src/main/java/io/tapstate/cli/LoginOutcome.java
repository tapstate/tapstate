package io.tapstate.cli;

import java.time.Instant;
import java.util.List;

/**
 * The outcome of a {@code POST /auth/login} attempt. A login either succeeds with a bearer token, is
 * rejected by the server with a coded reason (a bad credential is refused as {@code control.auth-failed}
 * and reveals nothing about which half was wrong), or the server could not be reached at all. Modelled
 * as a sealed result rather than an exception so the caller renders each branch without try/catch,
 * mirroring the never-throw transport seam.
 */
sealed interface LoginOutcome {

    /** The server verified the credential and minted a session token. */
    record Success(
            String token,
            Instant accessExpiresAt,
            String issuer,
            String principal,
            List<String> scopes,
            String sessionToken,
            Instant sessionIdleExpiresAt,
            Instant sessionAbsoluteExpiresAt) implements LoginOutcome {

        Success(String token) {
            this(token, null, null, null, List.of(), null, null, null);
        }

        public Success {
            scopes = scopes == null ? List.of() : List.copyOf(scopes);
        }

        boolean hasPersistentSession() {
            return accessExpiresAt != null
                    && issuer != null && !issuer.isBlank()
                    && principal != null && !principal.isBlank()
                    && !scopes.isEmpty()
                    && isValidSessionToken(sessionToken)
                    && !sessionToken.equals(token)
                    && sessionIdleExpiresAt != null
                    && sessionAbsoluteExpiresAt != null;
        }

        @Override
        public String toString() {
            return "Success[token=<redacted>, accessExpiresAt=" + accessExpiresAt
                    + ", issuer=" + issuer + ", principal=" + principal + ", scopes=" + scopes
                    + ", sessionToken=<redacted>, sessionIdleExpiresAt=" + sessionIdleExpiresAt
                    + ", sessionAbsoluteExpiresAt=" + sessionAbsoluteExpiresAt + ']';
        }
    }

    /** The server refused the login with a coded reason already rendered to a message. */
    record Rejected(String code, String message) implements LoginOutcome {

        @Override
        public String toString() {
            return "Rejected[code=<redacted>]";
        }
    }

    /** The server could not be reached (connection refused, timeout, or a malformed target). */
    record Unreachable() implements LoginOutcome {
    }

    /** Mirrors the server's opaque session envelope without constraining generated part lengths. */
    static boolean isValidSessionToken(String token) {
        if (token == null || !token.startsWith("tss_")) {
            return false;
        }
        String body = token.substring("tss_".length());
        int separator = body.indexOf('.');
        return separator > 0
                && separator < body.length() - 1
                && body.indexOf('.', separator + 1) < 0
                && isTokenPart(body.substring(0, separator))
                && isTokenPart(body.substring(separator + 1));
    }

    private static boolean isTokenPart(String value) {
        return value.codePoints().allMatch(c -> (c >= 'A' && c <= 'Z')
                || (c >= 'a' && c <= 'z')
                || (c >= '0' && c <= '9')
                || c == '-'
                || c == '_');
    }
}
