package io.tapstate.cli;

import java.time.Instant;
import java.util.List;

/** Result of exchanging a cached opaque session token for a process-local access token. */
sealed interface SessionExchangeOutcome {

    /** The server minted a new short-lived access token. */
    record Success(String token, Instant accessExpiresAt, String issuer, String principal, List<String> scopes)
            implements SessionExchangeOutcome {
        public Success {
            scopes = List.copyOf(scopes);
        }

        @Override
        public String toString() {
            return "Success[token=<redacted>, accessExpiresAt=" + accessExpiresAt
                    + ", issuer=" + issuer
                    + ", principal=" + principal
                    + ", scopes=" + scopes + ']';
        }
    }

    /** The server refused the session credential with a coded reason. */
    record Rejected(String code, String message) implements SessionExchangeOutcome {

        @Override
        public String toString() {
            return "Rejected[code=<redacted>]";
        }
    }

    /** The server could not be reached or did not return a usable exchange response. */
    record Unreachable() implements SessionExchangeOutcome {
    }
}
