package io.tapstate.cli;

/** Result of remotely revoking an opaque user session. */
sealed interface SessionLogoutOutcome {

    /** The session is revoked, including when it was already revoked. */
    record Success() implements SessionLogoutOutcome {
    }

    /** The server refused the credential. */
    record Rejected(String code, String message) implements SessionLogoutOutcome {

        @Override
        public String toString() {
            return "Rejected[code=<redacted>]";
        }
    }

    /** The server could not be reached or did not return a usable response. */
    record Unreachable() implements SessionLogoutOutcome {
    }
}
