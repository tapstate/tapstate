package io.tapstate.cli;

import java.util.Map;

/**
 * The outcome of a remote {@code POST /api/connections:test}. Either the server ran the test and returned
 * its structured report (whatever the connector concluded, pass or fail), the request was refused with a
 * coded reason, or the server could not be reached. Sealed so the caller renders each branch without
 * try/catch, mirroring the never-throw seam. A failed connection is a {@link Tested} carrying a report
 * whose outcome is {@code FAILED} — not a {@link Rejected}: the test ran, it just did not pass.
 */
sealed interface ConnectionTestOutcome {

    /** The server ran the test and returned its report. */
    record Tested(ConnectionReport report) implements ConnectionTestOutcome {
    }

    /** The server refused the request with a coded reason already rendered to a message. */
    record Rejected(String code, String message, Map<String, Object> params,
            Map<String, Object> diagnostic) implements ConnectionTestOutcome {
        Rejected(String code, String message) {
            this(code, message, Map.of(), Map.of());
        }
    }

    /** The server could not be reached (connection refused, connect timeout, or a malformed target). */
    record Unreachable() implements ConnectionTestOutcome {
    }

    /** The server was reached but did not answer within the request's timeout window — busy, not gone. */
    record TimedOut() implements ConnectionTestOutcome {
    }
}
