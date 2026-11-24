package io.tapstate.cli;

import java.util.Map;

/**
 * The outcome of a remote {@code GET /api/connections/{id}/test-result}. Either the connection's latest
 * stored test result was found, no result is stored for it (a 404: it has never been tested), the request
 * was refused with a coded reason, or the server could not be reached. Sealed so the caller renders each
 * branch without try/catch, mirroring the never-throw seam. A {@link Found} carries a report whose outcome
 * may itself be {@code FAILED} (the last test ran and failed) — distinct from {@link Absent} (never tested).
 */
sealed interface ConnectionTestResultOutcome {

    /** The connection's latest stored test result. */
    record Found(ConnectionReport report) implements ConnectionTestResultOutcome {
    }

    /** No test result is stored for the connection: it has never been tested. */
    record Absent() implements ConnectionTestResultOutcome {
    }

    /** The server refused the read with a coded reason already rendered to a message. */
    record Rejected(String code, String message, Map<String, Object> params,
            Map<String, Object> diagnostic) implements ConnectionTestResultOutcome {
        Rejected(String code, String message) {
            this(code, message, Map.of(), Map.of());
        }
    }

    /** The server could not be reached (connection refused, timeout, or a malformed target). */
    record Unreachable() implements ConnectionTestResultOutcome {
    }
}
