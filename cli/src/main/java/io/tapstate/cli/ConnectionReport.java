package io.tapstate.cli;

import java.util.List;
import java.util.Map;

/**
 * The CLI's view of a connection test the server ran: the overall outcome, the per-check reports the
 * connector returned in arrival order, and when it was tested (epoch milliseconds). The response-side
 * value the {@code test} verb decodes from the server's JSON. The CLI carries no shared control type
 * (rule R6: it reaches the server over HTTP only), so this mirrors the server's report shape
 * independently. {@code outcome} and each check {@code status} are the server's own strings
 * ({@code PASSED} / {@code WARNING} / {@code FAILED}), rendered rather than interpreted.
 */
record ConnectionReport(
        String connectionId, String connectorId, String outcome, List<Check> checks, long testedAt) {

    /**
     * One check within a connection test: the connector's own item name, its status, and optional
     * diagnostics (all {@code null} when the connector supplied none). {@code connectorErrorCode} is an
     * opaque connector-supplied display string, not a first-party error code.
     */
    record Check(
            String name, String status, String message, String reason, String solution,
            String connectorErrorCode, Map<String, Object> diagnostic) {

        Check(String name, String status, String message, String reason, String solution,
                String connectorErrorCode) {
            this(name, status, message, reason, solution, connectorErrorCode, Map.of());
        }
    }
}
