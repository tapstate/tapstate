package io.tapstate.control.core;

import io.tapstate.spi.store.ConnectionTestItem;
import io.tapstate.spi.store.ConnectionTestResult;

import java.util.List;
import java.util.Map;

/**
 * The surface-facing report of a connection test: the control ring's own projection of the storage-port
 * {@link ConnectionTestResult}, so the HTTP and CLI faces render a control-ring type and never reach into
 * the storage ports. It carries the overall {@link Outcome}, the per-check reports the connector returned
 * in arrival order, and the time it was tested (epoch milliseconds). An immutable value.
 */
public record ConnectionTestReport(
        String connectionId, String connectorId, Outcome outcome, List<Check> checks, long testedAt) {

    /** Projects a storage-port test result onto the surface report. */
    public static ConnectionTestReport from(ConnectionTestResult result) {
        return new ConnectionTestReport(
                result.connectionId(),
                result.connectorId(),
                Outcome.valueOf(result.outcome().name()),
                result.items().stream().map(Check::from).toList(),
                result.testedAt());
    }

    /** The overall outcome of a connection test; a warning check never fails the outcome. */
    public enum Outcome {

        /** The connection test passed. */
        PASSED,

        /** The connection test failed. */
        FAILED
    }

    /**
     * One check within a connection test: the connector's own item name, its status, and optional
     * diagnostics. {@code connectorErrorCode} is an opaque connector-supplied display string, not a
     * first-party error code.
     */
    public record Check(
            String name, Status status, String message, String reason, String solution,
            String connectorErrorCode, Map<String, Object> diagnostic) {

        public Check(String name, Status status, String message, String reason,
                String solution, String connectorErrorCode) {
            this(name, status, message, reason, solution, connectorErrorCode, Map.of());
        }

        static Check from(ConnectionTestItem item) {
            return new Check(item.name(), Status.valueOf(item.status().name()),
                    item.message(), item.reason(), item.solution(), item.connectorErrorCode(), item.diagnostic());
        }

        /** The outcome of a single check. */
        public enum Status {

            /** The check succeeded. */
            PASSED,

            /** The check succeeded but the connector attached a warning. */
            WARNING,

            /** The check failed. */
            FAILED
        }
    }
}
