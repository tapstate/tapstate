package io.tapstate.spi.store;

import io.tapstate.core.common.DiagnosticProjection;

import java.util.Map;
import java.util.Objects;

/**
 * One check within a connection test: a connector-reported item (Version / Connection / Login /
 * Read / Write / Time detection / ...) and its outcome, with optional human-facing diagnostics. An
 * immutable value.
 *
 * <p>{@code name} and {@code status} are always present. {@code name} is the connector's own display
 * name for the check and is passed through verbatim. {@code message}, {@code reason},
 * {@code solution} and {@code connectorErrorCode} are optional diagnostics that may be null.
 * {@code connectorErrorCode} is an opaque, connector-supplied display string — not a first-party
 * error code — shown as-is and taking no part in the error-code system.
 */
public record ConnectionTestItem(
        String name,
        Status status,
        String message,
        String reason,
        String solution,
        String connectorErrorCode,
        Map<String, Object> diagnostic) {

    public ConnectionTestItem {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(status, "status");
        diagnostic = DiagnosticProjection.read(diagnostic);
    }

    public ConnectionTestItem(String name, Status status, String message, String reason,
            String solution, String connectorErrorCode) {
        this(name, status, message, reason, solution, connectorErrorCode, Map.of());
    }

    /** The outcome of a single connection-test item. */
    public enum Status {

        /** The check succeeded. */
        PASSED,

        /** The check succeeded but the connector attached a warning. */
        WARNING,

        /** The check failed. */
        FAILED
    }
}
