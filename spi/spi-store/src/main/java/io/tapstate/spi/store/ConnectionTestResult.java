package io.tapstate.spi.store;

import java.util.List;
import java.util.Objects;

/**
 * The result of the latest connection test for one connection: an overall outcome plus the per-item
 * checks the connector reported, and the time it was tested. An immutable value.
 *
 * <p>{@code connectionId} is the tested connection's id (and the store key). {@code connectorId} is
 * the connector it configures, carried for display. {@code outcome} is stored independently and is
 * never derived from the items — a warning item does not make the outcome FAILED. {@code items} are
 * held as an unmodifiable defensive copy in arrival order; a null list is normalized to empty.
 * {@code testedAt} is epoch milliseconds.
 */
public record ConnectionTestResult(
        String connectionId,
        String connectorId,
        Outcome outcome,
        List<ConnectionTestItem> items,
        long testedAt,
        boolean diagnosticsRedacted) {

    /** Creates a result whose free-form diagnostics have not yet been checked for credentials. */
    public ConnectionTestResult(
            String connectionId, String connectorId, Outcome outcome,
            List<ConnectionTestItem> items, long testedAt) {
        this(connectionId, connectorId, outcome, items, testedAt, false);
    }

    public ConnectionTestResult {
        Objects.requireNonNull(connectionId, "connectionId");
        Objects.requireNonNull(connectorId, "connectorId");
        Objects.requireNonNull(outcome, "outcome");
        items = items == null ? List.of() : List.copyOf(items);
    }

    /** The overall outcome of a connection test. */
    public enum Outcome {

        /** The connection test passed; warnings do not fail it. */
        PASSED,

        /** The connection test failed. */
        FAILED
    }
}
