package io.tapstate.control.core;

import io.tapstate.spi.store.ConnectionTestResultStore;

import java.util.Objects;
import java.util.Optional;

/**
 * The read side of the connection-test result: the query peer of {@link ConnectionTestService} (the audited
 * write that runs the probe and persists its result). It returns a connection's latest stored result as the
 * surface {@link ConnectionTestReport}, or empty when the connection has never been tested. A read of derived
 * observation data, not the config truth layer, so it carries no audit gate.
 *
 * <p>The store is the truth layer; this only projects the storage-port result onto the control-ring report,
 * redacting values from the matching saved Source before projection so older stored diagnostics cannot expose
 * its current secrets. The faces render a control-ring type and never reach into the storage ports (the same
 * split the artifact read side keeps between {@link ArtifactQueryService} and {@link ApplyService}).
 */
public final class ConnectionTestResultQueryService {

    private final ConnectionTestResultStore store;
    private final SourceConnectionResolver sourceConnections;

    public ConnectionTestResultQueryService(ConnectionTestResultStore store) {
        this(store, null);
    }

    public ConnectionTestResultQueryService(
            ConnectionTestResultStore store, SourceConnectionResolver sourceConnections) {
        this.store = Objects.requireNonNull(store, "store");
        this.sourceConnections = sourceConnections;
    }

    /**
     * Returns the connection's latest test result as the surface report, or empty when it has never been
     * tested (which the HTTP face renders as a 404).
     */
    public Optional<ConnectionTestReport> find(String connectionId) {
        Objects.requireNonNull(connectionId, "connectionId");
        return store.find(connectionId)
                .map(result -> sourceConnections == null
                        ? result
                        : sourceConnections.redactStoredDiagnostics(result))
                .map(ConnectionTestReport::from);
    }
}
