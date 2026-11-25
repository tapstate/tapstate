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
 * <p>The store is the truth layer; this projects the storage-port result onto the control-ring report and hides
 * diagnostic text from legacy rows without a safety marker. New results are redacted before they are stored.
 * The faces render a control-ring type and never reach into the storage ports (the same split the artifact read
 * side keeps between {@link ArtifactQueryService} and {@link ApplyService}).
 */
public final class ConnectionTestResultQueryService {

    private final ConnectionTestResultStore store;
    private final java.util.function.Predicate<String> readable;

    public ConnectionTestResultQueryService(ConnectionTestResultStore store) {
        this(store, ignored -> true);
    }
    public ConnectionTestResultQueryService(ConnectionTestResultStore store, java.util.function.Predicate<String> readable) {
        this.store = Objects.requireNonNull(store, "store"); this.readable = Objects.requireNonNull(readable);
    }

    /**
     * Returns the connection's latest test result as the surface report, or empty when it has never been
     * tested (which the HTTP face renders as a 404).
     */
    public Optional<ConnectionTestReport> find(String connectionId) {
        Objects.requireNonNull(connectionId, "connectionId");
        if (!readable.test(connectionId)) return Optional.empty();
        return store.find(connectionId)
                .map(ConnectionTestResultRedactor::hideUntrustedDiagnostics)
                .map(ConnectionTestReport::from);
    }
}
