package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.probe.ConnectionProbe;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.ConnectionTestResultStore;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The control plane's connection-test verb: it drives the target connection through the runtime probe —
 * the first member of the synchronous control-to-runtime whitelist — records the normalized result as the connection's latest
 * test result, and returns it. The whole operation runs under the audit gate: a connection test is an
 * audited write, so an audit record is written before the probe runs and the operation is refused if that
 * write fails (no audit, no execute).
 *
 * <p>The probe is injected: the runtime supplies the seam implementation, and when the control and runtime
 * roles split the same call travels across instances. The result store keeps only the latest result per
 * connection; who tested when is answered by the audit log, not the result store, so the two never
 * overlap. A probe that cannot run the test at all throws a coded connector-domain failure, which
 * propagates out of here uncaught — no result is stored for a test that never produced one.
 */
public final class ConnectionTestService {

    /** Optional runtime-local observation; it never adds fields to the persisted test report. */
    public interface Observer {
        Attempt begin(ConnectionConfig connection);
        default boolean manages(ConnectionConfig connection) { return false; }
    }
    public interface Attempt extends AutoCloseable {
        Attempt NONE = new Attempt() {
            @Override public void completed(ConnectionTestResult result) {}
            @Override public void close() {}
        };
        void completed(ConnectionTestResult result);
        default java.util.OptionalLong deadlineNanos() { return java.util.OptionalLong.empty(); }
        default void publish(ConnectionTestResult result, ConnectionTestResultStore store) { store.save(result); completed(result); }
        @Override void close();
    }

    private final ConnectionProbe probe;
    private final ConnectionTestResultStore resultStore;
    private final AuditGate auditGate;
    private final ConnectorConfigValidator configValidator;
    private final SourceConnectionResolver sourceConnections;
    private final Observer observer;
    private record Flight(ConnectionConfig connection, java.util.concurrent.CompletableFuture<ConnectionTestReport> report) {}
    private Flight defaultFlight;

    public ConnectionTestService(
            ConnectionProbe probe, ConnectionTestResultStore resultStore, AuditGate auditGate,
            ConnectorConfigValidator configValidator, SourceConnectionResolver sourceConnections) {
        this(probe, resultStore, auditGate, configValidator, sourceConnections, ignored -> Attempt.NONE);
    }

    public ConnectionTestService(
            ConnectionProbe probe, ConnectionTestResultStore resultStore, AuditGate auditGate,
            ConnectorConfigValidator configValidator, SourceConnectionResolver sourceConnections, Observer observer) {
        this.probe = Objects.requireNonNull(probe, "probe");
        this.resultStore = Objects.requireNonNull(resultStore, "resultStore");
        this.auditGate = Objects.requireNonNull(auditGate, "auditGate");
        this.configValidator = configValidator;
        this.sourceConnections = Objects.requireNonNull(sourceConnections, "sourceConnections");
        this.observer = Objects.requireNonNull(observer, "observer");
    }

    /**
     * Tests the given connection through the runtime probe under the audit gate, saves the result as the
     * connection's latest, and returns the surface report. {@code connectionId} is the connection's id (the
     * result-store key and the audited resource), {@code connectorId} the connector it configures and
     * {@code settings} its configured values. {@code principal} is the authenticated subject the audit
     * record is attributed to. The stored result keeps the storage-port shape; the returned report is the
     * control ring's own projection, so the faces never reach into the storage ports.
     */
    public ConnectionTestReport test(
            String connectionId, String connectorId, Map<String, Object> settings, String principal) {
        SourceConnectionResolver.ResolvedConnection resolved =
                sourceConnections.resolveWithSecrets(connectionId, connectorId, settings);
        ConnectionConfig config = resolved.config();
        if (!observer.manages(config)) return execute(resolved, principal);
        Flight captured;
        boolean owner;
        synchronized (this) {
            owner = defaultFlight == null;
            if (owner) defaultFlight = new Flight(config, new java.util.concurrent.CompletableFuture<>());
            captured = defaultFlight;
            if (!captured.connection().equals(config)) throw new TapstateException(ControlError.MALFORMED_REQUEST, Map.of(), null);
        }
        if (!owner) {
            try { return captured.report().join(); }
            catch (java.util.concurrent.CompletionException failed) {
                if (failed.getCause() instanceof RuntimeException runtime) throw runtime;
                if (failed.getCause() instanceof Error error) throw error;
                throw failed;
            }
        }
        try {
            var report = execute(resolved, principal); captured.report().complete(report); return report;
        } catch (RuntimeException | Error failure) { captured.report().completeExceptionally(failure); throw failure; }
        finally { synchronized (this) { if (defaultFlight == captured) defaultFlight = null; } }
    }

    private ConnectionTestReport execute(SourceConnectionResolver.ResolvedConnection resolved, String principal) {
        ConnectionConfig config = resolved.config();
        List<String> secretValues = resolved.secretValues();
        try (var attempt = observer.begin(config)) {
            if (configValidator != null) {
                configValidator.validate(config.connectorId(), config.settings(), resolved.catalogSnapshot());
            }
            return auditGate.dispatch(
                    ControlOperations.CONNECTION_TEST,
                    new AuditContext(principal, config.id()),
                    () -> {
                        var deadline = attempt.deadlineNanos();
                        ConnectionTestResult result = deadline.isPresent() ? probe.probeDefault(config, deadline.getAsLong()) : probe.probe(config);
                        ConnectionTestResult safeResult = ConnectionTestResultRedactor.redact(result, secretValues);
                        attempt.publish(safeResult, resultStore);
                        return ConnectionTestReport.from(safeResult);
                    });
        } catch (TapstateException failure) {
            throw ConnectionTestResultRedactor.redact(failure, secretValues);
        }
    }
}
