package io.tapstate.control.core;

import io.tapstate.runtime.probe.ConnectionProbe;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.ConnectionTestResultStore;
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

    private final ConnectionProbe probe;
    private final ConnectionTestResultStore resultStore;
    private final AuditGate auditGate;
    private final ConnectorConfigValidator configValidator;
    private final SourceConnectionResolver sourceConnections;

    public ConnectionTestService(
            ConnectionProbe probe, ConnectionTestResultStore resultStore, AuditGate auditGate) {
        this(probe, resultStore, auditGate, null);
    }

    public ConnectionTestService(
            ConnectionProbe probe, ConnectionTestResultStore resultStore, AuditGate auditGate,
            ConnectorConfigValidator configValidator) {
        this(probe, resultStore, auditGate, configValidator, null);
    }

    public ConnectionTestService(
            ConnectionProbe probe, ConnectionTestResultStore resultStore, AuditGate auditGate,
            ConnectorConfigValidator configValidator, SourceConnectionResolver sourceConnections) {
        this.probe = Objects.requireNonNull(probe, "probe");
        this.resultStore = Objects.requireNonNull(resultStore, "resultStore");
        this.auditGate = Objects.requireNonNull(auditGate, "auditGate");
        this.configValidator = configValidator;
        this.sourceConnections = sourceConnections;
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
        ConnectionConfig config = sourceConnections == null
                ? new ConnectionConfig(connectionId, connectorId, settings)
                : sourceConnections.resolve(connectionId, connectorId, settings);
        if (configValidator != null) {
            configValidator.validate(config.connectorId(), config.settings());
        }
        return auditGate.dispatch(
                ControlOperations.CONNECTION_TEST,
                new AuditContext(principal, config.id()),
                () -> {
                    ConnectionTestResult result = probe.probe(config);
                    resultStore.save(result);
                    return ConnectionTestReport.from(result);
                });
    }
}
