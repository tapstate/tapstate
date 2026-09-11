package io.tapstate.control.core;

import io.tapstate.runtime.probe.SchemaDiscoveryProbe;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.SourceModel;
import java.time.Clock;
import java.util.Map;
import java.util.Objects;

/**
 * The control plane's discover-schema verb: it drives the target connection's schema discovery through
 * the runtime probe — the second member of the synchronous control-to-runtime whitelist — records the
 * normalized model as the connection's latest discovery, and returns it. The whole operation runs under
 * the audit gate: a discovery is an audited write, so an audit record is written before the probe runs
 * and the operation is refused if that write fails (no audit, no execute).
 *
 * <p>The probe is injected: the runtime supplies the seam implementation, and when the control and
 * runtime roles split the same call travels across instances. The schema store keeps only the latest
 * envelope per connection, stamped with this service's clock at persist time; who discovered when is
 * answered by the audit log, not the schema store, so the two never overlap. A probe that cannot run
 * discovery at all throws a coded connector-domain failure, which propagates out of here uncaught — no
 * envelope is stored for a discovery that never produced a model.
 */
public final class SchemaDiscoveryService {

    private final SchemaDiscoveryProbe probe;
    private final SchemaStore schemaStore;
    private final AuditGate auditGate;
    private final Clock clock;
    private final ConnectorConfigValidator configValidator;
    private final SourceConnectionResolver sourceConnections;

    public SchemaDiscoveryService(
            SchemaDiscoveryProbe probe, SchemaStore schemaStore, AuditGate auditGate, Clock clock) {
        this(probe, schemaStore, auditGate, clock, null);
    }

    public SchemaDiscoveryService(
            SchemaDiscoveryProbe probe, SchemaStore schemaStore, AuditGate auditGate, Clock clock,
            ConnectorConfigValidator configValidator) {
        this(probe, schemaStore, auditGate, clock, configValidator, null);
    }

    public SchemaDiscoveryService(
            SchemaDiscoveryProbe probe, SchemaStore schemaStore, AuditGate auditGate, Clock clock,
            ConnectorConfigValidator configValidator, SourceConnectionResolver sourceConnections) {
        this.probe = Objects.requireNonNull(probe, "probe");
        this.schemaStore = Objects.requireNonNull(schemaStore, "schemaStore");
        this.auditGate = Objects.requireNonNull(auditGate, "auditGate");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.configValidator = configValidator;
        this.sourceConnections = sourceConnections;
    }

    /**
     * Discovers the given connection's source model through the runtime probe under the audit gate,
     * saves it as the connection's latest discovery, and returns the surface report. {@code connectionId}
     * is the connection's id (the schema-store key and the audited resource), {@code connectorId} the
     * connector it configures and {@code settings} its configured values. {@code principal} is the
     * authenticated subject the audit record is attributed to. The stored envelope keeps the storage-port
     * shape; the returned report is the control ring's own projection, so the faces never reach into the
     * storage ports.
     */
    public SchemaReport discover(
            String connectionId, String connectorId, Map<String, Object> settings, String principal) {
        ConnectionConfig config = sourceConnections == null
                ? new ConnectionConfig(connectionId, connectorId, settings)
                : sourceConnections.resolve(connectionId, connectorId, settings);
        if (configValidator != null) {
            configValidator.validate(config.connectorId(), config.settings());
        }
        return auditGate.dispatch(
                ControlOperations.CONNECTION_DISCOVER_SCHEMA,
                new AuditContext(principal, config.id()),
                () -> {
                    SourceModel model = probe.discover(config);
                    DiscoveredSourceModel discovered =
                            new DiscoveredSourceModel(config.id(), config.connectorId(), clock.millis(), model);
                    schemaStore.save(discovered);
                    return SchemaReport.from(discovered);
                });
    }
}
