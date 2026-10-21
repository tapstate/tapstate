package io.tapstate.control.core;

import io.tapstate.spi.store.ConfigurationExportStore;
import io.tapstate.spi.store.ConfigurationImportStore;
import java.util.Objects;

/** Encrypts a selected configuration snapshot after the original administrative authorization and audit. */
public final class ConfigurationExportService {
    public interface Encoder { String sealExport(ConfigurationExportStore.Snapshot snapshot, String transferKey); }
    public record Result(String transfer, String defaultSourceId) {
        @Override public String toString() { return "ConfigurationExportResult[redacted]"; }
    }
    private final ConfigurationExportStore store;
    private final Encoder encoder;
    private final AuditGate audit;
    public ConfigurationExportService(ConfigurationExportStore store, Encoder encoder, AuditGate audit) {
        this.store = Objects.requireNonNull(store); this.encoder = Objects.requireNonNull(encoder); this.audit = Objects.requireNonNull(audit);
    }
    public Result export(String subject, ConfigurationImportStore.Scope scope, Runnable active,
            ConfigurationExportStore.Selection selection, String transferKey) {
        active.run();
        return audit.dispatch(ControlOperations.CONFIGURATION_EXPORT, new AuditContext(subject, scope.clusterId()), () -> {
            var snapshot = store.guardedBy(active).snapshot(scope, selection);
            var result = new Result(encoder.sealExport(snapshot, transferKey), snapshot.defaultSourceId());
            active.run(); return result;
        });
    }
}
