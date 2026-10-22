package io.tapstate.control.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.probe.SchemaDiscoveryProbe;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.AuditStore;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class SchemaDiscoveryServiceTest {

    private static final ConnectionConfig CONFIG =
            new ConnectionConfig("conn-orders", "mysql", Map.of("host", "db.local"));
    private static final SourceModel MODEL = new SourceModel(List.of(
            new SourceTable("orders", List.of(new SourceField("id", "bigint")), List.of("id"), List.of())));
    private static final long NOW = 1_700_000_000_000L;

    @Test
    void discoversStampsSavesTheLatestEnvelopeAndReturnsItsReport() {
        AtomicReference<ConnectionConfig> probed = new AtomicReference<>();
        SchemaDiscoveryProbe probe = config -> {
            probed.set(config);
            return MODEL;
        };
        RecordingSchemaStore store = new RecordingSchemaStore();
        SchemaDiscoveryService service = new SchemaDiscoveryService(
                probe, store, gate(new RecordingAuditStore()), fixedClock(), null,
                SourceConnectionResolver.forAdHocConnections());

        SchemaReport returned = service.discover(CONFIG.id(), CONFIG.connectorId(), CONFIG.settings(), "alice");

        // The returned report is the control-ring projection of the stored envelope.
        assertThat(returned.connectionId()).isEqualTo("conn-orders");
        assertThat(returned.connectorId()).isEqualTo("mysql");
        assertThat(returned.discoveredAt()).isEqualTo(NOW);
        assertThat(returned.tables()).extracting(SchemaReport.Table::name).containsExactly("orders");
        // The probe was driven with a config carrying exactly the given connection.
        assertThat(probed.get()).isEqualTo(CONFIG);
        // The persisted envelope wraps the probe's model unchanged, stamped with the service clock.
        assertThat(store.saved).isEqualTo(new DiscoveredSourceModel("conn-orders", "mysql", NOW, MODEL));
        assertThat(store.saved.model()).isSameAs(MODEL);
    }

    @Test
    void auditsTheDiscoveryUnderConnectionDiscoverSchemaAgainstTheConnectionId() {
        RecordingAuditStore auditStore = new RecordingAuditStore();
        SchemaDiscoveryService service = new SchemaDiscoveryService(
                config -> MODEL, new RecordingSchemaStore(), gate(auditStore), fixedClock(), null,
                SourceConnectionResolver.forAdHocConnections());

        service.discover(CONFIG.id(), CONFIG.connectorId(), CONFIG.settings(), "alice");

        assertThat(auditStore.records).hasSize(1);
        AuditRecord record = auditStore.records.get(0);
        assertThat(record.operationId()).isEqualTo("connection.discover-schema");
        assertThat(record.principal()).isEqualTo("alice");
        assertThat(record.resourceId()).isEqualTo("conn-orders");
    }

    @Test
    void refusesTheDiscoveryWhenTheAuditWriteFailsWithoutProbingOrSaving() {
        AtomicBoolean probed = new AtomicBoolean(false);
        SchemaDiscoveryProbe probe = config -> {
            probed.set(true);
            return MODEL;
        };
        RecordingSchemaStore store = new RecordingSchemaStore();
        AuditStore failing = record -> {
            throw new RuntimeException("audit log unavailable");
        };
        SchemaDiscoveryService service = new SchemaDiscoveryService(
                probe, store, gate(failing), fixedClock(), null,
                SourceConnectionResolver.forAdHocConnections());

        assertThatThrownBy(() -> service.discover(CONFIG.id(), CONFIG.connectorId(), CONFIG.settings(), "alice"))
                .isInstanceOf(TapstateException.class);
        assertThat(probed).isFalse();
        assertThat(store.saved).isNull();
    }

    @Test
    void redactsConnectorDiagnosticsAndDropsTheCauseBeforeReturningDiscoveryErrors() {
        String privateKey = "schema-private-key-sentinel";
        TapstateException failure = new TapstateException(
                ControlError.MALFORMED_REQUEST,
                Map.of("reason", "cannot parse private key: " + privateKey),
                new IllegalStateException("key content " + privateKey));
        SchemaDiscoveryService service = new SchemaDiscoveryService(
                config -> { throw failure; }, new RecordingSchemaStore(),
                gate(new RecordingAuditStore()), fixedClock(), null,
                SourceConnectionResolver.forAdHocConnections());

        assertThatThrownBy(() -> service.discover(
                "source-1", "mysql", Map.of("sslKey", privateKey), "alice"))
                .isInstanceOfSatisfying(TapstateException.class, safe -> {
                    assertThat(safe.args().get("reason")).isEqualTo("cannot parse private key: <redacted>");
                    assertThat(safe.getCause()).isNull();
                    assertThat(safe.getMessage()).doesNotContain(privateKey);
                });
    }

    @Test
    void requiresItsCollaborators() {
        SchemaDiscoveryProbe probe = config -> MODEL;
        RecordingSchemaStore store = new RecordingSchemaStore();
        AuditGate auditGate = gate(new RecordingAuditStore());
        Clock clock = fixedClock();
        SourceConnectionResolver resolver = SourceConnectionResolver.forAdHocConnections();
        assertThatThrownBy(() -> new SchemaDiscoveryService(null, store, auditGate, clock, null, resolver))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new SchemaDiscoveryService(probe, null, auditGate, clock, null, resolver))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new SchemaDiscoveryService(probe, store, null, clock, null, resolver))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new SchemaDiscoveryService(probe, store, auditGate, null, null, resolver))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new SchemaDiscoveryService(probe, store, auditGate, clock, null, null))
                .isInstanceOf(NullPointerException.class);
    }

    private static Clock fixedClock() {
        return Clock.fixed(Instant.ofEpochMilli(NOW), ZoneOffset.UTC);
    }

    private static AuditGate gate(AuditStore auditStore) {
        return new AuditGate(auditStore, fixedClock());
    }

    private static final class RecordingSchemaStore implements SchemaStore {
        private DiscoveredSourceModel saved;

        @Override
        public void save(DiscoveredSourceModel discovered) {
            this.saved = discovered;
        }

        @Override
        public Optional<DiscoveredSourceModel> get(String connectionId) {
            return Optional.ofNullable(saved);
        }
    }

    private static final class RecordingAuditStore implements AuditStore {
        private final List<AuditRecord> records = new ArrayList<>();

        @Override
        public void record(AuditRecord record) {
            records.add(record);
        }
    }
}
