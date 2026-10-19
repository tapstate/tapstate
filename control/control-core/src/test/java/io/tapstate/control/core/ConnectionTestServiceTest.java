package io.tapstate.control.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.runtime.probe.ConnectionProbe;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.AuditStore;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.ConnectionTestItem;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.ConnectionTestResult.Outcome;
import io.tapstate.spi.store.ConnectionTestResultStore;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class ConnectionTestServiceTest {

    private static final ConnectionConfig CONFIG =
            new ConnectionConfig("conn-orders", "mongodb", Map.of("uri", "mongodb://localhost"));
    private static final ConnectionTestResult RESULT =
            new ConnectionTestResult("conn-orders", "mongodb", Outcome.PASSED, List.of(), 1_700_000_000_000L);

    @Test
    void probesSavesTheLatestResultAndReturnsIt() {
        AtomicReference<ConnectionConfig> probed = new AtomicReference<>();
        ConnectionProbe probe = config -> {
            probed.set(config);
            return RESULT;
        };
        RecordingResultStore store = new RecordingResultStore();
        RecordingAuditStore auditStore = new RecordingAuditStore();
        ConnectionTestService service = new ConnectionTestService(probe, store, gate(auditStore));

        ConnectionTestReport returned = service.test(CONFIG.id(), CONFIG.connectorId(), CONFIG.settings(), "alice");

        // The returned report is the control-ring projection of the probe's result.
        assertThat(returned.connectionId()).isEqualTo("conn-orders");
        assertThat(returned.connectorId()).isEqualTo("mongodb");
        assertThat(returned.outcome()).isEqualTo(ConnectionTestReport.Outcome.PASSED);
        // The probe was driven with a config carrying exactly the given connection.
        assertThat(probed.get()).isEqualTo(CONFIG);
        // The storage-port result is what gets persisted, unchanged.
        assertThat(store.saved).isSameAs(RESULT);
    }

    @Test
    void redactsConfiguredSecretsFromImmediateAndStoredReportsWithoutDroppingDiagnostics() {
        String password = "database-password-sentinel";
        String tlsKey = "tls-private-key-sentinel";
        String tlsKeyPassword = "tls-key-password-sentinel";
        String sshPassword = "ssh-password-sentinel";
        Map<String, Object> savedConfig = new LinkedHashMap<>();
        savedConfig.put("password", password);
        savedConfig.put("sslKey", tlsKey);
        savedConfig.put("sslKeyPassword", tlsKeyPassword);
        savedConfig.put("__TAPDATA", Map.of("sshPassword", sshPassword));
        SourceResource source = new SourceResource(
                "source-transport", null, "mysql", savedConfig, null, null, null, null);
        ArtifactStore artifacts = new ArtifactStore() {
            @Override
            public void saveAll(List<Resource> resources) { }

            @Override
            public Optional<Resource> get(String id) {
                return source.id().equals(id) ? Optional.of(source) : Optional.empty();
            }

            @Override
            public List<Resource> list() {
                return List.of(source);
            }
        };
        ConnectionTestResult echoed = new ConnectionTestResult(
                "source-transport", "mysql", Outcome.FAILED,
                List.of(new ConnectionTestItem(
                        "Login", ConnectionTestItem.Status.FAILED,
                        "handshake detail includes " + tlsKey,
                        "authentication rejected " + password,
                        "use the configured key password " + tlsKeyPassword + " and SSH password " + sshPassword,
                        "TLS error after " + tlsKey)),
                1_700_000_000_000L);
        RecordingResultStore store = new RecordingResultStore();
        ConnectionTestService service = new ConnectionTestService(
                config -> echoed, store, gate(new RecordingAuditStore()), null,
                new SourceConnectionResolver(artifacts));

        ConnectionTestReport immediate = service.test(
                "source-transport", "mysql", Map.of("host", "preview"), "alice");
        ConnectionTestReport stored = new ConnectionTestResultQueryService(store).find("source-transport").orElseThrow();
        String visible = immediate.checks().toString() + stored.checks();

        assertThat(visible)
                .contains("handshake detail includes", "authentication rejected", "use the configured key password",
                        "and SSH password", "TLS error after", "<redacted>")
                .doesNotContain(password, tlsKey, tlsKeyPassword, sshPassword);
        assertThat(store.saved.items()).singleElement().satisfies(item -> {
            assertThat(item.message()).contains("handshake detail includes", "<redacted>");
            assertThat(item.reason()).contains("authentication rejected", "<redacted>");
            assertThat(item.solution()).contains("use the configured key password", "and SSH password", "<redacted>");
            assertThat(item.connectorErrorCode()).contains("TLS error after", "<redacted>");
        });

        RecordingResultStore legacyStore = new RecordingResultStore();
        legacyStore.saved = echoed;
        ConnectionTestReport legacy = new ConnectionTestResultQueryService(
                legacyStore, new SourceConnectionResolver(artifacts)).find("source-transport").orElseThrow();
        assertThat(legacy.checks().toString())
                .contains("authentication rejected", "use the configured key password", "<redacted>")
                .doesNotContain(password, tlsKey, tlsKeyPassword, sshPassword);
    }

    @Test
    void redactsTransportSecretsFromCodedProbeFailures() {
        String tlsKey = "private-key-sentinel";
        TapstateException failure = new TapstateException(
                ControlError.MALFORMED_REQUEST,
                Map.of("reason", "connector threw after reading " + tlsKey),
                null);
        RecordingResultStore store = new RecordingResultStore();
        ConnectionTestService service = new ConnectionTestService(
                config -> { throw failure; }, store, gate(new RecordingAuditStore()));

        Throwable observed = catchThrowable(() -> service.test(
                "conn-orders", "mysql", Map.of("sslKey", tlsKey), "alice"));

        assertThat(observed).isInstanceOf(TapstateException.class);
        TapstateException safeFailure = (TapstateException) observed;
        assertThat(safeFailure.code()).isEqualTo(failure.code());
        assertThat(safeFailure.args().get("reason"))
                .isEqualTo("connector threw after reading <redacted>");
        assertThat(safeFailure.getMessage()).doesNotContain(tlsKey);
        assertThat(store.saved).isNull();
    }

    @Test
    void auditsTheTestUnderConnectionTestAgainstTheConnectionId() {
        RecordingAuditStore auditStore = new RecordingAuditStore();
        ConnectionTestService service =
                new ConnectionTestService(config -> RESULT, new RecordingResultStore(), gate(auditStore));

        service.test(CONFIG.id(), CONFIG.connectorId(), CONFIG.settings(), "alice");

        assertThat(auditStore.records).hasSize(1);
        AuditRecord record = auditStore.records.get(0);
        assertThat(record.operationId()).isEqualTo("connection.test");
        assertThat(record.principal()).isEqualTo("alice");
        assertThat(record.resourceId()).isEqualTo("conn-orders");
    }

    @Test
    void refusesTheTestWhenTheAuditWriteFailsWithoutProbingOrSaving() {
        AtomicBoolean probed = new AtomicBoolean(false);
        ConnectionProbe probe = config -> {
            probed.set(true);
            return RESULT;
        };
        RecordingResultStore store = new RecordingResultStore();
        AuditStore failing = record -> {
            throw new RuntimeException("audit log unavailable");
        };
        ConnectionTestService service = new ConnectionTestService(probe, store, gate(failing));

        assertThatThrownBy(() -> service.test(CONFIG.id(), CONFIG.connectorId(), CONFIG.settings(), "alice"))
                .isInstanceOf(TapstateException.class);
        assertThat(probed).isFalse();
        assertThat(store.saved).isNull();
    }

    @Test
    void requiresItsCollaborators() {
        ConnectionProbe probe = config -> RESULT;
        RecordingResultStore store = new RecordingResultStore();
        AuditGate auditGate = gate(new RecordingAuditStore());
        assertThatThrownBy(() -> new ConnectionTestService(null, store, auditGate))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ConnectionTestService(probe, null, auditGate))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ConnectionTestService(probe, store, null))
                .isInstanceOf(NullPointerException.class);
    }

    private static AuditGate gate(AuditStore auditStore) {
        return new AuditGate(auditStore, Clock.fixed(Instant.ofEpochMilli(1_700_000_000_000L), ZoneOffset.UTC));
    }

    private static final class RecordingResultStore implements ConnectionTestResultStore {
        private ConnectionTestResult saved;

        @Override
        public void save(ConnectionTestResult result) {
            this.saved = result;
        }

        @Override
        public Optional<ConnectionTestResult> find(String connectionId) {
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
