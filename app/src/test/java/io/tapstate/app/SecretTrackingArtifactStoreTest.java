package io.tapstate.app;

import io.tapstate.core.catalog.TapstateCatalog;
import io.tapstate.core.logging.SecretRedactor;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.ArtifactStore;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class SecretTrackingArtifactStoreTest {

    @Test void productionReadDecorationPreservesManagedTargetAuthority() {
        var delegate = new RecordingStore(); delegate.managed = "views";
        delegate.save(new SourceResource("views", null, "mongodb", Map.of("uri", "mongodb://host/master"), null, null, null, null));
        var decorated = tracking(delegate, new SecretRedactor());
        var query = new io.tapstate.control.core.ArtifactQueryService(decorated);
        assertThat(query.managedDefaultSourceId()).contains("views");
        var sources = new io.tapstate.control.core.SourceProjectionService(org.mockito.Mockito.mock(io.tapstate.control.core.ApplyService.class), query,
                org.mockito.Mockito.mock(io.tapstate.control.core.ArtifactMutationService.class), new io.tapstate.control.core.SourceRepresentation(TapstateCatalog::load));
        assertThat(sources.get("views").metadata().labels()).containsEntry("store", "true");
        var apply = new io.tapstate.control.core.ApplyService(TapstateCatalog::load, decorated,
                new io.tapstate.control.core.AuditGate(record -> {}, java.time.Clock.systemUTC()), org.mockito.Mockito.mock(io.tapstate.spi.store.SchemaStore.class),
                io.tapstate.control.core.PlanAdvisories.none(), io.tapstate.control.core.SchemaDerivation.none());
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> apply.planPreviewWorkspace(List.of(new io.tapstate.control.core.ArtifactDraft(null,
                "version: tapstate/v1\nkind: pipeline\nid: p\nsource: views\nview: {from: orders, primary_key: id}\n"))))
                .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(io.tapstate.control.core.ControlError.MALFORMED_REQUEST));
    }

    @Test void readingANativelyPublishedSourceRegistersSecretsBeforePipelineLogging() {
        var delegate = new RecordingStore(); var redactor = new SecretRedactor();
        var decorated = tracking(delegate, redactor);
        delegate.save(source("imported", "imported-catalog-secret"));
        decorated.get("imported").orElseThrow();
        var sink = new io.tapstate.core.logging.RingBufferLogSink(8, 8);
        var event = org.mockito.Mockito.mock(ch.qos.logback.classic.spi.ILoggingEvent.class);
        org.mockito.Mockito.when(event.getMDCPropertyMap()).thenReturn(Map.of("pipeline_id", "p"));
        org.mockito.Mockito.when(event.getLevel()).thenReturn(ch.qos.logback.classic.Level.INFO);
        org.mockito.Mockito.when(event.getFormattedMessage()).thenReturn("connector emitted imported-catalog-secret");
        new PipelineLogAppender(sink, redactor).append(event);
        assertThat(sink.tail("p")).extracting(io.tapstate.core.logging.LogLine::message).containsExactly("connector emitted ********");
    }

    @Test void managedTrustBundleLinesAreTrackedForPlainAndStructuredLogs() {
        var redactor = new SecretRedactor(); var store = tracking(new RecordingStore(), redactor);
        String ca = "-----BEGIN CERTIFICATE-----\nsentinel-certificate-body\n-----END CERTIFICATE-----";
        store.save(new SourceResource("views", null, "mongodb", Map.of("uri", "mongodb://user:password@host/views", "tlsCaPem", ca), null, null, null, null));
        assertThat(redactor.redact(ca)).doesNotContain("sentinel-certificate-body");
        assertThat(redactor.redact(io.tapstate.core.common.JsonWriter.write(Map.of("tlsCaPem", ca)))).doesNotContain("sentinel-certificate-body");
    }

    @Test
    void registersExistingAndSuccessfullyWrittenSourceSecrets() {
        RecordingStore delegate = new RecordingStore();
        delegate.save(source("existing", "before-start"));
        SecretRedactor redactor = new SecretRedactor();

        SecretTrackingArtifactStore store = tracking(delegate, redactor);
        store.saveAll(List.of(source("applied", "from-apply")));
        store.save(source("created", "from-create"));

        assertThat(redactor.redact("before-start from-apply from-create"))
                .isEqualTo("******** ******** ********");
    }

    @Test
    void tracksSharedTlsAndNestedSshCredentialsForRegisteredSources() {
        RecordingStore delegate = new RecordingStore();
        SecretRedactor redactor = new SecretRedactor();
        SecretTrackingArtifactStore store = tracking(delegate, redactor);

        store.save(new SourceResource(
                "transport", null, "mongodb",
                Map.of("sslKey", "tls-private-key", "sslKeyPassword", "tls-key-password",
                        "__TAPDATA", Map.of("sshPassword", "bastion-password")),
                null, null, null, null));

        assertThat(redactor.redact("tls-private-key tls-key-password bastion-password"))
                .isEqualTo("******** ******** ********");
    }

    @Test
    void tracksOnlyClearlySecretValuesWhenAStoredConnectorIsNoLongerInTheCatalog() {
        RecordingStore delegate = new RecordingStore();
        SecretRedactor redactor = new SecretRedactor();
        SecretTrackingArtifactStore store = tracking(delegate, redactor);
        SourceResource source = new SourceResource(
                "legacy", null, "removed-connector",
                Map.of("host", "db.internal", "port", 3306,
                        "credentials", Map.of("password", "nested-secret")),
                null, null, null, null);

        store.save(source);

        assertThat(redactor.redact("db.internal 3306 nested-secret"))
                .isEqualTo("db.internal 3306 ********");
    }

    @Test
    void tracksAtlasUriUserInfoWithoutMaskingTheConnectionLocation() {
        RecordingStore delegate = new RecordingStore();
        SecretRedactor redactor = new SecretRedactor();
        SecretTrackingArtifactStore store = tracking(delegate, redactor);
        String uri = "mongodb+srv://alice:pa%40ss@cluster.example/test";

        store.save(new SourceResource(
                "atlas", null, "mongodb-atlas", Map.of("isUri", true, "uri", uri),
                null, null, null, null));

        assertThat(redactor.redact(uri + " decoded=pa@ss"))
                .isEqualTo("mongodb+srv://********@cluster.example/test decoded=********");

        store.save(new SourceResource(
                "atlas", null, "mongodb-atlas",
                Map.of("isUri", true, "uri", "mongodb+srv://bob:new-secret@cluster.example/test"),
                null, null, null, null));

        assertThat(redactor.redact("pa@ss new-secret"))
                .isEqualTo("pa@ss ********");
    }

    @Test
    void tracksTheManagedViewsSourceUriWithoutChangingOrdinaryMongoSources() {
        RecordingStore delegate = new RecordingStore();
        SecretRedactor redactor = new SecretRedactor();
        SecretTrackingArtifactStore store = tracking(delegate, redactor);
        String managed = "mongodb://state:managed-secret@db.example/metadata";
        String ordinary = "mongodb://app:ordinary-secret@db.example/user-data";

        store.save(new SourceResource(
                ViewTargetResolver.STATE_STORE_SOURCE_ID, null, "mongodb",
                Map.of("isUri", true, "uri", managed), null, null, null, null));
        store.save(new SourceResource(
                "user-mongo", null, "mongodb", Map.of("isUri", true, "uri", ordinary),
                null, null, null, null));

        assertThat(redactor.redact(managed + " " + ordinary))
                .isEqualTo("mongodb://********@db.example/metadata " + ordinary);
    }

    private static SecretTrackingArtifactStore tracking(ArtifactStore delegate, SecretRedactor redactor) {
        return new SecretTrackingArtifactStore(delegate, TapstateCatalog::load, redactor);
    }

    private static SourceResource source(String id, String password) {
        return new SourceResource(
                id, null, "mysql", Map.of("host", "localhost", "password", password),
                null, null, null, null);
    }

    private static final class RecordingStore implements ArtifactStore {
        String managed;
        @Override public Optional<String> managedDefaultSourceId() { return Optional.ofNullable(managed); }

        private final Map<String, Resource> resources = new LinkedHashMap<>();

        @Override
        public void saveAll(List<Resource> artifacts) {
            artifacts.forEach(artifact -> resources.put(artifact.id(), artifact));
        }

        @Override
        public Optional<Resource> get(String id) {
            return Optional.ofNullable(resources.get(id));
        }

        @Override
        public List<Resource> list() {
            return List.copyOf(resources.values());
        }
    }
}
