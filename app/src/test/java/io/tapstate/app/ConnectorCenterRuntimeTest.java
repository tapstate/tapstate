package io.tapstate.app;

import com.sun.net.httpserver.HttpServer;
import io.tapstate.adapters.pdk.ConnectorError;
import io.tapstate.adapters.pdk.LocalConnectorArtifacts;
import io.tapstate.control.core.ConnectorCatalogError;
import io.tapstate.control.core.ControlError;
import io.tapstate.core.catalog.ConnectorCatalogEntry;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ConnectorArtifactMetadata;
import io.tapstate.spi.store.ConnectorArtifactMetadata.Origin;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.RegistrationOutcome;
import io.tapstate.spi.store.RegistrationSource;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ConnectorCenterRuntimeTest {
    private static final String ID = "postgres";
    private static final String INSTALLED_HASH = "a".repeat(64);
    private static final String DOWNLOAD_HASH = "b".repeat(64);
    private static final byte[] ARTIFACT = {1, 2, 3};
    private static final Instant REFRESHED_AT = Instant.parse("2026-11-23T00:00:00Z");

    @Test
    void catalogPreservesLocalVersionsWhenRefreshCannotOfferDownloads() throws Exception {
        var local = mock(LocalConnectorArtifacts.class);
        var remote = catalogAt(URI.create("http://127.0.0.1:1/postgres.jar"));
        when(remote.admittedIds(any())).thenReturn(List.of(ID, "dummy"));
        when(remote.reachable()).thenReturn(false, true);
        var current = artifact(ID, "2.11.0", INSTALLED_HASH, "pg.svg");
        var former = artifact("formerly-supported", "2.11.0", "c".repeat(64), null);
        var inventory = new LocalConnectorArtifacts.Inventory(List.of(current, former), List.of(current, former));
        when(local.inventory()).thenReturn(inventory);
        when(local.manifestAuthentication(INSTALLED_HASH)).thenReturn("bEtA");

        try (var center = new ConnectorCenterRuntime(remote, local, false)) {
            var offline = center.catalog();
            assertThat(offline).containsEntry("downloadCenterAvailable", false)
                    .containsEntry("uploadAvailable", true).containsEntry("supportVersion", 10L)
                    .containsEntry("refreshedAt", REFRESHED_AT.toString());
            assertThat(row(offline, ID)).containsEntry("defaultSha256", INSTALLED_HASH)
                    .containsEntry("downloadable", false).containsEntry("updateAvailable", true)
                    .containsEntry("downloadHash", DOWNLOAD_HASH).containsEntry("icon", "pg.svg")
                    .containsEntry("properties", Map.of("authentication", "Beta"));
            assertThat(row(offline, "formerly-supported")).containsEntry("downloadable", false)
                    .containsEntry("defaultSha256", former.sha256()).doesNotContainKey("roles");
            assertThat(connectorRows(offline)).hasSize(2);

            var online = center.refresh();
            verify(remote).refreshNow();
            assertThat(row(online, ID)).containsEntry("downloadable", true);
            verify(local).manifestAuthentication(INSTALLED_HASH);
        }
    }

    @Test
    void theFirstInstallUsesRemoteStageAndHasNoDefaultArtifact() throws Exception {
        var local = mock(LocalConnectorArtifacts.class);
        when(local.inventory()).thenReturn(new LocalConnectorArtifacts.Inventory(List.of(), List.of()));
        var remote = catalogAt(URI.create("http://127.0.0.1:1/postgres.jar"));
        when(remote.admittedIds(any())).thenReturn(List.of(ID));
        try (var center = new ConnectorCenterRuntime(remote, local, true)) {
            var catalog = center.catalog();
            assertThat(catalog).containsEntry("uploadAvailable", false);
            assertThat(row(catalog, ID)).containsEntry("downloadable", true)
                    .containsEntry("installedVersions", List.of()).containsEntry("updateAvailable", false)
                    .containsEntry("properties", Map.of("authentication", "GA"))
                    .doesNotContainKey("defaultSha256");
            verify(local, never()).manifestAuthentication(anyString());
        }
    }

    @Test
    void downloadTracksBytesAndReportsTheAdmittedArtifactAfterInstallation() throws Exception {
        var repository = repository(200);
        try {
            var local = mock(LocalConnectorArtifacts.class);
            var staged = stage(local);
            when(staged.install(Origin.DOWNLOAD, ID, DOWNLOAD_HASH)).thenReturn(registered());
            var remote = catalogAt(uri(repository));
            try (var center = new ConnectorCenterRuntime(remote, local, true)) {
                var completed = complete(center, center.download(ID, DOWNLOAD_HASH));
                assertThat(completed).containsEntry("state", "INSTALLED")
                        .containsEntry("bytesTransferred", (long) ARTIFACT.length)
                        .containsEntry("totalBytes", (long) ARTIFACT.length)
                        .containsEntry("connectorId", ID).containsEntry("sha256", DOWNLOAD_HASH)
                        .containsEntry("pdkApiVersion", "2.0.11").containsEntry("newlyRegistered", true);
                await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> verify(staged).close());
            }
        } finally {
            repository.stop(0);
        }
    }

    @Test
    void remoteFailuresRemainTerminalWithoutRegisteringAnotherArtifact() throws Exception {
        var repository = repository(404);
        try {
            var local = mock(LocalConnectorArtifacts.class);
            var remote = catalogAt(uri(repository));
            try (var center = new ConnectorCenterRuntime(remote, local, false)) {
                assertThat(complete(center, center.download(ID, DOWNLOAD_HASH)))
                        .containsEntry("state", "FAILED").containsEntry("errorCode", ControlError.UNREACHABLE.code())
                        .doesNotContainKey("sha256");
            }
        } finally {
            repository.stop(0);
        }

        repository = repository(200);
        try {
            var local = mock(LocalConnectorArtifacts.class);
            var staged = stage(local);
            when(staged.install(Origin.DOWNLOAD, ID, DOWNLOAD_HASH)).thenThrow(
                    new TapstateException(ConnectorError.LOAD_FAILED, Map.of("connector", ID), null));
            try (var center = new ConnectorCenterRuntime(catalogAt(uri(repository)), local, false)) {
                assertThat(complete(center, center.download(ID, DOWNLOAD_HASH))).containsEntry("state", "FAILED")
                        .containsEntry("errorCode", ConnectorError.LOAD_FAILED.code()).doesNotContainKey("sha256");
                await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> verify(staged).close());
            }
        } finally {
            repository.stop(0);
        }
    }

    @Test
    void catalogRefusalsKeepTheInstallationBoundaries() throws Exception {
        var local = mock(LocalConnectorArtifacts.class);
        var remote = catalogAt(URI.create("http://127.0.0.1:1/postgres.jar"));
        when(remote.reachable()).thenReturn(false);
        try (var center = new ConnectorCenterRuntime(remote, local, true)) {
            refusal(() -> center.download(ID, INSTALLED_HASH), ConnectorCatalogError.DOWNLOAD_TARGET_CHANGED.code());
            refusal(() -> center.download("dummy", null), ConnectorCatalogError.NOT_FOUND.code());
            refusal(() -> center.download("unlisted", null), ConnectorCatalogError.NOT_FOUND.code());
            refusal(() -> center.download(ID, DOWNLOAD_HASH), ControlError.UNREACHABLE.code());
            refusal(() -> center.upload(new ByteArrayInputStream(ARTIFACT), ARTIFACT.length, ID),
                    ConnectorError.CLOUD_UPLOAD_DISABLED.code());
            refusal(() -> center.operation("unknown"), ConnectorCatalogError.NOT_FOUND.code());
        }

        var staged = stage(local);
        when(staged.install(Origin.UPLOAD, ID, null)).thenReturn(registered());
        try (var center = new ConnectorCenterRuntime(remote, local, false)) {
            assertThat(complete(center, center.upload(new ByteArrayInputStream(ARTIFACT), ARTIFACT.length, ID)))
                    .containsEntry("state", "INSTALLED").containsEntry("sha256", DOWNLOAD_HASH);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> verify(staged).close());
        }
    }

    @Test
    void failedUploadsReleaseTheirSlotsAndBoundTheOperationHistory() {
        var local = mock(LocalConnectorArtifacts.class);
        var failure = new TapstateException(ConnectorError.LOAD_FAILED, Map.of("connector", ID), null);
        when(local.stage(any(), any())).thenThrow(failure);
        try (var center = new ConnectorCenterRuntime(mock(ConnectorCatalogBackground.class), local, false)) {
            var first = center.upload(new ByteArrayInputStream(ARTIFACT), ARTIFACT.length, null);
            assertThat(first).containsEntry("state", "FAILED").containsEntry("errorCode", failure.code().code());
            for (int i = 0; i < 127; i++) {
                assertThat(center.upload(new ByteArrayInputStream(ARTIFACT), ARTIFACT.length, null))
                        .containsEntry("state", "FAILED");
            }
            assertThat(center.operation((String) first.get("operationId"))).isEqualTo(first);
            var next = center.upload(new ByteArrayInputStream(ARTIFACT), ARTIFACT.length, null);
            assertThat(center.operation((String) next.get("operationId"))).isEqualTo(next);
            refusal(() -> center.operation((String) first.get("operationId")), ConnectorCatalogError.NOT_FOUND.code());
        }
    }

    private static void refusal(Runnable attempt, String code) {
        assertThatThrownBy(attempt::run).isInstanceOfSatisfying(
                TapstateException.class, rejected -> assertThat(rejected.code().code()).isEqualTo(code));
    }

    private static ConnectorCatalogBackground catalogAt(URI jar) throws Exception {
        var support = ConnectorSupportManifest.read(ConnectorSupportSnapshotTest.manifest(10, ID));
        var coordinate = new MavenConnectorCatalog.Coordinate("io.tapdata", ID + "-connector", "2.11.1");
        var release = new MavenConnectorCatalog.Release(coordinate, "2.0.11");
        var download = new ConnectorReleaseCatalog.Download(ID, release, jar, DOWNLOAD_HASH);
        var remote = mock(ConnectorCatalogBackground.class);
        when(remote.current()).thenReturn(new ConnectorCatalogRefresh.Catalog(support, Map.of(ID, download),
                Map.of(ID, "GA"), REFRESHED_AT));
        when(remote.reachable()).thenReturn(true);
        return remote;
    }

    private static LocalConnectorArtifacts.StagedArtifact stage(LocalConnectorArtifacts local) {
        var staged = mock(LocalConnectorArtifacts.StagedArtifact.class);
        when(local.stage(any(), any())).thenAnswer(invocation -> {
            byte[] bytes = invocation.getArgument(0, InputStream.class).readAllBytes();
            assertThat(bytes).containsExactly(ARTIFACT);
            invocation.getArgument(1, LongConsumer.class).accept(bytes.length);
            return staged;
        });
        return staged;
    }

    private static RegistrationOutcome registered() {
        return new RegistrationOutcome(new ConnectorRegistration(ID, DOWNLOAD_HASH, "2.0.11", RegistrationSource.REGISTER), true);
    }

    private static ConnectorArtifactMetadata artifact(String id, String version, String hash, String icon) {
        var entry = new ConnectorCatalogEntry(id, id, id, icon, null, List.of(), null, null, false, List.of(), null);
        return new ConnectorArtifactMetadata(entry, hash, version, "2.0.11", Origin.UPLOAD, 100, Optional.empty());
    }

    private static HttpServer repository(int status) throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/postgres.jar", exchange -> {
            try (exchange) {
                exchange.sendResponseHeaders(status, status == 200 ? ARTIFACT.length : -1);
                if (status == 200) exchange.getResponseBody().write(ARTIFACT);
            }
        });
        server.start();
        return server;
    }

    private static URI uri(HttpServer server) {
        return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/postgres.jar");
    }

    private static Map<String, Object> complete(ConnectorCenterRuntime center, Map<String, Object> accepted) {
        String id = (String) accepted.get("operationId");
        return await().atMost(Duration.ofSeconds(5)).until(() -> center.operation(id),
                operation -> List.of("FAILED", "INSTALLED").contains(operation.get("state")));
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> connectorRows(Map<String, Object> catalog) {
        return (List<Map<String, Object>>) catalog.get("connectors");
    }

    private static Map<String, Object> row(Map<String, Object> catalog, String id) {
        return connectorRows(catalog).stream().filter(connector -> id.equals(connector.get("id")))
                .findFirst().orElseThrow();
    }
}
