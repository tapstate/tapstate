package io.tapstate.app;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectorCatalogRefreshTest {
    private static final URI SUPPORT = URI.create("https://config.test/supported-connectors.json");
    private static final URI REPOSITORIES = URI.create("https://config.test/repositories.json");
    private static final String BOM_VERSION = "2.0.11.20261024.3";
    private static final String PREFIX = "io/tapdata/";
    private static final String DIGEST = "a".repeat(64);

    @TempDir
    Path directory;

    @Test
    void refreshAdmitsNewConnectorsAndKeepsAnIssuedDownloadOnTheOldRepository() throws Exception {
        var remote = new RepositoryFixture();
        URI original = URI.create("https://original.test/");
        URI replacement = URI.create("https://replacement.test/releases/");
        remote.publish(original, "mysql", DIGEST);
        remote.publish(replacement, "mysql", DIGEST);
        remote.publish(replacement, "postgres", DIGEST);
        remote.repoint(original);
        remote.files.put(SUPPORT, ConnectorSupportSnapshotTest.manifest(10, "mysql"));
        var refresh = catalog(remote, ConnectorSupportSnapshotTest.manifest(10, "mysql"));
        assertThat(refresh.current().downloads()).isEmpty();

        var first = refresh.refresh();
        var captured = first.downloads().get("mysql");
        assertThat(captured.jar()).isEqualTo(original.resolve(jarPath("mysql")));
        assertThat(first.authentication()).containsEntry("mysql", "GA");
        assertThat(first.downloads()).doesNotContainKey("unlisted");

        remote.repoint(replacement);
        remote.files.put(SUPPORT, ConnectorSupportSnapshotTest.manifest(11, "postgres"));
        var second = refresh.refresh();
        assertThat(second.downloads()).containsOnlyKeys("postgres");
        assertThat(second.authentication()).containsOnlyKeys("postgres").containsEntry("postgres", "Beta");
        assertThat(second.downloads().get("postgres").jar()).isEqualTo(replacement.resolve(jarPath("postgres")));
        assertThat(captured.jar()).isEqualTo(original.resolve(jarPath("mysql")));
        assertThat(catalog(new RepositoryFixture(), ConnectorSupportSnapshotTest.manifest(10, "mysql"))
                .current().support().entries()).containsOnlyKeys("postgres");
    }

    @Test
    void aConflictingReplacementKeepsTheLastSuccessfulCatalogAndSnapshot() throws Exception {
        var remote = new RepositoryFixture();
        URI original = URI.create("https://original.test/");
        URI replacement = URI.create("https://replacement.test/");
        remote.publish(original, "mysql", DIGEST);
        remote.repoint(original);
        remote.files.put(SUPPORT, ConnectorSupportSnapshotTest.manifest(10, "mysql"));
        var refresh = catalog(remote, ConnectorSupportSnapshotTest.manifest(10, "mysql"));
        var first = refresh.refresh();

        remote.publish(replacement, "mysql", "b".repeat(64));
        remote.repoint(replacement);
        remote.files.put(SUPPORT, ConnectorSupportSnapshotTest.manifest(11, "mysql"));
        assertThatThrownBy(refresh::refresh).isInstanceOf(IOException.class).hasMessageContaining("Conflicting SHA-256");
        assertThat(refresh.current()).isSameAs(first);
        assertThat(Files.readString(directory.resolve("cache/connector-support.json")))
                .isEqualTo(ConnectorSupportSnapshotTest.manifest(10, "mysql"));
    }

    @Test
    void refreshReadsTheSameEligibilityAndRejectsInvalidEligibilityWithoutReplacingTheSnapshot() throws Exception {
        var remote = new RepositoryFixture();
        URI repository = URI.create("https://repository.test/");
        remote.publish(repository, "mysql", DIGEST);
        remote.repoint(repository);

        String bundled = ConnectorSupportSnapshotTest.manifestWithEligibility(10, "mysql", "false");
        String online = ConnectorSupportSnapshotTest.manifestWithEligibility(11, "mysql", "true");
        var refresh = catalog(remote, bundled);
        assertThat(refresh.current().support().entries().get("mysql").loaderRetirementEligible()).isFalse();

        remote.files.put(SUPPORT, online);
        var accepted = refresh.refresh();
        assertThat(accepted.support().entries().get("mysql").loaderRetirementEligible()).isTrue();
        assertThat(catalog(new RepositoryFixture(), bundled).current().support().entries())
                .isEqualTo(accepted.support().entries());

        remote.files.put(SUPPORT, ConnectorSupportSnapshotTest.manifestWithEligibility(12, "mysql", "null"));
        assertThatThrownBy(refresh::refresh).isInstanceOf(IOException.class)
                .hasCauseInstanceOf(IllegalArgumentException.class);
        assertThat(refresh.current()).isSameAs(accepted);
        assertThat(Files.readString(directory.resolve("cache/connector-support.json"))).isEqualTo(online);
    }

    @Test
    void anInvalidRepositoryConfigCannotReplaceTheLocalSupportSnapshot() throws Exception {
        var remote = new RepositoryFixture();
        URI repository = URI.create("https://repository.test/");
        remote.publish(repository, "mysql", DIGEST);
        remote.repoint(repository);
        String bundled = ConnectorSupportSnapshotTest.manifest(10, "mysql");
        remote.files.put(SUPPORT, bundled);
        var refresh = catalog(remote, bundled);
        var original = refresh.refresh();
        String settings = remote.files.get(REPOSITORIES);

        remote.files.put(SUPPORT, ConnectorSupportSnapshotTest.manifest(11, "mysql"));
        for (String invalid : List.of(
                "{}",
                settings.replace("oss", ""),
                settings.replace(repository.toString(), "https://repository.test/?ambiguous=1"),
                settings.replace(repository.toString(), "https://repository.test/#fragment"),
                settings.replace(repository.toString(), "https://invalid-user@repository.test/"),
                settings.replace(repository.toString(), "file:///fixture/"),
                settings.replace(repository.toString(), "ftp://repository.test/"),
                settings.replace(BOM_VERSION, ""),
                settings.replace("oss", "missing"))) {
            remote.files.put(REPOSITORIES, invalid);
            assertThatThrownBy(refresh::refresh).isInstanceOf(IOException.class);
            assertThat(refresh.current()).isSameAs(original);
            assertThat(Files.readString(directory.resolve("cache/connector-support.json"))).isEqualTo(bundled);
        }
    }

    @Test
    void anUnsupportedOrUnavailableArtifactIsNotOffered() throws Exception {
        var remote = new RepositoryFixture();
        URI repository = URI.create("https://repository.test/");
        remote.publish(repository, "mysql", DIGEST);
        remote.repoint(repository);
        String support = ConnectorSupportSnapshotTest.manifest(10, "mysql");
        remote.files.put(SUPPORT, support);
        var refresh = catalog(remote, support);

        remote.missing = Set.of(repository.resolve(jarPath("mysql")));
        assertThat(refresh.refresh().downloads()).isEmpty();

        remote.missing = Set.of();
        remote.files.put(SUPPORT, support.replace("\"0.8.0\"", "\"0.9.0\"").replace("\"version\": 10", "\"version\": 11"));
        assertThat(refresh.refresh().downloads()).isEmpty();
        assertThat(refresh.current().support().version()).isEqualTo(11);
    }

    @Test
    void anIncompatibleCurrentBomUsesThePinnedCompatibleHistory() throws Exception {
        var remote = new RepositoryFixture();
        URI repository = URI.create("https://repository.test/releases/");
        remote.repoint(repository);
        remote.publish(repository, "mysql", DIGEST);
        remote.files.put(SUPPORT, ConnectorSupportSnapshotTest.manifest(10, "mysql"));

        var bom = new MavenConnectorCatalog.Coordinate("io.tapdata", "connectors-bom", BOM_VERSION);
        URI activeBom = bom.file(repository, "pom");
        String historicalVersion = "2.0.9.20261023.1";
        var historicalBom = new MavenConnectorCatalog.Coordinate(bom.groupId(), bom.artifactId(), historicalVersion);
        remote.files.put(historicalBom.file(repository, "pom"), remote.files.get(activeBom)
                .replace(BOM_VERSION, historicalVersion)
                .replace(">2.0.11<", ">2.0.9<").replace(">2.11.0<", ">2.9.12<"));
        remote.files.put(activeBom, remote.files.get(activeBom).replace(">2.0.11<", ">2.0.12<"));
        remote.files.put(bom.metadata(repository), """
                <metadata><groupId>io.tapdata</groupId><artifactId>connectors-bom</artifactId>
                  <versioning><versions><version>%s</version></versions></versioning>
                </metadata>
                """.formatted(historicalVersion));

        var refresh = catalog(remote, ConnectorSupportSnapshotTest.manifest(10, "mysql"));
        URI compatibleJar = repository.resolve(PREFIX + "mysql-connector/2.9.12/mysql-connector-2.9.12.jar");
        URI checksum = URI.create(compatibleJar + ".sha256");
        assertThat(refresh.refresh().downloads()).isEmpty();
        remote.files.put(checksum, DIGEST);
        var available = refresh.refresh();
        assertThat(available.downloads().get("mysql").jar()).isEqualTo(compatibleJar);
        assertThat(available.downloads().get("mysql").sha256()).isEqualTo(DIGEST);

        remote.files.put(checksum, "invalid checksum");
        assertThatThrownBy(refresh::refresh).isInstanceOf(IOException.class).hasMessageContaining("Invalid SHA-256");
        assertThat(refresh.current()).isSameAs(available);
    }

    @Test
    void eachBundleRequiresTheExactPublishedConnectorSet() throws Exception {
        var remote = new RepositoryFixture();
        URI repository = URI.create("https://repository.test/");
        var cloud = List.of("mongodb", "mysql", "postgres", "mongodb-atlas");
        var onPrem = List.of("mongodb", "mysql", "postgres", "mongodb-atlas", "oracle", "sqlserver",
                "aws-rds-mysql", "dummy");
        remote.repoint(repository);
        var rows = new HashMap<String, ConnectorSupportManifest.Entry>();
        for (String id : onPrem) {
            remote.publish(repository, id, DIGEST);
            rows.put(id, new ConnectorSupportManifest.Entry(id, id, List.of("source"), "0.8.0", "oss",
                    id + "-connector", false));
        }
        var manifest = new ConnectorSupportManifest(10, rows);
        var repositories = ConnectorReleaseCatalog.repositories(new String(remote.read(REPOSITORIES), StandardCharsets.UTF_8));
        for (var profile : Map.of("cloud", cloud, "onprem", onPrem).entrySet()) {
            var downloads = ConnectorBundleSelection.select(profile.getKey(), manifest, repositories,
                    "0.8.0", "2.0.11-SNAPSHOT", remote);
            assertThat(downloads).extracting(ConnectorReleaseCatalog.Download::connectorId)
                    .containsExactlyElementsOf(profile.getValue());
            assertThat(downloads).allSatisfy(download -> {
                assertThat(download.jar()).isEqualTo(repository.resolve(jarPath(download.connectorId())));
                assertThat(download.sha256()).isEqualTo(DIGEST);
            });
        }

        remote.missing = Set.of(repository.resolve(jarPath("oracle")));
        assertThatThrownBy(() -> ConnectorBundleSelection.select("onprem", manifest, repositories,
                "0.8.0", "2.0.11-SNAPSHOT", remote))
                .isInstanceOf(IOException.class).hasMessage("No compatible published JAR is available for oracle");

        rows.remove("mongodb");
        assertThatThrownBy(() -> ConnectorBundleSelection.select("cloud",
                new ConnectorSupportManifest(11, rows), repositories, "0.8.0", "2.0.11-SNAPSHOT", remote))
                .isInstanceOf(IOException.class).hasMessage("Required connector is outside the official support manifest: mongodb");
    }

    @Test
    void readsAndNormalizesTheAuthenticationManifestAttribute() throws Exception {
        var manifest = new Manifest();
        manifest.getMainAttributes().putValue("Manifest-Version", "1.0");
        manifest.getMainAttributes().putValue("Authentication", "bEtA");
        var bytes = new ByteArrayOutputStream();
        try (var jar = new JarOutputStream(bytes, manifest)) {
            jar.finish();
        }
        assertThat(ConnectorCenterRuntime.releaseStage(ConnectorCatalogHttp.authentication(bytes.toByteArray())))
                .isEqualTo("Beta");
        assertThat(ConnectorCenterRuntime.releaseStage(" ga ")).isEqualTo("GA");
        assertThat(ConnectorCenterRuntime.releaseStage("")).isEqualTo("Alpha");
        assertThat(ConnectorCenterRuntime.releaseStage(null)).isEqualTo("Alpha");
    }

    private ConnectorCatalogRefresh catalog(RepositoryFixture remote, String bundled) throws IOException {
        return new ConnectorCatalogRefresh(new ConnectorSupportSnapshot(directory), bundled, SUPPORT, REPOSITORIES,
                "0.8.0", "2.0.11-SNAPSHOT", remote, Clock.systemUTC());
    }

    private static String jarPath(String id) {
        return PREFIX + id + "-connector/2.11.0/" + id + "-connector-2.11.0.jar";
    }

    private static final class RepositoryFixture implements ConnectorReleaseCatalog.Fetch {
        private final Map<URI, String> files = new HashMap<>();
        private Set<URI> missing = Set.of();

        @Override
        public byte[] read(URI uri) throws IOException {
            String value = files.get(uri);
            if (value == null) throw new ConnectorReleaseCatalog.MissingResource(uri);
            return value.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public boolean available(URI uri) {
            return !missing.contains(uri) && files.containsKey(URI.create(uri + ".sha256"));
        }

        @Override
        public String authentication(URI jar) {
            return jar.getPath().endsWith("mysql-connector-2.11.0.jar") ? "GA" : "Beta";
        }

        private void repoint(URI repository) {
            files.put(REPOSITORIES, """
                    {"schemaVersion": 1, "repositories": {
                      "oss": {"baseUrl": "%s",
                              "bom": {"groupId": "io.tapdata", "artifactId": "connectors-bom", "version": "%s"}}
                    }}
                    """.formatted(repository, BOM_VERSION));
        }

        private void publish(URI repository, String id, String digest) {
            var props = new StringBuilder();
            var managed = new StringBuilder();
            for (String name : List.of("mysql", "postgres", "unlisted", "mongodb", "mongodb-atlas",
                    "oracle", "sqlserver", "aws-rds-mysql", "dummy")) {
                String tag = "tapdata.connector." + name + "-connector.pdk.api.version";
                props.append("<").append(tag).append(">2.0.11</").append(tag).append(">");
                managed.append("""
                        <dependency><groupId>io.tapdata</groupId><artifactId>%s-connector</artifactId>
                        <version>2.11.0</version></dependency>
                        """.formatted(name));
            }
            files.put(repository.resolve(PREFIX + "connectors-bom/" + BOM_VERSION + "/connectors-bom-" + BOM_VERSION + ".pom"), """
                    <project xmlns="http://maven.apache.org/POM/4.0.0">
                      <groupId>io.tapdata</groupId><artifactId>connectors-bom</artifactId><version>%s</version>
                      <properties>%s</properties>
                      <dependencyManagement><dependencies>%s</dependencies></dependencyManagement>
                    </project>
                    """.formatted(BOM_VERSION, props, managed));
            files.put(repository.resolve(jarPath(id) + ".sha256"), digest);
        }
    }
}
