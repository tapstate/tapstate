package io.tapstate.app;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MavenConnectorCatalogTest {
    private static final MavenConnectorCatalog.Coordinate BOM =
            new MavenConnectorCatalog.Coordinate("io.tapdata", "connectors-bom", "2.0.11.20261024.3");

    @Test
    void derivesReleaseUrlsFromTheBomCoordinates() throws IOException {
        var releases = MavenConnectorCatalog.read(xml(bom("postgres-connector", "2.11.1", "2.0.11-SNAPSHOT")), BOM);

        assertThat(releases).containsExactly(release("postgres-connector", "2.11.1", "2.0.11-SNAPSHOT"));
        assertThat(releases.getFirst().coordinate().file(URI.create("https://example.test/releases/"), "jar.sha256"))
                .isEqualTo(URI.create("https://example.test/releases/io/tapdata/postgres-connector/2.11.1/postgres-connector-2.11.1.jar.sha256"));
        assertThat(BOM.metadata(URI.create("https://example.test/releases/")))
                .isEqualTo(URI.create("https://example.test/releases/io/tapdata/connectors-bom/maven-metadata.xml"));
    }

    @Test
    void rejectsArtifactsAboveTheHostGenerationBeforeCheckingTheAbiLevel() {
        var eligible = MavenConnectorCatalog.compatibleReleases(List.of(
                release("mongodb-connector", "2.10.0", "2.0.10"),
                release("mongodb-connector", "2.9.9", "2.9"),
                release("mongodb-connector", "2.9.12", "2.0.9-SNAPSHOT"),
                release("mongodb-connector", "2.8.20", "2.0.8"),
                release("mysql-connector", "2.9.100", "2.0.9")
        ), "mongodb-connector", "2.0.9-20261024.103212-1");

        assertThat(eligible).extracting(release -> release.coordinate().version())
                .containsExactly("2.9.12", "2.9.9", "2.8.20");
    }

    @Test
    void fallsBackToAnOlderGenerationOnlyWhenItHasAnEligibleRelease() {
        var releases = List.of(
                release("mongodb-connector", "2.11.1", "2.0.11"),
                release("mongodb-connector", "2.8.1", "2.0.8"),
                release("mongodb-connector", "2.8.2", "2.0.8"),
                release("mongodb-connector", "2.6.4", "unknown"));
        assertThat(MavenConnectorCatalog.compatibleReleases(releases, "mongodb-connector", "2.0.9"))
                .extracting(release -> release.coordinate().version()).containsExactly("2.8.2", "2.8.1");
        assertThat(MavenConnectorCatalog.compatibleReleases(releases, "mongodb-connector", "2.0.7")).isEmpty();
    }

    @Test
    void listsOnlyPublishedHistoricalBomsOlderThanTheConfiguredRelease() throws IOException {
        assertThat(MavenConnectorCatalog.readBomVersions(xml("""
                <metadata>
                  <groupId>io.tapdata</groupId>
                  <artifactId>connectors-bom</artifactId>
                  <versioning><versions>
                    <version>2.0.11.20261024.4</version>
                    <version>2.0.11.20261024.3</version>
                    <version>2.0.11.20261024.1</version>
                    <version>2.0.11.20261024.2</version>
                    <version>2.0.11.20261024.2-SNAPSHOT</version>
                  </versions></versioning>
                </metadata>
                """), BOM)).containsExactly("2.0.11.20261024.2", "2.0.11.20261024.1");
    }

    @Test
    void rejectsAMismatchedBomAndExternalXmlDeclarations() {
        assertThatThrownBy(() -> MavenConnectorCatalog.read(xml(bom("mongodb-connector", "2.11.0", "2.0.11")
                .replace(BOM.version(), "2.0.11.20261024.2")), BOM)).isInstanceOf(IOException.class);
        assertThatThrownBy(() -> MavenConnectorCatalog.read(xml("""
                <!DOCTYPE project [
                    <!ENTITY external SYSTEM "file:///tmp/connector-bom.xml">
                ]>
                <project>&external;</project>
                """), BOM)).isInstanceOf(IOException.class);
    }

    private static ByteArrayInputStream xml(String value) {
        return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String bom(String artifactId, String version, String api) {
        return """
                <project xmlns="http://maven.apache.org/POM/4.0.0">
                  <groupId>io.tapdata</groupId><artifactId>connectors-bom</artifactId>
                  <version>%s</version>
                  <properties><tapdata.connector.%s.pdk.api.version>%s</tapdata.connector.%s.pdk.api.version></properties>
                  <dependencyManagement><dependencies><dependency>
                    <groupId>io.tapdata</groupId><artifactId>%s</artifactId><version>%s</version>
                  </dependency></dependencies></dependencyManagement>
                </project>
                """.formatted(BOM.version(), artifactId, api, artifactId, artifactId, version);
    }

    private static MavenConnectorCatalog.Release release(String id, String version, String api) {
        return new MavenConnectorCatalog.Release(new MavenConnectorCatalog.Coordinate("io.tapdata", id, version), api);
    }
}
