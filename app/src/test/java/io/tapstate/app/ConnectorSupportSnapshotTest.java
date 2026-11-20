package io.tapstate.app;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectorSupportSnapshotTest {
    @TempDir
    Path directory;

    @Test
    void startupUsesTheNewerValidManifestAndKeepsTheLocalCopyWhenVersionsMatch() throws IOException {
        var snapshot = new ConnectorSupportSnapshot(directory);
        assertThat(snapshot.restore(manifest(10, "mysql")).version()).isEqualTo(10);

        assertThat(snapshot.restore(manifest(12, "postgres")).entries()).containsOnlyKeys("postgres");
        assertThat(snapshot.restore(manifest(12, "mysql")).entries()).containsOnlyKeys("postgres");
        assertThat(snapshot.restore(manifest(11, "mysql")).entries()).containsOnlyKeys("postgres");
        assertThat(Files.readString(directory.resolve("cache/connector-support.json")))
                .isEqualTo(manifest(12, "postgres"));
    }

    @Test
    void startupUsesTheBundledManifestIfTheLocalSnapshotIsInvalid() throws IOException {
        Path cache = Files.createDirectories(directory.resolve("cache"));
        Files.writeString(cache.resolve("connector-support.json"), "{");
        var snapshot = new ConnectorSupportSnapshot(directory);

        assertThat(snapshot.restore(manifest(12, "postgres")).entries()).containsOnlyKeys("postgres");
        assertThat(Files.readString(cache.resolve("connector-support.json"))).isEqualTo(manifest(12, "postgres"));
    }

    @Test
    void uncommittedTemporarySnapshotIsIgnoredAfterRestart() throws IOException {
        var snapshot = new ConnectorSupportSnapshot(directory);
        snapshot.restore(manifest(10, "mysql"));
        Files.writeString(directory.resolve("cache/.connector-support-fixture.tmp"), manifest(12, "postgres"));

        assertThat(snapshot.restore(manifest(10, "mysql")).entries()).containsOnlyKeys("mysql");
    }

    @Test
    void theSameEligibilitySurvivesBundledStartupAndOfflineSnapshotRecovery() throws IOException {
        var snapshot = new ConnectorSupportSnapshot(directory);
        String legacy = manifest(10, "mysql");
        String eligible = manifestWithEligibility(11, "mysql", "true");

        assertThat(snapshot.restore(legacy).entries().get("mysql").loaderRetirementEligible()).isFalse();
        assertThat(snapshot.restore(eligible).entries().get("mysql").loaderRetirementEligible()).isTrue();
        assertThat(snapshot.restore(legacy).entries().get("mysql").loaderRetirementEligible()).isTrue();
        assertThat(Files.readString(directory.resolve("cache/connector-support.json"))).isEqualTo(eligible);
    }

    static String manifest(long version, String id) {
        return manifest(version, id, "");
    }

    static String manifestWithEligibility(long version, String id, String value) {
        return manifest(version, id, ", \"loaderRetirementEligible\": " + value);
    }

    private static String manifest(long version, String id, String entrySuffix) {
        return """
                {
                  "schemaVersion": 1,
                  "version": %d,
                  "connectors": [{
                    "id": "%s", "name": "Fixture", "roles": ["source", "target"],
                    "minimumTapstateVersion": "0.8.0", "repository": "oss", "artifactId": "%s-connector"%s
                  }]
                }
                """.formatted(version, id, id, entrySuffix);
    }
}
