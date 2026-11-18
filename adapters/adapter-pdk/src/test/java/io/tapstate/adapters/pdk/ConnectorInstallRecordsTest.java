package io.tapstate.adapters.pdk;

import io.tapstate.spi.store.ConnectorArtifactMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectorInstallRecordsTest {
    private static final String HASH = "a".repeat(64);

    @TempDir
    Path plugins;

    @Test
    void retainsTheInstallationFactsAcrossThePendingAndCompleteCommit() throws IOException {
        var records = new ConnectorInstallRecords(plugins);
        var pending = new ConnectorInstallRecords.Installation("mongodb", "2.11.0",
                ConnectorArtifactMetadata.Origin.DOWNLOAD, 16063838L, ConnectorInstallRecords.State.PENDING);
        assertThat(records.read(HASH)).isEmpty();

        records.save(HASH, pending);

        assertThat(new ConnectorInstallRecords(plugins).read(HASH)).contains(pending);
        records.save(HASH, pending.complete());

        assertThat(new ConnectorInstallRecords(plugins).read(HASH)).contains(pending.complete());
        try (var files = Files.list(plugins)) {
            assertThat(files.map(path -> path.getFileName().toString())).containsExactly(HASH + ".json");
        }
    }

    @Test
    void ignoresAnUnpublishedTemporaryFileAndReportsAnInvalidRecord() throws IOException {
        var records = new ConnectorInstallRecords(plugins);
        Files.writeString(plugins.resolve("." + HASH + "-fixture.tmp"), "{}");

        assertThat(records.read(HASH)).isEmpty();

        Files.writeString(plugins.resolve(HASH + ".json"), "{}");
        assertThatThrownBy(() -> records.read(HASH)).isInstanceOf(IOException.class);
    }
}
