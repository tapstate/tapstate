package io.tapstate.e2e;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ConnectorJarsTest {

    @Test
    void exactMongoIdDoesNotCollideWithTheAtlasVariant(@TempDir Path directory) throws IOException {
        Files.write(directory.resolve("mongodb-connector.jar"), new byte[] {1});
        Files.write(directory.resolve("mongodb-atlas-connector.jar"), new byte[] {2});

        assertThat(bytesFor(directory, "mongodb")).containsExactly((byte) 1);
        assertThat(bytesFor(directory, "mongodb-atlas")).containsExactly((byte) 2);
    }

    @Test
    void atlasVariantCannotStandInForMissingMongoId(@TempDir Path directory) throws IOException {
        Files.write(directory.resolve("mongodb-atlas-connector.jar"), new byte[] {2});

        assertThatThrownBy(() -> bytesFor(directory, "mongodb"))
                .isInstanceOf(EnvelopeException.class)
                .hasMessageContaining("no mongodb connector jar");
    }

    @Test
    void theHarnessOwnJarAndVersionedConnectorAssetsRemainResolvable(@TempDir Path directory) throws IOException {
        Files.write(directory.resolve("e2e_file.jar"), new byte[] {1});
        Files.write(directory.resolve("mysql-connector-1.0.jar"), new byte[] {2});

        assertThat(bytesFor(directory, "e2e_file")).containsExactly((byte) 1);
        assertThat(bytesFor(directory, "mysql")).containsExactly((byte) 2);
    }

    private static byte[] bytesFor(Path directory, String id) {
        String previous = System.getProperty("tapstate.e2e.connectors-dir");
        System.setProperty("tapstate.e2e.connectors-dir", directory.toString());
        try {
            return ConnectorJars.bytesFor(id);
        } finally {
            if (previous == null) {
                System.clearProperty("tapstate.e2e.connectors-dir");
            } else {
                System.setProperty("tapstate.e2e.connectors-dir", previous);
            }
        }
    }
}
