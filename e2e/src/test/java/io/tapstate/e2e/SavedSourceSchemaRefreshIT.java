package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Proves schema discovery restores a saved Source's secret after the API redacts its view. */
class SavedSourceSchemaRefreshIT {

    private static final String SOURCE_ID = "src_saved";
    private static final String PASSWORD = "saved-password";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aRedactedSavedSourceStillDiscoversWithItsPersistedPassword(Tiers tier, @TempDir Path directory) {
        writeTable(directory);
        try (ServerHandle server = tier.launch(SharedMongo.replicaSetUrl(
                "saved_source_schema_refresh_" + tier.name().toLowerCase(Locale.ROOT)))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID,
                    read(E2eConnectorJar.buildInto(directory)));
            control.apply(Map.of("src_saved.tap.yml", sourceYaml(directory)));

            Map<String, Object> saved = control.source(SOURCE_ID);
            @SuppressWarnings("unchecked")
            Map<String, Object> redacted = (Map<String, Object>) saved.get("config");
            assertThat(redacted).doesNotContainKey("password");

            control.discoverSchema(
                    String.valueOf(saved.get("id")), String.valueOf(saved.get("connector")), redacted);

            assertThat(control.sourceSchemaTables(SOURCE_ID)).containsExactly("orders");
        }
    }

    private static String sourceYaml(Path directory) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s", password: "%s", require_password: true }
                mode: cdc
                tables: [ orders ]
                """.formatted(SOURCE_ID, E2eConnectorJar.CONNECTOR_ID, directory, PASSWORD);
    }

    private static void writeTable(Path directory) {
        try {
            Files.writeString(directory.resolve("orders.csv"), "id,name\n1,one\n");
        } catch (IOException e) {
            throw new UncheckedIOException("cannot write the source table", e);
        }
    }

    private static byte[] read(Path file) {
        try {
            return Files.readAllBytes(file);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read the connector jar", e);
        }
    }
}
