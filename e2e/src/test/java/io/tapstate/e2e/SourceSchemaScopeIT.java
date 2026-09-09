package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Proves the shipped Source schema read is narrower than its connection-level discovery. */
class SourceSchemaScopeIT {

    private static final String SOURCE_ID = "src_file";

    @TempDir
    private Path connectorJars;

    @TempDir
    private Path sourceDirectory;

    private Path connectorJar;
    private String previousConnectorsDir;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @BeforeEach
    void publishTheConnectorJar() {
        connectorJar = E2eConnectorJar.buildInto(connectorJars);
        previousConnectorsDir = System.setProperty("tapstate.e2e.connectors-dir", connectorJars.toString());
    }

    @AfterEach
    void restoreTheConnectorsDirectory() {
        if (previousConnectorsDir == null) {
            System.clearProperty("tapstate.e2e.connectors-dir");
        } else {
            System.setProperty("tapstate.e2e.connectors-dir", previousConnectorsDir);
        }
    }

    @Test
    void sourceSchemaExcludesAnUnselectedTableWhileConnectionSchemaKeepsIt() {
        writeTable("orders", "id,amount\n1,12\n");
        writeTable("payments", "id,status\n1,paid\n");

        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_source_schema_scope"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, read(connectorJar));
            control.apply(Map.of("src_file.tap.yml", source()));
            control.discoverSchema(
                    SOURCE_ID, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", sourceDirectory.toString()));

            assertThat(control.sourceSchemaTables(SOURCE_ID)).containsExactly("orders");
            assertThat(control.connectionSchemaTables(SOURCE_ID)).containsExactly("orders", "payments");
        }
    }

    private String source() {
        return """
                version: tapstate/v1
                kind: source
                id: src_file
                connector: e2e_file
                config: { uri: "%s" }
                mode: cdc
                tables: [ orders ]
                """.formatted(sourceDirectory);
    }

    private void writeTable(String table, String rows) {
        try {
            Files.writeString(sourceDirectory.resolve(table + ".csv"), rows);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static byte[] read(Path file) {
        try {
            return Files.readAllBytes(file);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
