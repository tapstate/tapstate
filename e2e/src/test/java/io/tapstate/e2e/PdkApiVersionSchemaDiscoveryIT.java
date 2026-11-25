package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A registered connector reaches schema discovery with either the old or the current API declaration.
 * The declarative vocabulary cannot choose an artifact's manifest version or read discovered fields.
 * One in-process server per version exercises the real HTTP surface and isolated connector loader;
 * this fixture does not establish real database connector or floating-point codec compatibility.
 */
class PdkApiVersionSchemaDiscoveryIT {
    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @ValueSource(strings = {"2.0.8", "2.0.11-SNAPSHOT"})
    void aRecognizedApiDeclarationReachesTheDiscoveredFields(String version, @TempDir Path directory)
            throws Exception {
        FileEndpoints.replaceTable(directory.resolve("orders.csv"), "id,name\n1,one\n");
        byte[] artifact = Files.readAllBytes(E2eConnectorJar.buildInto(
                directory, E2eConnectorJar.CONNECTOR_ID, version));
        Map<String, Object> settings = Map.of("uri", directory.toString());

        try (var storage = PreparedNativeStore.create();
                ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, artifact);
            control.apply(Map.of("source.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: versioned_source
                    connector: %s
                    config: { uri: "%s" }
                    tables: [orders]
                    """.formatted(E2eConnectorJar.CONNECTOR_ID, directory)));

            control.discoverSchema("versioned_source", E2eConnectorJar.CONNECTOR_ID, settings);

            assertThat(control.sourceSchemaTables("versioned_source")).containsExactly("orders");
            assertThat(control.sourceSchemaFields("versioned_source", "orders")).containsExactly("id", "name");
        }
    }
}
