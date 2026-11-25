package io.tapstate.e2e;

import io.tapstate.core.event.RowValueError;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Connector rows with colliding Integer/String map keys fail through the real HTTP status surface.
 * The declarative seed vocabulary cannot represent these distinct Java keys, so the harness connector
 * supplies them before the host's normal PDK conversion and snapshot handoff. Ordinary rows still cross.
 */
class AMixedMapKeyRowFailsBeforeDeliveryIT {

    private static final String PIPELINE = "map_keys";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aMixedKeyRowFailsWithItsDiagnosticWhileAnOrdinaryRowIsDelivered(
            boolean mixedKeys, @TempDir Path directory) throws Exception {
        Path source = Files.createDirectory(directory.resolve("source"));
        Path target = Files.createDirectory(directory.resolve("target"));
        Files.writeString(source.resolve("orders.csv"), "id,seq,dict\n1,1,ordinary\n");
        Path written = target.resolve("orders.csv");
        Path jar = E2eConnectorJar.buildInto(directory);
        try (PreparedNativeStore storage = PreparedNativeStore.create();
                ServerHandle server = InProcessServer.startPrepared(storage)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(jar));
            Map<String, Object> config = Map.of("uri", source.toString(), "mixed_map_keys", mixedKeys);
            control.discoverSchema("src_orders", E2eConnectorJar.CONNECTOR_ID, config);
            Map<String, String> resources = new LinkedHashMap<>();
            resources.put("source.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: src_orders
                    connector: e2e_file
                    config: { uri: "%s", mixed_map_keys: %s }
                    mode: cdc
                    tables: [orders]
                    """.formatted(source, mixedKeys));
            resources.put("target.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: tgt_orders
                    connector: e2e_file
                    config: { uri: "%s" }
                    """.formatted(target));
            resources.put("pipeline.tap.yml", """
                    version: tapstate/v1
                    kind: pipeline
                    id: map_keys
                    source: [{ id: src_orders, srs: true }]
                    settings: { read_mode: snapshot_only }
                    serve:
                      from: orders
                      sync: [{ source: tgt_orders }]
                    """);
            control.apply(resources);
            control.lifecycle(PIPELINE, LifecycleVerb.START);

            if (mixedKeys) {
                ControlPlane.Failure failure = Await.answered("the invalid row's published failure",
                        () -> control.failure(PIPELINE));
                assertThat(failure.code()).isEqualTo(RowValueError.NON_STRING_MAP_KEY.code());
                assertThat(failure.params()).containsEntry("path", "$/after/dict")
                        .containsEntry("keyType", "java.lang.Integer");
                assertThat(control.state(PIPELINE)).contains(PipelineState.FAILED);
                assertThat(written).doesNotExist();
            } else {
                Await.until("the ordinary row to reach its target", () -> Files.exists(written),
                        () -> "state=" + control.state(PIPELINE) + ", failure=" + control.failure(PIPELINE));
                assertThat(Files.readAllLines(written)).containsExactly("id,seq,dict", "1,1,ordinary");
                assertThat(control.failure(PIPELINE)).isEmpty();
            }
        }
    }
}
