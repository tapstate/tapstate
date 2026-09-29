package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.assertThat;

/** A pipeline's first full load applies its target policy before any source row lands. */
class AFirstStartHonorsOnFullLoadIT {
    private static final String TABLE = "orders";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void clearRemovesExistingRowsAndFailRefusesThemOnTheFirstStart(@TempDir Path directory) throws Exception {
        String storeUri = SharedMongo.replicaSetUrl("first_start_on_full_load");
        try (ServerHandle server = InProcessServer.start(storeUri)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(
                    E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(E2eConnectorJar.buildInto(directory)));

            runClear(control, directory);
            runFail(control, directory);
        }
    }

    private static void runClear(ControlPlane control, Path directory) throws Exception {
        String pipeline = "first_start_clear";
        EndpointAddress source = endpoint(directory, "clear_source");
        EndpointAddress target = endpoint(directory, "clear_target");
        FileEndpoints files = new FileEndpoints();
        files.seed(source, TABLE, SeedRows.generated(2));
        files.seed(target, TABLE, List.of(Map.of("id", 99L, "seq", 99L)));

        control.discoverSchema("clear_source", E2eConnectorJar.CONNECTOR_ID, source.settings());
        control.apply(workspace(pipeline, "clear_source", source, "clear_target", target, "clear"));
        control.lifecycle(pipeline, LifecycleVerb.START);

        Await.until("the first clear load to replace the seeded target rows", Duration.ofMinutes(1),
                () -> files.count(target, TABLE) == 2
                        && files.fetch(target, TABLE, Map.of("id", 99L)).isEmpty(),
                () -> "target rows=" + files.count(target, TABLE));
        assertThat(files.fetch(target, TABLE, Map.of("id", 1L))).isPresent();
        assertThat(files.fetch(target, TABLE, Map.of("id", 2L))).isPresent();
    }

    private static void runFail(ControlPlane control, Path directory) throws Exception {
        String pipeline = "first_start_fail";
        EndpointAddress source = endpoint(directory, "fail_source");
        EndpointAddress target = endpoint(directory, "fail_target");
        FileEndpoints files = new FileEndpoints();
        files.seed(source, TABLE, SeedRows.generated(2));
        files.seed(target, TABLE, List.of(Map.of("id", 99L, "seq", 99L)));

        control.discoverSchema("fail_source", E2eConnectorJar.CONNECTOR_ID, source.settings());
        control.apply(workspace(pipeline, "fail_source", source, "fail_target", target, "fail"));
        control.lifecycle(pipeline, LifecycleVerb.START);

        Await.until("the first fail load to refuse the nonempty target", Duration.ofMinutes(1),
                () -> control.state(pipeline).filter(PipelineState.FAILED::equals).isPresent(),
                () -> String.valueOf(control.state(pipeline)));
        assertThat(files.count(target, TABLE)).isOne();
        assertThat(files.fetch(target, TABLE, Map.of("id", 99L))).isPresent();
        assertThat(files.fetch(target, TABLE, Map.of("id", 1L))).isEmpty();
    }

    private static EndpointAddress endpoint(Path directory, String name) throws Exception {
        return EndpointAddress.uri(Files.createDirectories(directory.resolve(name)).toString());
    }

    private static Map<String, String> workspace(
            String pipeline, String sourceId, EndpointAddress source,
            String targetId, EndpointAddress target, String policy) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(sourceId + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                mode: cdc
                tables: [ %s ]
                """.formatted(sourceId, E2eConnectorJar.CONNECTOR_ID, source.text("uri"), TABLE));
        resources.put(targetId + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                """.formatted(targetId, E2eConnectorJar.CONNECTOR_ID, target.text("uri")));
        resources.put(pipeline + ".tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: %s
                  sync:
                    - source: %s
                      on_full_load: %s
                """.formatted(pipeline, sourceId, TABLE, targetId, policy));
        return resources;
    }
}
