package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The specification vocabulary cannot assert current-run load phase or per-table measured counts. */
class ABoundedLoadKeepsItsMonitoringProgressIT {

    private static final String PIPELINE = "bounded_monitoring";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void completedRowsRemainVisibleAfterTheActivePlanIsRemoved(@TempDir Path directory) throws Exception {
        Path source = Files.createDirectory(directory.resolve("source"));
        Path target = Files.createDirectory(directory.resolve("target"));
        FileEndpoints.replaceTable(source.resolve("orders.csv"), "id,seq\n10,1\n11,2\n");
        try (PreparedNativeStore storage = PreparedNativeStore.create();
                ServerHandle server = Tiers.IN_PROCESS.launch(storage);
                FileEndpoints files = new FileEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID,
                    Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
            Map<String, String> resources = new LinkedHashMap<>();
            resources.put("src.tap.yml", Workspaces.cdcSourceYaml("src", source));
            resources.put("target.tap.yml", Workspaces.targetYaml("target", target));
            resources.put("pipeline.tap.yml", """
                    version: tapstate/v1
                    kind: pipeline
                    id: bounded_monitoring
                    source: src
                    settings: { read_mode: snapshot_only }
                    transforms:
                      - { id: orders_step, from: [ orders ], type: filter, expr: "true" }
                    serve:
                      from: orders_step
                      sync: [ { id: out, source: target } ]
                    """);
            control.apply(resources);
            control.discoverSchema("src", E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            Instant started = Instant.now();
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            EndpointAddress destination = EndpointAddress.uri(target.toString());
            Await.until("both source rows to reach the target", () -> files.count(destination, "orders") == 2,
                    () -> "target rows=" + files.count(destination, "orders"));
            assertThat(files.fetch(destination, "orders", Map.of("id", 10L))).isPresent();
            assertThat(files.fetch(destination, "orders", Map.of("id", 11L))).isPresent();
            Await.until("the bounded pipeline to complete", () ->
                            control.state(PIPELINE).filter(PipelineState.COMPLETED::equals).isPresent(),
                    () -> String.valueOf(control.state(PIPELINE)));
            Await.until("normal completion to remove the active execution plan", () ->
                            control.executionPlan(PIPELINE).isEmpty(),
                    () -> String.valueOf(control.executionPlan(PIPELINE)));

            Map<?, ?> load = Await.answered("completed initial-load facts on the Monitoring HTTP face", () ->
                    control.initialLoad(PIPELINE).filter(reading -> "COMPLETED".equals(reading.get("phase"))));
            assertThat(load.get("readMode")).isEqualTo("snapshot_only");
            assertThat(load.get("runKey")).asString().startsWith("execution:" + PIPELINE + ":");
            assertThat(Instant.parse((String) load.get("observedAt"))).isAfterOrEqualTo(started);
            assertThat(((Number) load.get("landedTables")).longValue()).isEqualTo(1);
            assertThat(((Number) load.get("totalTables")).intValue()).isEqualTo(1);
            Map<?, ?> tables = (Map<?, ?>) load.get("tables");
            assertThat(tables).hasSize(1);
            assertThat(tables.containsKey("orders")).isTrue();
            Map<?, ?> orders = (Map<?, ?>) tables.get("orders");
            assertThat(orders.get("stage")).isEqualTo("LANDED");
            assertThat(((Number) orders.get("rowsRead")).longValue()).isEqualTo(2);
            assertThat(orders.get("rowsTotalEstimate")).as("no source estimate was measured").isNull();
        }
    }
}
