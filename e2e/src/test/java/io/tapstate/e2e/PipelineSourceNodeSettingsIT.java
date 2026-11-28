package io.tapstate.e2e;

import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.PipelineStateMapping;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Draft replacement and two simultaneous Pipelines are outside the single-Pipeline example vocabulary. */
@RequiresDocker
class PipelineSourceNodeSettingsIT {

    private static final String SOURCE = "shared_orders";
    private static final String EXPLICIT = "orders_explicit";
    private static final String UNCONFIGURED = "orders_unconfigured";
    private static final Duration TIMEOUT = Duration.ofSeconds(90);

    @Test
    void onlyTheNodeWithAnExplicitValueFailsEvenWhenTheSourceHasTheSameValue(@TempDir Path directory) throws Exception {
        Path input = Files.createDirectory(directory.resolve("input"));
        Path healthyOutput = Files.createDirectory(directory.resolve("healthy"));
        Path failingOutput = Files.createDirectory(directory.resolve("failing"));
        Path orders = input.resolve("orders.csv");
        FileEndpoints.replaceTable(orders, "id,seq\n1,10\n");

        try (var storage = PreparedNativeStore.create(); var server = Tiers.IN_PROCESS.launch(storage);
                var files = new FileEndpoints()) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID,
                    Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
            control.apply(Map.of("input.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: shared_orders
                    connector: e2e_file
                    config: { uri: "%s", fail_cdc: true }
                    mode: cdc
                    tables: [orders]
                    """.formatted(input),
                    "healthy.tap.yml", Workspaces.targetYaml("healthy", healthyOutput),
                    "failing.tap.yml", Workspaces.targetYaml("failing", failingOutput)));
            control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", input.toString()));
            String originalSource = control.contentHash(SOURCE);

            control.createPipelineDraft(EXPLICIT, draft(EXPLICIT, "failing", Map.of()));
            String endpoint = "/api/pipelines/" + EXPLICIT;
            Map<String, Object> overrides = Map.of("orders", Map.of("fail_cdc", true));
            control.sourceRequest("PUT", endpoint + "/draft", draft(EXPLICIT, "failing", overrides), "\"1\"", 200);
            Map<?, ?> reopened = (Map<?, ?>) control.pipelineDraft(EXPLICIT).orElseThrow().get("wizard");
            assertThat(reopened.get("sourceNodeConfigs")).isEqualTo(overrides);
            control.sourceRequest("POST", endpoint + "/draft:publish", Map.of("revision", 2), null, 200);
            Map<?, ?> projected = (Map<?, ?>) JsonReader.parse(
                    control.sourceRequest("GET", endpoint, null, null, 200).body());
            Map<?, ?> source = (Map<?, ?>) ((List<?>) projected.get("sources")).getFirst();
            assertThat(source.get("nodeConfig")).isEqualTo(Map.of("fail_cdc", true));

            control.createPipelineDraft(UNCONFIGURED, draft(UNCONFIGURED, "healthy", Map.of()));
            control.sourceRequest("POST", "/api/pipelines/" + UNCONFIGURED + "/draft:publish",
                    Map.of("revision", 1), null, 200);
            control.lifecycle(UNCONFIGURED, LifecycleVerb.START);
            Await.until("the Pipeline with no node setting to settle", TIMEOUT,
                    () -> control.state(UNCONFIGURED).filter(state -> state == PipelineState.RUNNING
                            || state == PipelineState.FAILED).isPresent(),
                    () -> control.pipelineStatus(UNCONFIGURED).toString());
            assertThat(control.state(UNCONFIGURED)).contains(PipelineState.RUNNING);
            control.lifecycle(EXPLICIT, LifecycleVerb.START);
            awaitState(control, EXPLICIT, PipelineState.FAILED);

            FileEndpoints.replaceTable(orders, "id,seq\n1,10\n2,20\n");
            EndpointAddress target = EndpointAddress.uri(healthyOutput.toString());
            Await.until("the unconfigured capture to keep delivering after the explicit capture fails", TIMEOUT,
                    () -> files.fetch(target, "orders", Map.of("id", 2L)).isPresent(),
                    () -> control.pipelineStatus(UNCONFIGURED).toString());
            control.stop(EXPLICIT, false);
            awaitState(control, EXPLICIT, PipelineState.STOPPED);
            String previousArtifact = control.contentHash(EXPLICIT);
            control.sourceRequest("PUT", endpoint + "/draft", draft(EXPLICIT, "failing", Map.of()), "\"2\"", 200);
            control.sourceRequest("POST", endpoint + "/draft:publish",
                    Map.of("revision", 3, "artifactHash", previousArtifact), null, 200);
            assertThat(control.artifact(EXPLICIT).orElseThrow().canonicalForm()).doesNotContain("nodeConfig");
            assertThat(control.contentHash(SOURCE)).isEqualTo(originalSource);
            assertThat(((Map<?, ?>) control.source(SOURCE).get("config")).get("fail_cdc")).isEqualTo(true);
        }
    }

    @Test
    void nodesOfOnePipelineOpenSeparateCapturesAndClearOneNodeSetting(@TempDir Path directory) throws Exception {
        Path input = Files.createDirectory(directory.resolve("input"));
        Path output = Files.createDirectory(directory.resolve("output"));
        FileEndpoints.replaceTable(input.resolve("orders.csv"), "id,seq\n1,10\n");
        FileEndpoints.replaceTable(input.resolve("items.csv"), "id,order_id,seq\n2,1,20\n");

        try (var storage = PreparedNativeStore.create(); var server = Tiers.IN_PROCESS.launch(storage)) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID,
                    Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
            control.apply(Map.of("input.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: shared_orders
                    connector: e2e_file
                    config: { uri: "%s", fail_cdc: false }
                    mode: cdc
                    tables: [orders, items]
                    srs: { key: order_database }
                    """.formatted(input), "target.tap.yml", Workspaces.targetYaml("output", output)));
            control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", input.toString()));
            String originalSource = control.contentHash(SOURCE);
            String pipeline = "orders_with_items";
            String endpoint = "/api/pipelines/" + pipeline;
            Map<String, Object> overrides = Map.of(
                    "orders", Map.of("fail_cdc", false), "items", Map.of("fail_cdc", true));
            control.createPipelineDraft(pipeline, draftWithItems(pipeline, overrides));
            control.sourceRequest("POST", endpoint + "/draft:publish", Map.of("revision", 1), null, 200);

            Map<?, ?> published = (Map<?, ?>) JsonReader.parse(
                    control.sourceRequest("GET", endpoint, null, null, 200).body());
            var sources = (List<?>) published.get("sources");
            assertThat(sources).hasSize(2);
            assertThat(sources.stream().map(value -> ((Map<?, ?>) value).get("nodeConfig")).toList())
                    .isEqualTo(List.of(Map.of("fail_cdc", false), Map.of("fail_cdc", true)));
            control.lifecycle(pipeline, LifecycleVerb.START);
            awaitState(control, pipeline, PipelineState.FAILED);
            Await.until("the failure to come from the configured connector", TIMEOUT,
                    () -> control.logs(pipeline).contains("the change stream stopped and will not resume"),
                    () -> control.logs(pipeline));
            control.stop(pipeline, false);
            awaitState(control, pipeline, PipelineState.STOPPED);

            String previousArtifact = control.contentHash(pipeline);
            Map<String, Object> restored = Map.of("orders", Map.of("fail_cdc", false));
            control.sourceRequest("PUT", endpoint + "/draft", draftWithItems(pipeline, restored), "\"1\"", 200);
            control.sourceRequest("POST", endpoint + "/draft:publish",
                    Map.of("revision", 2, "artifactHash", previousArtifact), null, 200);
            Map<?, ?> wizard = (Map<?, ?>) control.pipelineDraft(pipeline).orElseThrow().get("wizard");
            assertThat(wizard.get("sourceNodeConfigs")).isEqualTo(restored);
            clearForUpdatedSelection(control, storage, pipeline);
            control.lifecycle(pipeline, LifecycleVerb.START);
            awaitState(control, pipeline, PipelineState.RUNNING);
            assertThat(control.contentHash(SOURCE)).isEqualTo(originalSource);
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"snapshot_only", "snapshot_and_cdc"})
    void aliasedNodesCanOpenTheirPrivateCaptureScopes(String mode, @TempDir Path directory) throws Exception {
        Path input = Files.createDirectory(directory.resolve("input"));
        Path output = Files.createDirectory(directory.resolve("output"));
        FileEndpoints.replaceTable(input.resolve("orders.csv"), "id,seq\n1,10\n");
        FileEndpoints.replaceTable(input.resolve("items.csv"), "id,seq\n2,20\n");

        try (var storage = PreparedNativeStore.create(); var server = Tiers.IN_PROCESS.launch(storage);
                var files = new FileEndpoints()) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID,
                    Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
            String pipeline = "private_aliases";
            control.apply(Map.of("input.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: shared_orders
                    connector: e2e_file
                    config: { uri: "%s", fail_cdc: true }
                    mode: cdc
                    tables: [orders, items]
                    """.formatted(input), "output.tap.yml", Workspaces.targetYaml("output", output),
                    "pipeline.tap.yml", """
                    version: tapstate/v1
                    kind: pipeline
                    id: private_aliases
                    source:
                      - { id: shared_orders, as: parent, srs: false, nodeConfig: { fail_cdc: false } }
                      - { id: shared_orders, as: child, srs: false, nodeConfig: { fail_cdc: false } }
                    settings: { read_mode: %s }
                    serve:
                      from: [parent.orders, child.items]
                      sync: [{ id: sink, source: output }]
                    """.formatted(mode)));
            control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", input.toString()));
            String originalSource = control.contentHash(SOURCE);
            control.lifecycle(pipeline, LifecycleVerb.START);
            var target = EndpointAddress.uri(output.toString());
            Await.until("both aliased node snapshots to reach the sink", TIMEOUT,
                    () -> files.fetch(target, "orders", Map.of("id", 1L)).isPresent()
                            && files.fetch(target, "items", Map.of("id", 2L)).isPresent(),
                    () -> control.pipelineStatus(pipeline) + " " + control.logs(pipeline));
            if (mode.equals("snapshot_only")) {
                awaitState(control, pipeline, PipelineState.COMPLETED);
            } else {
                awaitState(control, pipeline, PipelineState.RUNNING);
                FileEndpoints.replaceTable(input.resolve("items.csv"), "id,seq\n2,20\n3,30\n");
                Await.until("the child node to deliver CDC through its own private capture", TIMEOUT,
                        () -> files.fetch(target, "items", Map.of("id", 3L)).isPresent(),
                        () -> control.pipelineStatus(pipeline).toString());
            }
            assertThat(control.contentHash(SOURCE)).isEqualTo(originalSource);
        }
    }

    private static void clearForUpdatedSelection(ControlPlane control, PreparedNativeStore storage, String pipeline) {
        // The revised node configuration has a different capture selection.
        try (var configuration = MongoConfigurationDomain.openExisting(new MongoConnectionSettings(
                storage.configurationUri(), null, Duration.ofSeconds(5)))) {
            long original = configuration.pipelineStates().read(pipeline).orElseThrow().stateGeneration();
            control.stop(pipeline, true);
            Await.until("the updated Pipeline's state to be cleared", TIMEOUT,
                    () -> configuration.pipelineStates().read(pipeline)
                            .filter(state -> state.stateGeneration() > original
                                    && state.phase() == PipelineStateMapping.Phase.ACTIVE).isPresent()
                            && control.state(pipeline).filter(PipelineState.STOPPED::equals).isPresent(),
                    () -> configuration.pipelineStates().read(pipeline).toString());
        }
    }

    private static Map<String, Object> draftWithItems(String id, Map<String, Object> overrides) {
        return draft(id, "output", overrides, List.of(Map.of(
                "id", "items", "parentId", "orders", "sourceId", SOURCE, "table", "items",
                "relation", Map.of("shape", "array", "path", "items", "key", List.of("id"),
                        "arrayKey", List.of("id"), "on", List.of(Map.of("childField", "order_id", "parentField", "id"))))));
    }

    private static void awaitState(ControlPlane control, String id, PipelineState state) {
        Await.until(id + " to reach " + state, TIMEOUT,
                () -> control.state(id).filter(state::equals).isPresent(),
                () -> control.pipelineStatus(id).toString());
    }

    private static Map<String, Object> draft(String id, String target, Map<String, Object> overrides) {
        return draft(id, target, overrides, List.of());
    }

    private static Map<String, Object> draft(String id, String target, Map<String, Object> overrides,
            List<Map<String, Object>> related) {
        return Map.of("pipelineId", id, "mode", "wizard", "name", id, "wizard", Map.of(
                "root", Map.of("id", "orders", "sourceId", SOURCE, "table", "orders",
                        "key", List.of("id"), "preTransforms", List.of()),
                "related", related, "transforms", List.of(), "sourceNodeConfigs", overrides,
                "output", Map.of("kind", "source", "config", Map.of("sourceId", target, "table", "orders"))));
    }
}
