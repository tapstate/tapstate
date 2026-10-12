package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The witness that removing a resource another resource still names is refused, that the refusal says
 * who is naming it, and that nothing was removed on the way to saying so.
 *
 * <p>Three assertions, and the third is the one with teeth. "Refused" and "refused, having already
 * deleted it" answer the identical code on the identical call, and only reading the resource back tells
 * them apart - so a case asserting the code alone would pass against an implementation that destroys
 * first and checks afterwards, which is the exact ordering the gate exists to forbid. The stored bytes
 * and the version they hash to are both read back, because a resource that survived the call but was
 * rewritten by it has still been written to during a refusal.
 *
 * <p>The referrers travel in the refusal and are asserted by name. Without that, the case is satisfied by
 * a product that refuses every removal with an empty list attached, and an author is told "something
 * references this" with no way to find out what - which makes the refusal a dead end rather than the next
 * step. Nothing cascades on the author's behalf: the pipeline is still there afterwards, and that is
 * asserted too, because deleting the referrer to make room is the one helpful-looking behaviour the
 * design ruled out.
 *
 * <p>Runs on the harness's own connector, so it needs Docker for the store and nothing else: whether a
 * reference blocks a removal is a control-plane fact, and no real database is required to demonstrate it.
 */
class ArtifactDeleteRefusalIT {

    private static final String SOURCE_ID = "referenced_source";
    private static final String PIPELINE_ID = "referring_pipeline";
    private static final String IN_USE = "artifact.in-use";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void removingAResourceAPipelineStillNamesIsRefusedAndNothingIsRemoved(Tiers tier, @TempDir Path directory)
            throws Exception {
        try (ServerHandle server = tier.launch(storeUri("delete_in_use", tier))) {
            ControlPlane control = connected(server, directory);
            control.apply(workspace());

            ControlPlane.StoredArtifact before = control.artifact(SOURCE_ID).orElseThrow();

            ControlPlane.Refusal refusal = control.deleteArtifactExpectingRefusal(SOURCE_ID, before.contentHash());

            assertThat(refusal.code())
                    .as("the code refusing to remove a resource another resource still names")
                    .isEqualTo(IN_USE);
            assertThat(refusal.params())
                    .as("the refusal names the resource it is about")
                    .containsEntry("id", SOURCE_ID);
            assertThat(refusal.params().get("referrers"))
                    .as("who is still naming it - without this the author is told 'something references "
                            + "this' and has nowhere to go next")
                    .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.LIST)
                    .contains(PIPELINE_ID);

            assertThat(control.artifact(SOURCE_ID))
                    .as("the resource after a refused removal - 'refused' and 'refused, having already "
                            + "deleted it' answer the same code, and only this read tells them apart")
                    .contains(before);
            assertThat(control.artifactIds())
                    .as("the referrer is left alone: removing it to make room is the author's decision, "
                            + "never a side effect of the removal they asked for")
                    .contains(PIPELINE_ID, SOURCE_ID);
        }
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void anInlineViewProtectsItsManagedStoreThroughBothDeleteRoutes(Tiers tier, @TempDir Path directory)
            throws Exception {
        try (ServerHandle server = tier.launch(storeUri("delete_managed_view_store", tier))) {
            ControlPlane control = connected(server, directory);
            control.apply(viewWorkspace());
            ControlPlane.StoredArtifact before = control.artifact("views").orElseThrow();
            HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
            // Pin the existing diagnostic projection of each public route.
            Map<String, String> inUseCodes = Map.of("sources", "source.in-use", "artifacts", IN_USE);

            for (String kind : List.of("sources", "artifacts")) {
                HttpRequest request = HttpRequest.newBuilder(server.baseUrl().resolve("/api/" + kind + "/views"))
                        .timeout(Duration.ofSeconds(30))
                        .header("Authorization", "Bearer " + control.credential())
                        .header("If-Match", "\"" + before.contentHash() + "\"")
                        .DELETE().build();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                ControlPlane.Refusal refusal = ControlPlane.interpretRefusal(
                        response.statusCode(), response.body(), "deleting the managed view store through " + kind);

                assertThat(refusal.status()).isEqualTo(409);
                assertThat(refusal.code()).isEqualTo(inUseCodes.get(kind));
                assertThat(refusal.params())
                        .isEqualTo(Map.of("id", "views", "referrers", List.of(PIPELINE_ID)));
                assertThat(control.artifact("views")).contains(before);
                assertThat(control.artifactIds()).contains(PIPELINE_ID, SOURCE_ID, "views");
            }
        }
    }

    private static ControlPlane connected(ServerHandle server, Path directory) throws Exception {
        ControlPlane control = new ControlPlane(server.baseUrl());
        control.bootstrapAndLogin("e2e", "e2e-password");
        control.registerConnector(
                E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
        return control;
    }

    private static String storeUri(String name, Tiers tier) {
        return SharedMongo.replicaSetUrl(name + "_" + tier.name().toLowerCase(Locale.ROOT));
    }

    private static Map<String, String> workspace() {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("source.tap.yml", sourceYaml());
        resources.put("pipeline.tap.yml", pipelineYaml());
        return resources;
    }

    private static Map<String, String> viewWorkspace() {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("source.tap.yml", sourceYaml());
        resources.put("pipeline.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                view: { id: order_state, from: orders, primary_key: id }
                """.formatted(PIPELINE_ID, SOURCE_ID));
        return resources;
    }

    private static String sourceYaml() {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "/tmp/%s" }
                mode: cdc
                tables: [ orders ]
                """
                .formatted(SOURCE_ID, E2eConnectorJar.CONNECTOR_ID, SOURCE_ID);
    }

    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { type: filter, from: [orders], expr: "op != 'd'" }
                serve:
                  from: orders
                  sync:
                    - source: %s
                """
                .formatted(PIPELINE_ID, SOURCE_ID, SOURCE_ID);
    }
}
