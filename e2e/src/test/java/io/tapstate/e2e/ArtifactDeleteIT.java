package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The witness that an applied resource can be removed, for every kind the grammar has, and that the
 * removal is real.
 *
 * <p>Real is the discriminating claim, and the listing is what carries it. A tombstone implementation
 * answers a removal exactly as a real one does and even answers the later read with "not found" - what it
 * cannot do is drop the row from the listing, because the row is still there to be filtered. So the
 * listing is asserted alongside the read, and asserting only that the removal call did not throw would
 * check neither.
 *
 * <p>Re-applying the id afterwards is the third assertion, and it is not a nicety. An id that is gone
 * from every read but still occupied is precisely what a tombstone leaves behind, and it is the state an
 * author meets as "I deleted it, why can I not recreate it" - the one symptom that reaches a user rather
 * than a maintainer. It also pins the promise the design made in place of a recycle bin: the way back is
 * to apply the document again.
 *
 * <p>All five kinds, in one launch. The verb is type-agnostic by construction, so the risk is not that
 * one kind takes a different code path but that one kind was never wired to the path at all - which is a
 * per-kind fact and cannot be witnessed by a case that only ever removes a source. They are removed in an
 * order the reference gate permits: a pipeline names its source, so the pipeline goes first and its source
 * is removed as part of the sweep that follows, not left as an exception.
 *
 * <p>Runs on the harness's own connector, so it needs Docker for the store and nothing else.
 */
class ArtifactDeleteIT {

    private static final String SOURCE_ID = "del_source";
    private static final String TRANSFORM_ID = "del_transform";
    private static final String VIEW_ID = "del_view";
    private static final String SERVE_ID = "del_serve";
    private static final String PIPELINE_ID = "del_pipeline";
    private static final String PIPELINE_SOURCE_ID = "del_pipeline_src";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void everyKindCanBeRemovedAndTheRemovalIsRealRatherThanATombstone(Tiers tier, @TempDir Path directory)
            throws Exception {
        try (var storage = PreparedNativeStore.create(); ServerHandle server = tier.launch(storage)) {
            ControlPlane control = connected(server, storage, directory);

            // Each kind arrives in the smallest batch that is a closure: a pipeline names its source, so the
            // two travel together; the reusable definitions stand alone.
            control.apply(Map.of("source.tap.yml", sourceYaml(SOURCE_ID)));
            control.apply(Map.of("transform.tap.yml", transformYaml()));
            control.apply(Map.of("view.tap.yml", viewYaml()));
            control.apply(Map.of("serve.tap.yml", serveYaml()));
            control.apply(pipelineWorkspace());

            assertThat(control.artifactIds())
                    .as("everything this case is about to remove is actually there first - a removal witness "
                            + "over an empty store witnesses nothing")
                    .contains(SOURCE_ID, TRANSFORM_ID, VIEW_ID, SERVE_ID, PIPELINE_ID, PIPELINE_SOURCE_ID);

            // The pipeline first: it is the only resource here that names another, so any other order meets
            // the reference gate rather than the removal path this case is about.
            for (String id : List.of(PIPELINE_ID, PIPELINE_SOURCE_ID, SOURCE_ID, TRANSFORM_ID, VIEW_ID, SERVE_ID)) {
                int idsBefore = control.artifactIds().size();

                control.deleteArtifact(id, control.contentHash(id));

                assertThat(control.artifact(id))
                        .as("reading %s back after removing it", id)
                        .isEmpty();
                assertThat(control.artifactIds())
                        .as("the listing after removing %s - a tombstone answers the read above with "
                                + "'not found' and still keeps its row here", id)
                        .doesNotContain(id)
                        .hasSize(idsBefore - 1);
            }

            assertThat(control.artifactIds())
                    .as("the store this case emptied of its own resources")
                    .doesNotContain(SOURCE_ID, TRANSFORM_ID, VIEW_ID, SERVE_ID, PIPELINE_ID, PIPELINE_SOURCE_ID);

            // The way back. An id that reads as absent but cannot be applied again is a tombstone wearing a
            // removal's answers, and this is the assertion that tells them apart.
            control.apply(Map.of("source.tap.yml", sourceYaml(SOURCE_ID)));
            assertThat(control.artifactIds())
                    .as("the id is free again, which is the design's stated way back from a removal")
                    .contains(SOURCE_ID);
        }
    }

    private static ControlPlane connected(ServerHandle server, PreparedNativeStore storage, Path directory) throws Exception {
        ControlPlane control = new ControlPlane(server.baseUrl());
        storage.loginAndAwaitBusiness(control);
        control.registerConnector(
                E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
        return control;
    }

    /** A pipeline and the source it names: references resolve within one submitted batch, so they travel as one. */
    private static Map<String, String> pipelineWorkspace() {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("pipeline_src.tap.yml", sourceYaml(PIPELINE_SOURCE_ID));
        resources.put("pipeline.tap.yml", pipelineYaml());
        return resources;
    }

    private static String sourceYaml(String id) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "/tmp/%s" }
                mode: cdc
                tables: [ orders ]
                """
                .formatted(id, E2eConnectorJar.CONNECTOR_ID, id);
    }

    /** A reusable definition body: pure logic, with the wiring left to whatever pipeline names it. */
    private static String transformYaml() {
        return """
                version: tapstate/v1
                kind: transform
                id: %s
                type: map
                fields: { ssn: false }
                """
                .formatted(TRANSFORM_ID);
    }

    private static String viewYaml() {
        return """
                version: tapstate/v1
                kind: view
                id: %s
                primary_key: id
                storage: { warm: { collection: orders } }
                """
                .formatted(VIEW_ID);
    }

    private static String serveYaml() {
        return """
                version: tapstate/v1
                kind: serve
                id: %s
                query: [ { type: rest } ]
                """
                .formatted(SERVE_ID);
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
                .formatted(PIPELINE_ID, PIPELINE_SOURCE_ID, PIPELINE_SOURCE_ID);
    }
}
