package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Starting a sync pipeline before its source schema is discovered is refused with a useful diagnosis,
 * rather than reaching a sink with an incomplete table descriptor and crashing.
 *
 * <p>The pipeline is applied successfully first: discovery is a start precondition for a sync, not an
 * apply precondition. The start intent is then driven through the public control plane and the published
 * failure is read back from the public status face. Running the same case against both server tiers also
 * checks that the shipped process preserves the in-process behavior.
 */
class StartBeforeSourceDiscoveryIsRefusedIT {

    private static final String SOURCE_ID = "src_file";
    private static final String TARGET_ID = "tgt_file";
    private static final String PIPELINE_ID = "start_before_discovery";
    private static final String EXPECTED_CODE = "actuation.source-schema-not-discovered";

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void startingASyncBeforeDiscoveryPublishesACodedFailure(
            Tiers tier, @org.junit.jupiter.api.io.TempDir Path directory) throws Exception {
        try (ServerHandle server = tier.launch(SharedMongo.replicaSetUrl("start_before_discovery_"
                + tier.name().toLowerCase(Locale.ROOT)))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(
                    E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(E2eConnectorJar.buildInto(directory)));

            control.apply(workspace(directory));
            assertThat(control.artifactIds())
                    .as("apply succeeds before the source schema is discovered")
                    .contains(SOURCE_ID, TARGET_ID, PIPELINE_ID);

            control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

            Await.until(
                    "the undiscovered sync pipeline to publish its start refusal",
                    () -> control.failureCode(PIPELINE_ID).isPresent(),
                    () -> control.metrics(PIPELINE_ID));

            assertThat(control.state(PIPELINE_ID)).contains(PipelineState.FAILED);
            assertThat(control.failureCode(PIPELINE_ID)).contains(EXPECTED_CODE);
            assertThat(control.errorCount(PIPELINE_ID)).contains(1L);
        }
    }

    private static Map<String, String> workspace(Path directory) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("src_file.tap.yml", endpointYaml(SOURCE_ID, directory.resolve("src")));
        resources.put("tgt_file.tap.yml", endpointYaml(TARGET_ID, directory.resolve("tgt")));
        resources.put("pipeline.tap.yml", pipelineYaml());
        return resources;
    }

    private static String endpointYaml(String id, Path uri) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                mode: cdc
                tables: [ orders ]
                """
                .formatted(id, E2eConnectorJar.CONNECTOR_ID, uri);
    }

    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: changes, from: [orders], type: filter, expr: "op == 'i'" }
                serve:
                  from: changes
                  sync:
                    - source: %s
                """
                .formatted(PIPELINE_ID, SOURCE_ID, TARGET_ID);
    }
}
