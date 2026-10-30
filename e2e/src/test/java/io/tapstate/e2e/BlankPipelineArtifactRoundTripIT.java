package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** The control-plane smoke case for applying and immediately reading a blank pipeline artifact. */
class BlankPipelineArtifactRoundTripIT {

    private static final String PIPELINE_ID = "blank_pipeline";
    private static final String PIPELINE = """
            version: tapstate/v1
            kind: pipeline
            id: blank_pipeline
            source: []
            """;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aBlankPipelineIsReadableImmediatelyAfterItIsApplied(Tiers tier) {
        try (var storage = PreparedNativeStore.create("e2e", "e2e-password"); ServerHandle server = tier.launch(storage)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);

            control.apply(Map.of("blank_pipeline.tap.yml", PIPELINE));

            ControlPlane.StoredArtifact artifact = control.artifact(PIPELINE_ID).orElseThrow();
            assertThat(artifact.kind()).isEqualTo("pipeline");
            assertThat(artifact.canonicalForm()).isEqualTo(PIPELINE);
            assertThat(control.artifactIds()).contains(PIPELINE_ID);
        }
    }
}
