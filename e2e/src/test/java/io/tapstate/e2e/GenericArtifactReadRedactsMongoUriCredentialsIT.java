package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Locale;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** The public artifact read omits a Source connection config instead of returning its credentials. */
class GenericArtifactReadRedactsMongoUriCredentialsIT {

    private static final String SOURCE_ID = "atlas";
    private static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: atlas
            connector: mongodb-atlas
            config: { uri: "mongodb+srv://probe:sentinel-secret@cluster.example/test" }
            """;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aGenericArtifactReadDoesNotReturnMongoUriCredentials(Tiers tier) {
        try (ServerHandle server = tier.launch(SharedMongo.replicaSetUrl(
                "artifact_read_redaction_" + tier.name().toLowerCase(Locale.ROOT)))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");

            control.apply(Map.of("atlas.tap.yml", SOURCE));

            String canonical = control.artifact(SOURCE_ID).orElseThrow().canonicalForm();
            assertThat(canonical)
                    .contains("id: atlas", "connector: mongodb-atlas")
                    .doesNotContain("config:", "cluster.example", "probe", "sentinel-secret");
        }
    }
}
