package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/** An authenticated upload of a non-ZIP artifact is a coded client refusal over the real HTTP path. */
class AnUnreadableConnectorArtifactIsRefusedIT {

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aNonZipUploadIsRefusedWithACodeNamingTheArtifact() {
        try (var storage = PreparedNativeStore.create(); ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);

            byte[] artifact = "<html><body>Download failed</body></html>".getBytes(StandardCharsets.UTF_8);
            ControlPlane.Refusal refusal = control.registerConnectorExpectingRefusal(artifact);

            assertThat(refusal.status()).isEqualTo(400);
            assertThat(refusal.code()).isEqualTo("connector.artifact-unreadable");
            assertThat(refusal.params()).containsKey("artifact");
            assertThat(refusal.params().get("artifact")).asString()
                    .contains("tapstate-connector-").endsWith(".jar");
        }
    }
}
