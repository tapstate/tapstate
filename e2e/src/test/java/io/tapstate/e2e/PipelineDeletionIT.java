package io.tapstate.e2e;

import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** A real run must be stopped before one Web deletion removes both durable representations. */
class PipelineDeletionIT {

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void oneDeleteRemovesTheStoppedPipelineAndItsDraftFromTheCatalog(Tiers tier, @TempDir Path directory)
            throws Exception {
        try (var storage = PreparedNativeStore.create(); ServerHandle server = tier.launch(storage)) {
            RunningPipeline running = RunningPipeline.started(server, directory, storage);
            ControlPlane control = running.control();
            String id = running.pipelineId();
            ControlPlane.StoredArtifact before = control.artifact(id).orElseThrow();
            Map<String, Object> draft = control.createPipelineDraft(id, Map.of(
                    "pipelineId", id, "mode", "dag", "name", "Pipeline deletion witness",
                    "baseArtifactHash", before.contentHash(),
                    "graph", Map.of("nodes", List.of(), "edges", List.of(),
                            "viewport", Map.of("x", 0.0, "y", 0.0, "zoom", 1.0))));
            long revision = ((Number) draft.get("revision")).longValue();

            ControlPlane.Refusal refusal = control.deletePipelineExpectingRefusal(id, before.contentHash(), revision);
            assertThat(refusal.status()).isEqualTo(409);
            assertThat(refusal.code()).isEqualTo("artifact.pipeline-not-stopped");
            assertThat(control.artifact(id)).contains(before);
            assertThat(control.pipelineDraft(id)).contains(draft);
            assertThat(control.pipelineCatalogIds()).contains(id);

            running.stopAndSettle();
            control.deletePipeline(id, control.contentHash(id), revision);

            assertThat(control.artifact(id)).isEmpty();
            assertThat(control.pipelineDraft(id)).isEmpty();
            assertThat(control.pipelineCatalogIds()).doesNotContain(id);
        }
    }
}
