package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ArtifactBatchWrite;
import io.tapstate.spi.store.ArtifactMutation;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ProjectionWriteRefusalTest {

    @Test
    void pipelineDependencyConflictStaysInArtifactDomain() {
        assertThatThrownBy(() -> PipelineProjectionService.throwForWriteRefusal(
                "pipeline_a", ArtifactBatchWrite.refused("source_a", ArtifactMutation.VERSION_CONFLICT)))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(ArtifactError.VERSION_CONFLICT);
                    assertThat(error.args()).containsEntry("id", "source_a");
                });
    }

    @Test
    void pipelineOwnVersionConflictKeepsPipelineCode() {
        assertThatThrownBy(() -> PipelineProjectionService.throwForWriteRefusal(
                "pipeline_a", ArtifactBatchWrite.refused("pipeline_a", ArtifactMutation.VERSION_CONFLICT)))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(PipelineError.VERSION_CONFLICT);
                    assertThat(error.args()).containsEntry("id", "pipeline_a");
                });
    }

    @Test
    void sourceDependencyConflictStaysInArtifactDomain() {
        assertThatThrownBy(() -> SourceProjectionService.throwForWriteRefusal(
                "source_a", ArtifactBatchWrite.refused("pipeline_a", ArtifactMutation.VERSION_CONFLICT)))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(ArtifactError.VERSION_CONFLICT);
                    assertThat(error.args()).containsEntry("id", "pipeline_a");
                });
    }

    @Test
    void sourceOwnVersionConflictKeepsSourceCode() {
        assertThatThrownBy(() -> SourceProjectionService.throwForWriteRefusal(
                "source_a", ArtifactBatchWrite.refused("source_a", ArtifactMutation.VERSION_CONFLICT)))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(SourceError.VERSION_CONFLICT);
                    assertThat(error.args()).containsEntry("id", "source_a");
                });
    }
}
