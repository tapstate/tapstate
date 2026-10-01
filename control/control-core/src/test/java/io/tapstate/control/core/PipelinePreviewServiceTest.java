package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelinePreviewServiceTest {

    @Test
    void acceptsCandidateDraftsAtTheConfiguredLimits() {
        List<ArtifactDraft> drafts = new ArrayList<>();
        for (int index = 0; index < PipelinePreviewService.MAX_DRAFTS; index++) {
            drafts.add(new ArtifactDraft("resource-" + index, "x"));
        }

        assertThat(PipelinePreviewService.requireDrafts(drafts)).hasSize(PipelinePreviewService.MAX_DRAFTS);
        assertThat(PipelinePreviewService.requireDrafts(List.of(new ArtifactDraft(
                "large", "x".repeat((int) PipelinePreviewService.MAX_DRAFT_BYTES)))))
                .hasSize(1);
    }

    @Test
    void refusesTooManyCandidateDraftsWithACodedInputError() {
        List<ArtifactDraft> drafts = new ArrayList<>();
        for (int index = 0; index <= PipelinePreviewService.MAX_DRAFTS; index++) {
            drafts.add(new ArtifactDraft("resource-" + index, "x"));
        }

        assertThatThrownBy(() -> PipelinePreviewService.requireDrafts(drafts))
                .isInstanceOf(TapstateException.class)
                .extracting(failure -> ((TapstateException) failure).code().code())
                .isEqualTo("control.malformed-request");
    }

    @Test
    void refusesCandidateContentAboveTheByteLimitWithACodedInputError() {
        ArtifactDraft oversized = new ArtifactDraft(
                "large", "x".repeat((int) PipelinePreviewService.MAX_DRAFT_BYTES + 1));

        assertThatThrownBy(() -> PipelinePreviewService.requireDrafts(List.of(oversized)))
                .isInstanceOf(TapstateException.class)
                .extracting(failure -> ((TapstateException) failure).code().code())
                .isEqualTo("control.malformed-request");
    }
}
