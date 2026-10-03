package io.tapstate.control.restapi;

import io.tapstate.core.common.TapstateException;
import io.tapstate.control.core.PipelinePreviewCommand;
import io.tapstate.control.core.ArtifactDraft;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PipelinePreviewControllerTest {

    @Test
    void readsARequestWithinTheByteLimit() throws IOException {
        byte[] body = {1, 2, 3, 4};

        assertThat(PipelinePreviewController.readBounded(new ByteArrayInputStream(body), body.length))
                .containsExactly(body);
    }

    @Test
    void refusesAnOversizedRequestWithACodedInputError() {
        byte[] body = {1, 2, 3, 4, 5};

        assertThatThrownBy(() -> PipelinePreviewController.readBounded(new ByteArrayInputStream(body), 4))
                .isInstanceOf(TapstateException.class)
                .extracting(failure -> ((TapstateException) failure).code().code())
                .isEqualTo("control.malformed-request");
    }

    @Test
    void parsesOptionalPreviewFieldsAndDraftPreconditions() {
        PipelinePreviewCommand command = PipelinePreviewController.parse(Map.of(
                "pipelineId", "orders", "outputId", "mongo", "rootLimit", 20, "sampleId", "sample-a",
                "drafts", List.of(Map.of("source", "editor", "content", "candidate",
                        "expectedContentHash", "abc123"))));

        assertThat(command.pipelineId()).isEqualTo("orders");
        assertThat(command.outputId()).isEqualTo("mongo");
        assertThat(command.rootLimit()).isEqualTo(20);
        assertThat(command.sampleId()).isEqualTo("sample-a");
        assertThat(command.drafts()).containsExactly(new ArtifactDraft("editor", "candidate", "abc123"));
        assertThat(PipelinePreviewController.parse(Map.of("pipelineId", "orders", "drafts", List.of()))
                .outputId()).isNull();
    }

    @Test
    void refusesMalformedPreviewShapesUnknownFieldsAndInvalidIntegerValues() {
        assertThatThrownBy(() -> PipelinePreviewController.parse(null)).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> PipelinePreviewController.parse(Map.of("pipelineId", "orders", "extra", true)))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> PipelinePreviewController.parse(Map.of("pipelineId", " ")))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> PipelinePreviewController.parse(Map.of("pipelineId", "orders", "rootLimit", 1.5)))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> PipelinePreviewController.parse(Map.of("pipelineId", "orders", "rootLimit", "2")))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> PipelinePreviewController.parse(Map.of("pipelineId", "orders", "outputId", 3)))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> PipelinePreviewController.parse(Map.of("pipelineId", "orders", "drafts", "bad")))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> PipelinePreviewController.parse(Map.of("pipelineId", "orders", "drafts",
                List.of("bad")))).isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> PipelinePreviewController.parse(Map.of("pipelineId", "orders", "drafts",
                List.of(Map.of("content", "ok", "unexpected", true))))).isInstanceOf(TapstateException.class);
    }
}
