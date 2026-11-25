package io.tapstate.control.core;

import java.util.List;

/** A preview request over the same untrusted DSL drafts accepted by artifact validation. */
public record PipelinePreviewCommand(
        String pipelineId,
        String outputId,
        Integer rootLimit,
        String sampleId,
        List<ArtifactDraft> drafts) {
}
