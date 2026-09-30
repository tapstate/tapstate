package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineView;

import java.util.List;

/** Artifact-only Pipeline definitions, retained for the model-facing pipeline.list contract. */
record PipelineArtifactList(List<PipelineView> items) {

    PipelineArtifactList {
        items = List.copyOf(items);
    }
}
