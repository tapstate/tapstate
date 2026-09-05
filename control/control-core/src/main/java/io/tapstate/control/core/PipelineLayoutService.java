package io.tapstate.control.core;

import io.tapstate.spi.store.PipelineLayout;
import io.tapstate.spi.store.PipelineLayoutStore;

import java.util.Map;
import java.util.Objects;

/** Stores editor-only pipeline layout state independently of canonical Pipeline artifacts. */
public final class PipelineLayoutService {

    private final PipelineViewService pipelines;
    private final PipelineLayoutStore layouts;

    public PipelineLayoutService(PipelineViewService pipelines, PipelineLayoutStore layouts) {
        this.pipelines = Objects.requireNonNull(pipelines, "pipelines");
        this.layouts = Objects.requireNonNull(layouts, "layouts");
    }

    /** Returns an empty layout for an existing Pipeline that has not yet been arranged. */
    public PipelineLayout get(String pipelineId) {
        requirePipeline(pipelineId);
        return layouts.get(pipelineId).orElseGet(() -> new PipelineLayout(pipelineId, Map.of(), null));
    }

    /** Replaces the editor-only layout for an existing Pipeline without mutating its semantic artifact. */
    public PipelineLayout save(
            String pipelineId, Map<String, PipelineLayout.NodePosition> nodes, PipelineLayout.Viewport viewport) {
        requirePipeline(pipelineId);
        PipelineLayout layout = new PipelineLayout(pipelineId, nodes, viewport);
        layouts.save(layout);
        return layout;
    }

    private void requirePipeline(String pipelineId) {
        pipelines.get(pipelineId);
    }
}
