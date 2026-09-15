package io.tapstate.control.core;

import io.tapstate.spi.store.PipelineLayout;
import io.tapstate.spi.store.PipelineLayoutStore;

import java.util.LinkedHashMap;
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
    public PipelineLayoutView get(String pipelineId) {
        requirePipeline(pipelineId);
        return layouts.get(pipelineId)
                .map(PipelineLayoutService::toView)
                .orElseGet(() -> new PipelineLayoutView(pipelineId, Map.of(), null));
    }

    /** Replaces the editor-only layout for an existing Pipeline without mutating its semantic artifact. */
    public PipelineLayoutView save(
            String pipelineId,
            Map<String, PipelineLayoutView.NodePosition> nodes,
            PipelineLayoutView.Viewport viewport) {
        requirePipeline(pipelineId);
        PipelineLayout layout = new PipelineLayout(pipelineId, toStoreNodes(nodes), toStoreViewport(viewport));
        layouts.save(layout);
        return toView(layout);
    }

    private static PipelineLayoutView toView(PipelineLayout layout) {
        Map<String, PipelineLayoutView.NodePosition> nodes = new LinkedHashMap<>();
        layout.nodes().forEach((id, position) ->
                nodes.put(id, new PipelineLayoutView.NodePosition(position.x(), position.y())));
        PipelineLayout.Viewport viewport = layout.viewport();
        PipelineLayoutView.Viewport viewViewport = viewport == null
                ? null
                : new PipelineLayoutView.Viewport(viewport.x(), viewport.y(), viewport.zoom());
        return new PipelineLayoutView(layout.pipelineId(), nodes, viewViewport);
    }

    private static Map<String, PipelineLayout.NodePosition> toStoreNodes(
        Map<String, PipelineLayoutView.NodePosition> nodes) {
        Objects.requireNonNull(nodes, "nodes");
        Map<String, PipelineLayout.NodePosition> result = new LinkedHashMap<>();
        nodes.forEach((id, position) -> {
            Objects.requireNonNull(position, "node position");
            result.put(id, new PipelineLayout.NodePosition(position.x(), position.y()));
        });
        return result;
    }

    private static PipelineLayout.Viewport toStoreViewport(PipelineLayoutView.Viewport viewport) {
        return viewport == null ? null : new PipelineLayout.Viewport(viewport.x(), viewport.y(), viewport.zoom());
    }

    private void requirePipeline(String pipelineId) {
        pipelines.get(pipelineId);
    }
}
