package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineCatalogItem;
import io.tapstate.control.core.PipelineCatalogService;
import io.tapstate.control.core.PipelineView;
import io.tapstate.control.core.PipelineViewService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Objects;

/** Unified Pipeline catalog for lists, plus the artifact-backed editor projection by id. */
@RestController
class PipelineViewController {

    private final PipelineCatalogService catalog;
    private final PipelineViewService pipelines;

    PipelineViewController(PipelineCatalogService catalog, PipelineViewService pipelines) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.pipelines = Objects.requireNonNull(pipelines, "pipelines");
    }

    @Verb("pipeline.catalog")
    @GetMapping("/pipelines")
    PipelineList list(
            @RequestParam(name = "limit", required = false) Integer limit,
            @RequestParam(name = "offset", required = false) Integer offset) {
        ListWindow.Window window = ListWindow.window(limit, offset);
        return new PipelineList(catalog.list(window.offset(), window.limit()));
    }

    @Verb("pipeline.list")
    @GetMapping("/pipelines:artifacts")
    PipelineArtifactList listArtifacts(
            @RequestParam(name = "limit", required = false) Integer limit,
            @RequestParam(name = "offset", required = false) Integer offset) {
        ListWindow.Window window = ListWindow.window(limit, offset);
        return new PipelineArtifactList(pipelines.list(window.offset(), window.limit()));
    }

    @Verb("pipeline.get")
    @GetMapping("/pipelines/{id}")
    ResponseEntity<PipelineView> get(@PathVariable("id") String id) {
        PipelineView view = pipelines.get(id);
        return ResponseEntity.ok().eTag(view.contentHash()).body(view);
    }
}
