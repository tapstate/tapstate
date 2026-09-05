package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineView;
import io.tapstate.control.core.PipelineViewService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

import java.util.Objects;

/** Structured JSON projection of static Pipeline artifacts. */
@RestController
class PipelineViewController {

    private final PipelineViewService pipelines;

    PipelineViewController(PipelineViewService pipelines) {
        this.pipelines = Objects.requireNonNull(pipelines, "pipelines");
    }

    @Verb("pipeline.list")
    @GetMapping("/pipelines")
    PipelineList list() {
        return new PipelineList(pipelines.list());
    }

    @Verb("pipeline.get")
    @GetMapping("/pipelines/{id}")
    ResponseEntity<PipelineView> get(@PathVariable("id") String id) {
        PipelineView view = pipelines.get(id);
        return ResponseEntity.ok().eTag(view.contentHash()).body(view);
    }
}
