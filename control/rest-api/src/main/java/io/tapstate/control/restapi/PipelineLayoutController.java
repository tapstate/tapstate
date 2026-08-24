package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineLayoutService;
import io.tapstate.spi.store.PipelineLayout;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Objects;

/** HTTP projection for editor-only Pipeline canvas layout state. */
@RestController
class PipelineLayoutController {

    private final PipelineLayoutService layouts;

    PipelineLayoutController(PipelineLayoutService layouts) {
        this.layouts = Objects.requireNonNull(layouts, "layouts");
    }

    @Verb("pipeline.layout.get")
    @GetMapping("/pipelines/{id}/layout")
    PipelineLayout get(@PathVariable("id") String id) {
        return layouts.get(id);
    }

    @Verb("pipeline.layout.update")
    @PutMapping("/pipelines/{id}/layout")
    PipelineLayout update(@PathVariable("id") String id, @RequestBody PipelineLayoutUpdate update) {
        return layouts.save(id, update.nodes(), update.viewport());
    }

    /** The request deliberately excludes pipeline id: the path owns identity. */
    record PipelineLayoutUpdate(Map<String, PipelineLayout.NodePosition> nodes, PipelineLayout.Viewport viewport) {
        PipelineLayoutUpdate {
            nodes = Map.copyOf(Objects.requireNonNull(nodes, "nodes"));
        }
    }
}
