package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelinePosition;
import io.tapstate.control.core.PipelinePositionService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Where a pipeline resumes from, projected onto HTTP: a {@code GET} that reads it and a {@code PUT} of
 * the same document that moves it ({@code /api/pipelines/{id}/position}). Both handlers are thin
 * pass-throughs to the control-core service — the guard on writing under a live reader, the refusal of
 * edits outside the resume point, and the audited write all live there.
 *
 * <p>The same shape both ways on purpose. What is read is what is edited and handed back, so nothing has
 * to be reassembled by hand out of a different document, and the values that must not move travel with
 * the one that may — which is what lets the write refuse a changed reading by name rather than silently
 * dropping it.
 *
 * <p>A {@code PUT} rather than a custom method: this replaces a named sub-resource with the version the
 * caller holds. The lifecycle verbs next door are {@code POST .../{id}:verb} because they ask for an act,
 * not for a document to take a value.
 *
 * <p>The path names the pipeline, and a body is not required to repeat it. One that arrives empty asks
 * for no move, which the service refuses by name rather than answering as a write-back that happened.
 */
@RestController
class PipelinePositionController {

    private final PipelinePositionService positions;

    PipelinePositionController(PipelinePositionService positions) {
        this.positions = positions;
    }

    @Verb("pipeline.position")
    @GetMapping("/pipelines/{id}/position")
    PipelinePosition position(@PathVariable("id") String id) {
        return positions.read(id);
    }

    @Verb("pipeline.set-position")
    @PutMapping("/pipelines/{id}/position")
    PipelinePosition setPosition(
            @PathVariable("id") String id, @RequestBody(required = false) PipelinePosition requested) {
        return positions.writeBack(AuthenticatedCaller.subject(), id,
                requested == null ? new PipelinePosition(id, List.of()) : requested);
    }
}
