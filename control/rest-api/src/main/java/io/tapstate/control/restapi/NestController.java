package io.tapstate.control.restapi;

import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.NestInspectService;
import io.tapstate.control.core.NestStateInspector;
import io.tapstate.control.core.NestProgress;
import io.tapstate.control.core.NestProgressService;
import io.tapstate.control.core.PipelineObservationQueryService;
import io.tapstate.core.lifecycle.NestInspection;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * The nest reads, projected onto HTTP. A thin pass-through: which tables a step depends on and what it is
 * waiting for are worked out behind the service, from the definition and the published observation.
 */
@RestController
class NestController {

    private final NestProgressService progress;
    private final NestInspectService inspect;

    NestController(ArtifactQueryService artifacts, PipelineObservationQueryService observations,
            ObjectProvider<NestStateInspector> inspector) {
        this.progress = new NestProgressService(artifacts, observations);
        this.inspect = new NestInspectService(artifacts, inspector.getIfAvailable(() -> NestStateInspector.NONE));
    }

    @Verb("pipeline.nest.progress")
    @GetMapping("/pipelines/{id}/nest/progress")
    ResponseEntity<NestProgress> progress(@PathVariable("id") String id,
            @RequestParam(name = "step", required = false) String step) {
        return ResponseEntity.ok()
                .cacheControl(CacheControl.noStore())
                .body(progress.progress(id, step == null || step.isBlank() ? null : step));
    }

    /** What {@code step} holds under the root key, one {@code key} parameter per root key column, in order. */
    @Verb("pipeline.nest.inspect")
    @GetMapping("/pipelines/{id}/nest/{step}/documents")
    ResponseEntity<NestInspection> inspect(@PathVariable("id") String id, @PathVariable("step") String step,
            @RequestParam(name = "key") List<String> key) {
        return ResponseEntity.ok()
                .cacheControl(CacheControl.noStore())
                .body(inspect.inspect(id, step, key));
    }
}
