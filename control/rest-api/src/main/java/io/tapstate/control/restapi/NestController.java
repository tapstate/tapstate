package io.tapstate.control.restapi;

import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.NestProgress;
import io.tapstate.control.core.NestProgressService;
import io.tapstate.control.core.PipelineObservationQueryService;
import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * The nest reads, projected onto HTTP. A thin pass-through: which tables a step depends on and what it is
 * waiting for are worked out behind the service, from the definition and the published observation.
 */
@RestController
class NestController {

    private final NestProgressService progress;

    NestController(ArtifactQueryService artifacts, PipelineObservationQueryService observations) {
        this.progress = new NestProgressService(artifacts, observations);
    }

    @Verb("pipeline.nest.progress")
    @GetMapping("/pipelines/{id}/nest/progress")
    ResponseEntity<NestProgress> progress(@PathVariable("id") String id,
            @RequestParam(name = "step", required = false) String step) {
        return ResponseEntity.ok()
                .cacheControl(CacheControl.noStore())
                .body(progress.progress(id, step == null || step.isBlank() ? null : step));
    }
}
