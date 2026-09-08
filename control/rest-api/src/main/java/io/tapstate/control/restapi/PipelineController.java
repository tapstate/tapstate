package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineLifecycleService;
import io.tapstate.core.lifecycle.DesiredState;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * The pipeline lifecycle verbs projected onto HTTP: start / stop / pause / resume, each a custom method on
 * a pipeline instance ({@code POST /api/pipelines/{id}:start}). Each handler is a thin pass-through to the
 * control-core lifecycle service — it names the target pipeline and the authenticated caller, then calls
 * the verb — and carries no business logic of its own: the state-machine check, the revision-compatibility
 * check, and the audited desired-state write all live in the service.
 *
 * <p>The caller principal is read from Spring Security's current context, so the audited write records the
 * real caller rather than a placeholder.
 * There is deliberately no {@code rewind} verb: a re-dig is the explicit two-step stop then start, composed
 * by the caller.
 *
 * <p>Stop is the one verb here that takes a body, and the one that refuses without it. It is also the only
 * one that can destroy something a caller may have wanted: reading the absent answer as either yes or no
 * would make the same request mean two different outcomes to two callers, and only one of them finds out.
 */
@RestController
class PipelineController {

    private final PipelineLifecycleService lifecycle;

    PipelineController(PipelineLifecycleService lifecycle) {
        this.lifecycle = lifecycle;
    }

    @Verb("pipeline.start")
    @PostMapping("/pipelines/{id}:start")
    DesiredState start(@PathVariable("id") String id) {
        return lifecycle.start(AuthenticatedCaller.subject(), id);
    }

    @Verb("pipeline.stop")
    @PostMapping("/pipelines/{id}:stop")
    DesiredState stop(@PathVariable("id") String id,
            @RequestBody(required = false) PipelineStopRequest request) {
        return lifecycle.stop(
                AuthenticatedCaller.subject(), id, PipelineStopRequest.purgeStateOf(request, id));
    }

    @Verb("pipeline.pause")
    @PostMapping("/pipelines/{id}:pause")
    DesiredState pause(@PathVariable("id") String id) {
        return lifecycle.pause(AuthenticatedCaller.subject(), id);
    }

    @Verb("pipeline.resume")
    @PostMapping("/pipelines/{id}:resume")
    DesiredState resume(@PathVariable("id") String id) {
        return lifecycle.resume(AuthenticatedCaller.subject(), id);
    }
}
