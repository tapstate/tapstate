package io.tapstate.control.restapi;

import io.tapstate.control.core.AggregateStepChecks;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Checking and repairing a running aggregate step, projected onto HTTP as two acts on the step:
 * {@code POST /api/pipelines/{id}/aggregates/{step}:check} and {@code ...:repair}. Both are thin
 * pass-throughs; which member answers, the refusal of a step that is not running, and the audit of a
 * repair all live behind the service.
 *
 * <p>Both are {@code POST}. They ask the running step to do something and carry a body that names a
 * group or a place in the walk over the groups; a check changes nothing but is not a read of a stored
 * document either.
 *
 * <p>A body is not required. One that is absent asks for the first page of the step's groups.
 */
@RestController
class AggregateStepController {

    private final AggregateStepChecks checks;

    AggregateStepController(AggregateStepChecks checks) {
        this.checks = checks;
    }

    @Verb("pipeline.aggregate-check")
    @PostMapping("/pipelines/{id}/aggregates/{step}:check")
    AggregateStepChecks.Report check(@PathVariable("id") String id, @PathVariable("step") String step,
            @RequestBody(required = false) AggregateStepChecks.Request request) {
        return checks.check(id, step, request == null ? new AggregateStepChecks.Request(null, null) : request);
    }

    @Verb("pipeline.aggregate-repair")
    @PostMapping("/pipelines/{id}/aggregates/{step}:repair")
    AggregateStepChecks.Report repair(@PathVariable("id") String id, @PathVariable("step") String step,
            @RequestBody(required = false) AggregateStepChecks.Request request) {
        return checks.repair(AuthenticatedCaller.subject(), id, step,
                request == null ? new AggregateStepChecks.Request(null, null) : request);
    }
}
