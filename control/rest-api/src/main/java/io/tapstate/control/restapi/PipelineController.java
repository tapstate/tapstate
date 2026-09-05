package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineError;
import io.tapstate.control.core.PipelineInput;
import io.tapstate.control.core.PipelineLifecycleService;
import io.tapstate.control.core.PipelineProjectionService;
import io.tapstate.control.core.PipelineView;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import java.net.URI;
import java.util.Map;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

/**
 * The Pipeline write verbs projected onto HTTP: a conditional structured replacement at
 * {@code PUT /api/pipelines/{id}} and lifecycle start / stop / pause / resume custom methods on a pipeline
 * instance ({@code POST /api/pipelines/{id}:start}). Each handler delegates to a control-core projection or
 * lifecycle service — it names the target pipeline and the authenticated caller, then calls the verb — and
 * carries no business logic of its own: request mapping, workspace validation, optimistic concurrency, the
 * state-machine check, and the audited writes live in those services.
 *
 * <p>The caller principal is read from Spring Security's current context, so the audited write records the
 * real caller rather than a placeholder.
 * There is deliberately no {@code rewind} verb: a re-dig is the explicit two-step stop then start, composed
 * by the caller.
 */
@RestController
class PipelineController {

    private static final String QUOTED_HASH = "\"[0-9a-f]{64}\"";

    private final PipelineLifecycleService lifecycle;
    private final PipelineProjectionService pipelines;

    PipelineController(PipelineLifecycleService lifecycle, PipelineProjectionService pipelines) {
        this.lifecycle = lifecycle;
        this.pipelines = pipelines;
    }

    @Verb("pipeline.create")
    @PostMapping("/pipelines")
    ResponseEntity<PipelineView> create(@RequestBody PipelineInput input) {
        PipelineView created = pipelines.create(AuthenticatedCaller.subject(), input);
        return ResponseEntity.created(URI.create("/api/pipelines/" + created.id()))
                .eTag(created.contentHash())
                .body(created);
    }

    @Verb("pipeline.update")
    @PutMapping("/pipelines/{id}")
    ResponseEntity<PipelineView> replace(
            @PathVariable("id") String id,
            @RequestHeader(name = HttpHeaders.IF_MATCH, required = false) String ifMatch,
            @RequestBody PipelineInput input) {
        PipelineView replaced = pipelines.replace(
                AuthenticatedCaller.subject(), id, expectedHash(id, ifMatch), input);
        return ResponseEntity.ok().eTag(replaced.contentHash()).body(replaced);
    }

    @Verb("pipeline.start")
    @PostMapping("/pipelines/{id}:start")
    DesiredState start(@PathVariable("id") String id) {
        return lifecycle.start(AuthenticatedCaller.subject(), id);
    }

    @Verb("pipeline.stop")
    @PostMapping("/pipelines/{id}:stop")
    DesiredState stop(@PathVariable("id") String id) {
        return lifecycle.stop(AuthenticatedCaller.subject(), id);
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

    private static String expectedHash(String id, String ifMatch) {
        if (ifMatch == null || !ifMatch.matches(QUOTED_HASH)) {
            throw new TapstateException(PipelineError.PRECONDITION_REQUIRED, Map.of("id", id), null);
        }
        return ifMatch.substring(1, ifMatch.length() - 1);
    }
}
