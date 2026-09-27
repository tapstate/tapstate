package io.tapstate.control.restapi;

import io.tapstate.control.core.ApplyResult;
import io.tapstate.control.core.ApplyService;
import io.tapstate.control.core.ArtifactDraft;
import io.tapstate.control.core.ArtifactError;
import io.tapstate.control.core.ArtifactMutationService;
import io.tapstate.control.core.ArtifactQueryService;
import io.tapstate.control.core.ArtifactValidationResult;
import io.tapstate.control.core.StoredArtifact;
import io.tapstate.core.common.TapstateException;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * The artifact verbs projected onto HTTP: apply and delete (write), get and list (read). Each handler is a thin
 * pass-through to a control-core service — it decodes the request, calls the verb, and encodes the
 * result — and carries no business logic of its own. Every handler is annotated with the operation id
 * it projects ({@link Verb}); mounted under the {@code /api} prefix by the path configuration.
 */
@RestController
class ArtifactController {

    /** An {@code If-Match} carrying exactly one canonical content hash, quoted as an entity tag. */
    private static final String QUOTED_HASH = "\"[0-9a-f]{64}\"";

    private final ApplyService applyService;
    private final ArtifactQueryService queryService;
    private final ArtifactMutationService mutationService;

    ArtifactController(ApplyService applyService, ArtifactQueryService queryService,
                       ArtifactMutationService mutationService) {
        this.applyService = applyService;
        this.queryService = queryService;
        this.mutationService = mutationService;
    }

    @Verb("artifact.apply")
    @PostMapping("/artifacts:apply")
    ApplyResult apply(@RequestBody ApplyRequest request) {
        // Refuse a body with no drafts array at the boundary as a coded 400, rather than letting a null trip
        // the service's bare invariant guard (a 500). A missing body is already a framework-level 400 upstream.
        List<ArtifactDraft> drafts = requireDrafts(request);
        return applyService.apply(AuthenticatedCaller.subject(), drafts, project(request));
    }

    @Verb("artifact.validate")
    @PostMapping("/artifacts:validate")
    ArtifactValidationResult validate(@RequestBody ApplyRequest request) {
        List<ArtifactDraft> drafts = requireDrafts(request);
        return applyService.validate(drafts, project(request));
    }

    /** The project a request is applied from, or null when it names none; a blank name is refused. */
    private static String project(ApplyRequest request) {
        String project = request.project();
        if (project != null && project.isBlank()) {
            throw MalformedRequest.rejecting("`project`, when present, must name a project", null);
        }
        return project;
    }

    private static List<ArtifactDraft> requireDrafts(ApplyRequest request) {
        List<ArtifactDraft> drafts = MalformedRequest.require(
                request == null ? null : request.drafts(), "the request must carry a `drafts` array");
        for (ArtifactDraft draft : drafts) {
            if (draft == null || draft.content().isBlank()) {
                throw MalformedRequest.rejecting("each draft must carry non-blank content", null);
            }
        }
        return drafts;
    }

    @Verb("artifact.get")
    @GetMapping("/artifacts/{id}")
    ResponseEntity<StoredArtifact> get(@PathVariable("id") String id) {
        // ResponseEntity.of maps a present artifact to 200 and an absent one to 404, with no error logic here.
        return ResponseEntity.of(queryService.get(id));
    }

    @Verb("artifact.list")
    @GetMapping("/artifacts")
    ArtifactList list(@RequestParam(name = "kind", required = false) String kind) {
        return new ArtifactList(queryService.list(kind));
    }

    @Verb("artifact.delete")
    @DeleteMapping("/artifacts/{id}")
    ResponseEntity<Void> delete(
            @PathVariable("id") String id,
            @RequestHeader(name = HttpHeaders.IF_MATCH, required = false) String ifMatch) {
        mutationService.delete(AuthenticatedCaller.subject(), id, expectedHash(id, ifMatch));
        return ResponseEntity.status(HttpStatus.NO_CONTENT).build();
    }

    /**
     * The precondition an unconditional delete is missing. A header that is absent and one that is present
     * but not a quoted hash are the same refusal: passing a malformed value through would reach the store
     * as a hash that simply does not match, answering "someone else changed it" (412) for a request that
     * never carried a version at all (428).
     */
    private static String expectedHash(String id, String ifMatch) {
        if (ifMatch == null || !ifMatch.matches(QUOTED_HASH)) {
            throw new TapstateException(ArtifactError.PRECONDITION_REQUIRED, Map.of("id", id), null);
        }
        return ifMatch.substring(1, ifMatch.length() - 1);
    }
}
