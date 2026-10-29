package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineDeletionService;
import io.tapstate.control.core.PipelineDraftError;
import io.tapstate.core.common.TapstateException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Objects;

/** Whole-Pipeline deletion for the Web authoring surface. */
@RestController
class PipelineDeletionController {

    private static final String NO_DRAFT_ETAG = "\"none\"";
    private static final String QUOTED_REVISION = "\"[1-9][0-9]*\"";

    private final ObjectProvider<PipelineDeletionService> deletion;

    PipelineDeletionController(ObjectProvider<PipelineDeletionService> deletion) {
        this.deletion = Objects.requireNonNull(deletion, "deletion");
    }

    @Verb("pipeline.delete")
    @DeleteMapping("/pipelines/{id}")
    ResponseEntity<Void> delete(
            @PathVariable("id") String id,
            @RequestHeader(name = HttpHeaders.IF_MATCH, required = false) String ifMatch,
            @RequestHeader(name = "If-Draft-Match", required = false) String ifDraftMatch) {
        String expectedArtifactHash = ArtifactController.expectedHash(id, ifMatch);
        Long expectedDraftRevision = expectedDraftRevision(id, ifDraftMatch);
        deletion.getObject().delete(AuthenticatedCaller.subject(), id, expectedArtifactHash, expectedDraftRevision);
        return ResponseEntity.noContent().build();
    }

    private static Long expectedDraftRevision(String id, String ifMatch) {
        if (ifMatch == null) {
            throw new TapstateException(PipelineDraftError.PRECONDITION_REQUIRED, Map.of("id", id), null);
        }
        if (NO_DRAFT_ETAG.equals(ifMatch)) {
            return null;
        }
        if (!ifMatch.matches(QUOTED_REVISION)) {
            throw new TapstateException(PipelineDraftError.PRECONDITION_REQUIRED, Map.of("id", id), null);
        }
        try {
            return Long.parseLong(ifMatch.substring(1, ifMatch.length() - 1));
        } catch (NumberFormatException error) {
            throw new TapstateException(PipelineDraftError.PRECONDITION_REQUIRED, Map.of("id", id), error);
        }
    }
}
