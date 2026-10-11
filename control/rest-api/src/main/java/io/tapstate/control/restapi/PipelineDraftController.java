package io.tapstate.control.restapi;

import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.PipelineDraftError;
import io.tapstate.control.core.PipelineDraftService;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.PipelineDraft;
import io.tapstate.spi.store.PipelineDraftMutation;
import io.tapstate.spi.store.PipelineDraftSummary;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.util.UriComponentsBuilder;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.net.URI;
import java.util.Map;
import java.util.List;
import java.util.Objects;

/** HTTP projection for durable Pipeline authoring drafts. */
@RestController
class PipelineDraftController {

    private final ObjectProvider<PipelineDraftService> service;
    private final ObjectMapper json;

    PipelineDraftController(ObjectProvider<PipelineDraftService> service, ObjectMapper json) {
        this.service = Objects.requireNonNull(service, "service");
        this.json = Objects.requireNonNull(json, "json");
    }

    @Verb("pipeline-draft.list")
    @GetMapping("/pipeline-drafts")
    Map<String, Object> list(
            @RequestParam(name = "limit", required = false) Integer limit,
            @RequestParam(name = "offset", required = false) Integer offset) {
        ListWindow.Window window = ListWindow.window(limit, offset);
        List<Map<String, Object>> items = service().listSummaries(window.offset(), window.limit()).stream()
                .map(PipelineDraftController::summaryJson).toList();
        return Map.of("items", items);
    }

    @Verb("pipeline-draft.get")
    @GetMapping("/pipelines/{id}/draft")
    ResponseEntity<Map<String, Object>> get(@PathVariable("id") String id) {
        PipelineDraft draft = service().find(id).orElseThrow(() -> error(PipelineDraftError.NOT_FOUND, id));
        return response(draft);
    }

    @Verb("pipeline-draft.create")
    @PostMapping("/pipelines/{id}/draft")
    ResponseEntity<Map<String, Object>> create(@PathVariable("id") String id,
            @RequestBody Map<String, Object> body) {
        String actor = AuthenticatedCaller.subject();
        PipelineDraft draft = parse(body, id, 1, actor);
        PipelineDraftMutation outcome = service().create(actor, draft);
        refuse(outcome, id);
        PipelineDraft created = service().find(id).orElseThrow(() -> error(PipelineDraftError.NOT_FOUND, id));
        URI location = UriComponentsBuilder.fromPath("/api/pipelines/{id}/draft")
                .buildAndExpand(id).encode().toUri();
        return ResponseEntity.created(location)
                .eTag(etag(created.revision())).body(PipelineDraftJson.write(json, created));
    }

    @Verb("pipeline-draft.replace")
    @PutMapping("/pipelines/{id}/draft")
    ResponseEntity<Map<String, Object>> replace(@PathVariable("id") String id,
            @RequestHeader(name = HttpHeaders.IF_MATCH, required = false) String ifMatch,
            @RequestBody Map<String, Object> body) {
        long expected = expectedRevision(id, ifMatch);
        String actor = AuthenticatedCaller.subject();
        PipelineDraft replacement = parse(body, id, expected + 1, actor);
        PipelineDraftMutation outcome = service().save(actor, id, expected, replacement);
        refuse(outcome, id);
        return response(service().find(id).orElseThrow(() -> error(PipelineDraftError.NOT_FOUND, id)));
    }

    @Verb("pipeline-draft.delete")
    @DeleteMapping("/pipelines/{id}/draft")
    ResponseEntity<Void> delete(@PathVariable("id") String id,
            @RequestHeader(name = HttpHeaders.IF_MATCH, required = false) String ifMatch) {
        long expected = expectedRevision(id, ifMatch);
        PipelineDraftMutation outcome = service().discard(AuthenticatedCaller.subject(), id, expected);
        refuse(outcome, id);
        return ResponseEntity.noContent().build();
    }

    @Verb("pipeline-draft.preview")
    @PostMapping("/pipelines/{id}/draft:preview")
    Map<String, Object> preview(@PathVariable("id") String id,
            @RequestBody(required = false) Map<String, Object> body) {
        long expected = body == null ? currentRevision(id) : number(body.get("revision"), "revision");
        PipelineResource artifact;
        if (body != null && body.containsKey("candidate")) {
            if (!(body.get("candidate") instanceof Map<?, ?> raw)) {
                throw new TapstateException(ControlError.MALFORMED_REQUEST,
                        Map.of("reason", "candidate must be a draft object"), null);
            }
            Map<String, Object> candidate = new java.util.LinkedHashMap<>();
            raw.forEach((key, value) -> {
                if (!(key instanceof String field)) {
                    throw new TapstateException(ControlError.MALFORMED_REQUEST,
                            Map.of("reason", "candidate field names must be strings"), null);
                }
                candidate.put(field, value);
            });
            artifact = service().previewCandidate(id,
                    parse(candidate, id, expected, AuthenticatedCaller.subject()));
        } else {
            artifact = service().preview(id, expected);
        }
        return Map.of("pipelineId", id, "revision", expected, "artifact", artifact,
                "dsl", new CanonicalWriter().write(artifact));
    }

    @Verb("pipeline-draft.publish")
    @PostMapping("/pipelines/{id}/draft:publish")
    Map<String, Object> publish(@PathVariable("id") String id, @RequestBody Map<String, Object> body) {
        long expected = number(body.get("revision"), "revision");
        String artifactHash = body.get("artifactHash") instanceof String value ? value : null;
        PipelineDraftService.PublishResult result = service().publish(
                id, expected, artifactHash, AuthenticatedCaller.subject());
        refuse(result.mutation(), id);
        return Map.of("pipelineId", id, "revision", expected, "artifactHash", result.artifactHash(),
                "published", true, "warnings", result.warnings());
    }

    @Verb("pipeline-draft.rebase")
    @PostMapping("/pipelines/{id}/draft:rebase")
    ResponseEntity<Map<String, Object>> rebase(@PathVariable("id") String id,
            @RequestBody Map<String, Object> body) {
        long expected = number(body.get("revision"), "revision");
        if (!body.containsKey("artifactHash")) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "artifactHash must be provided, as a string or null"), null);
        }
        Object hash = body.get("artifactHash");
        if (hash != null && !(hash instanceof String)) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", "artifactHash must be a string or null"), null);
        }
        PipelineDraftMutation outcome = service().rebase(
                AuthenticatedCaller.subject(), id, expected, (String) hash);
        refuse(outcome, id);
        PipelineDraft rebased = service().find(id).orElseThrow(() -> error(PipelineDraftError.NOT_FOUND, id));
        return ResponseEntity.ok().eTag(etag(rebased.revision())).body(PipelineDraftJson.write(json, rebased));
    }

    private ResponseEntity<Map<String, Object>> response(PipelineDraft draft) {
        return ResponseEntity.ok().eTag(etag(draft.revision())).body(PipelineDraftJson.write(json, draft));
    }

    private static Map<String, Object> summaryJson(PipelineDraftSummary draft) {
        Map<String, Object> result = new java.util.LinkedHashMap<>();
        result.put("pipelineId", draft.pipelineId());
        result.put("mode", draft.mode().name().toLowerCase(java.util.Locale.ROOT));
        result.put("name", draft.name());
        result.put("description", draft.description());
        result.put("revision", draft.revision());
        result.put("baseArtifactHash", draft.baseArtifactHash());
        result.put("publishedDraftRevision", draft.publishedDraftRevision());
        result.put("publishedArtifactHash", draft.publishedArtifactHash());
        result.put("createdAt", draft.createdAt());
        result.put("updatedAt", draft.updatedAt());
        result.put("updatedBy", draft.updatedBy());
        return result;
    }

    private PipelineDraftService service() {
        PipelineDraftService value = service.getIfAvailable();
        if (value == null) {
            throw new IllegalStateException("pipeline draft service is not configured");
        }
        return value;
    }

    private PipelineDraft parse(Map<String, Object> body, String id, long defaultRevision, String actor) {
        try {
            return PipelineDraftJson.read(json, body, id, defaultRevision, actor);
        } catch (RuntimeException error) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", error.getMessage() == null ? "invalid draft" : error.getMessage()), error);
        }
    }

    private long currentRevision(String id) {
        return service().find(id).map(PipelineDraft::revision)
                .orElseThrow(() -> error(PipelineDraftError.NOT_FOUND, id));
    }

    private static long expectedRevision(String id, String ifMatch) {
        if (ifMatch == null || !ifMatch.matches("\"[1-9][0-9]*\"")) {
            throw error(PipelineDraftError.PRECONDITION_REQUIRED, id);
        }
        try {
            return Long.parseLong(ifMatch.substring(1, ifMatch.length() - 1));
        } catch (NumberFormatException error) {
            throw error(PipelineDraftError.PRECONDITION_REQUIRED, id);
        }
    }

    private static long number(Object value, String field) {
        if (!(value instanceof Number number)) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", field + " must be a positive integer"), null);
        }
        try {
            long result = new BigDecimal(number.toString()).longValueExact();
            if (result < 1) {
                throw new ArithmeticException("integer must be positive");
            }
            return result;
        } catch (NumberFormatException | ArithmeticException error) {
            throw new TapstateException(ControlError.MALFORMED_REQUEST,
                    Map.of("reason", field + " must be a positive integer in range"), error);
        }
    }

    private static String etag(long revision) {
        return "\"" + revision + "\"";
    }

    private static void refuse(PipelineDraftMutation outcome, String id) {
        if (outcome == PipelineDraftMutation.CREATED || outcome == PipelineDraftMutation.REPLACED
                || outcome == PipelineDraftMutation.DELETED || outcome == PipelineDraftMutation.PUBLISHED) {
            return;
        }
        PipelineDraftError error = switch (outcome) {
            case NOT_FOUND -> PipelineDraftError.NOT_FOUND;
            case ALREADY_EXISTS -> PipelineDraftError.ALREADY_EXISTS;
            case REVISION_CONFLICT -> PipelineDraftError.REVISION_CONFLICT;
            case MODE_CONFLICT -> PipelineDraftError.MODE_CONFLICT;
            case ARTIFACT_CONFLICT -> PipelineDraftError.ARTIFACT_CONFLICT;
            default -> throw new IllegalStateException("unexpected draft mutation: " + outcome);
        };
        throw error(error, id);
    }

    private static TapstateException error(PipelineDraftError error, String id) {
        return new TapstateException(error, Map.of("id", id), null);
    }
}
