package io.tapstate.control.restapi;

import io.tapstate.control.core.AiProviderService;
import io.tapstate.control.core.AiProviderClient;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.PipelineDraftError;
import io.tapstate.control.core.PipelineDraftService;
import io.tapstate.control.core.SourceProjectionService;
import io.tapstate.control.core.SourceSchemaQueryService;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.PipelineDraft;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.CacheControl;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import tools.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Semaphore;

/** Produces transient proposals only; existing draft CAS remains the sole Apply write. */
@RestController
class PipelineAssistantController {
    private static final Semaphore CAPACITY = new Semaphore(4);
    private final ObjectProvider<PipelineDraftService> drafts;
    private final ObjectProvider<SourceProjectionService> sources;
    private final ObjectProvider<SourceSchemaQueryService> schemas;
    private final AiProviderController providers;
    private final ObjectMapper json;
    private final AiProviderClient client;
    PipelineAssistantController(ObjectProvider<PipelineDraftService> drafts,
            ObjectProvider<SourceProjectionService> sources, ObjectProvider<SourceSchemaQueryService> schemas,
            AiProviderController providers, AiProviderClient client, ObjectMapper json) {
        this.drafts = drafts; this.sources = sources; this.schemas = schemas; this.providers = providers;
        this.client = client; this.json = json;
    }
    @Verb("pipeline-draft.assistant")
    @PostMapping(value = "/pipelines/{id}/draft:assistant", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    ResponseEntity<StreamingResponseBody> assistant(@PathVariable("id") String id,
            @RequestBody Map<String, Object> body, HttpServletRequest request) {
        String owner = providers.owner(request);
        var settings = providers.service().require(owner);
        long revision = revision(body.get("revision"));
        PipelineDraft base = current(id, revision);
        List<Map<String, String>> messages = messages(body.get("messages"));
        Map<String, Object> context = context(base);
        Map<String, Object> focus = focus(base, body.get("focus"));
        if (!focus.isEmpty()) context.put("focus", focus);
        StreamingResponseBody stream = output -> {
            Object streamLock = new Object();
            if (!CAPACITY.tryAcquire()) {
                event(output, streamLock, Map.of("type", "error", "errorText", ControlError.AI_OVERLOADED.code()));
                event(output, streamLock, Map.of("type", "data-status", "data", Map.of("state", "error", "status", "error",
                        "code", ControlError.AI_OVERLOADED.code())));
                event(output, streamLock, Map.of("type", "finish", "finishReason", "error"));
                done(output, streamLock);
                return;
            }
            String textId = UUID.randomUUID().toString();
            Thread worker = Thread.currentThread();
            var heartbeat = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory());
            heartbeat.scheduleAtFixedRate(() -> {
                try { synchronized (streamLock) { output.write(": heartbeat\n\n".getBytes(StandardCharsets.UTF_8)); output.flush(); } }
                catch (IOException disconnected) { worker.interrupt(); }
            }, 5, 5, java.util.concurrent.TimeUnit.SECONDS);
            try {
                event(output, streamLock, Map.of("type", "start", "messageId", UUID.randomUUID().toString()));
                status(output, streamLock, "connecting");
                event(output, streamLock, Map.of("type", "text-start", "id", textId));
                status(output, streamLock, "streaming");
                Map<String, Object> candidate = client.propose(settings, messages, context,
                        delta -> event(output, streamLock, Map.of("type", "text-delta", "id", textId, "delta", delta)));
                event(output, streamLock, Map.of("type", "text-end", "id", textId));
                status(output, streamLock, "validating");
                Map<String, Object> validated = validate(id, revision, candidate);
                if (!Boolean.TRUE.equals(validated.get("valid"))) {
                    throw AiProviderService.error(ControlError.AI_INVALID_PROPOSAL);
                }
                Map<String, Object> proposal = new LinkedHashMap<>(validated);
                proposal.put("baseRevision", revision);
                event(output, streamLock, Map.of("type", "data-proposal", "data", proposal));
                status(output, streamLock, "ready");
                event(output, streamLock, Map.of("type", "finish", "finishReason", "stop"));
                done(output, streamLock);
            } catch (UncheckedIOException disconnected) {
                // Client cancellation closes the provider body through the streaming consumer.
            } catch (TapstateException failure) {
                try {
                    event(output, streamLock, Map.of("type", "error", "errorText", failure.code().code()));
                    event(output, streamLock, Map.of("type", "data-status", "data", Map.of("state", "error", "status", "error", "code", failure.code().code())));
                    event(output, streamLock, Map.of("type", "finish", "finishReason", "error"));
                    done(output, streamLock);
                } catch (UncheckedIOException disconnected) {
                    // A canceled client needs no further terminal event.
                }
            } finally { heartbeat.shutdownNow(); CAPACITY.release(); }
        };
        return ResponseEntity.ok().cacheControl(CacheControl.noStore())
                .header("x-vercel-ai-ui-message-stream", "v1").header("X-Accel-Buffering", "no")
                .contentType(MediaType.TEXT_EVENT_STREAM).body(stream);
    }
    @Verb("pipeline-draft.assistant-validate")
    @PostMapping("/pipelines/{id}/draft:assistant-validate")
    ResponseEntity<Map<String, Object>> validate(@PathVariable("id") String id,
            @RequestBody Map<String, Object> body, HttpServletRequest request) {
        providers.owner(request);
        Object raw = body.get("candidateWizard");
        if (!(raw instanceof Map<?, ?>)) throw malformed("candidateWizard must be an object");
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(validate(id, revision(body.get("revision")), AssistantWizardJson.object(raw)));
    }
    Map<String, Object> validate(String id, long revision, Map<String, Object> view) {
        PipelineDraft base = current(id, revision);
        try {
            if (!view.keySet().containsAll(Set.of("root", "relations", "transforms", "destinationTable", "destinationSourceId", "destinationWriteMode"))) {
                throw new IllegalArgumentException("candidateWizard is incomplete");
            }
            Map<String, Object> before = wizard(base);
            Map<String, Object> wire = AssistantWizardJson.wire(view, before);
            enforceScope(base, wire);
            Map<String, Object> input = new LinkedHashMap<>(PipelineDraftJson.write(json, base));
            input.put("wizard", wire);
            PipelineDraft candidate = PipelineDraftJson.read(json, input, id, revision, base.updatedBy());
            String afterDsl = new CanonicalWriter().write(service().previewCandidate(id, candidate));
            String beforeDsl;
            try { beforeDsl = new CanonicalWriter().write(service().preview(id, revision)); }
            catch (TapstateException incompleteBase) { beforeDsl = ""; }
            List<Map<String, Object>> changes = new ArrayList<>();
            diff("wizard", before, wizard(candidate), changes);
            current(id, revision);
            return Map.of("valid", true, "candidateWizard", AssistantWizardJson.view(wizard(candidate)),
                    "validation", Map.of("valid", true, "diagnostics", List.of()),
                    "diff", Map.of("before", beforeDsl, "after", afterDsl, "changes", changes));
        } catch (IllegalArgumentException malformed) {
            return invalid("candidateWizard does not match the supported Builder schema");
        } catch (TapstateException invalid) {
            if (invalid.code() == PipelineDraftError.REVISION_CONFLICT || invalid.code() == PipelineDraftError.NOT_FOUND) throw invalid;
            return invalid(invalid.code().code());
        }
    }
    static Map<String, Object> focus(PipelineDraft base, Object raw) {
        if (!(raw instanceof Map<?, ?> input)) return Map.of();
        String kind = AssistantWizardJson.text(input.get("kind"));
        if (!Set.of("root", "relation", "transforms").contains(kind)) return Map.of();
        String node = AssistantWizardJson.text(input.get("nodeId"));
        if (node.length() > 200) return Map.of();
        if (kind.equals("root") && node.equals("root")) return Map.of("kind", kind, "nodeId", node);
        if (kind.equals("transforms") && node.equals("doc_process")) return Map.of("kind", kind, "nodeId", node);
        if (kind.equals("relation") && base.wizard().related().stream().anyMatch(related -> related.id().equals(node))) {
            return Map.of("kind", kind, "nodeId", node);
        }
        return Map.of("kind", kind);
    }
    private Map<String, Object> context(PipelineDraft base) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("currentWizard", AssistantWizardJson.view(wizard(base)));
        List<Map<String, Object>> inventory = new ArrayList<>();
        for (String sourceId : inputSources(base)) {
            var source = sources.getObject().get(sourceId);
            String connector = source.connector().toLowerCase(java.util.Locale.ROOT);
            if (!connector.contains("postgres") && !connector.contains("mysql")) throw malformed("assistant inputs require PostgreSQL or MySQL");
            var schema = schemas.getObject().find(sourceId).orElseThrow(() -> malformed("discover the selected Source schema before requesting a proposal"));
            List<Map<String, Object>> tables = schema.tables().stream().map(table -> Map.<String, Object>of(
                    "name", table.name(), "primaryKey", table.primaryKey(), "fields", table.fields().stream()
                            .map(field -> Map.of("name", field.name(), "type", field.type() == null ? "unknown" : field.type())).toList())).toList();
            inventory.add(Map.of("sourceId", sourceId, "connector", source.connector(), "tables", tables));
        }
        if (inventory.isEmpty()) throw malformed("select a root table before requesting a proposal");
        result.put("sources", inventory);
        if (JsonWriter.write(result).length() > 120000) throw malformed("selected schema is too large for the assistant");
        return result;
    }
    private void enforceScope(PipelineDraft base, Map<String, Object> candidate) {
        Set<String> allowed = inputSources(base);
        List<Map<String, Object>> inputs = new ArrayList<>(); inputs.add(AssistantWizardJson.object(candidate.get("root")));
        inputs.addAll(AssistantWizardJson.list(candidate.get("related")).stream().map(AssistantWizardJson::object).toList());
        for (var input : inputs) {
            String source = AssistantWizardJson.text(input.get("sourceId"));
            if (!allowed.contains(source)) throw malformed("candidate references an unselected Source");
            var schema = schemas.getObject().find(source).orElseThrow(() -> malformed("selected Source has no discovered schema"));
            if (schema.tables().stream().noneMatch(table -> table.name().equals(input.get("table")))) throw malformed("candidate references an unknown table");
        }
        var beforeOutput = AssistantWizardJson.object(AssistantWizardJson.object(wizard(base).get("output")).get("config"));
        var output = AssistantWizardJson.object(AssistantWizardJson.object(candidate.get("output")).get("config"));
        if (!Objects.equals(beforeOutput.get("sourceId"), output.get("sourceId"))) throw malformed("candidate may not change the selected destination Source");
    }
    private Set<String> inputSources(PipelineDraft draft) {
        Set<String> ids = new LinkedHashSet<>();
        if (draft.wizard().root() != null && draft.wizard().root().sourceId() != null && !draft.wizard().root().sourceId().isBlank()) ids.add(draft.wizard().root().sourceId());
        for (var related : draft.wizard().related()) if (related.sourceId() != null && !related.sourceId().isBlank()) ids.add(related.sourceId());
        return ids;
    }
    private PipelineDraft current(String id, long revision) {
        PipelineDraft draft = service().find(id).orElseThrow(() -> new TapstateException(PipelineDraftError.NOT_FOUND, Map.of("id", id), null));
        if (draft.revision() != revision) throw new TapstateException(PipelineDraftError.REVISION_CONFLICT, Map.of("id", id), null);
        if (draft.mode() != PipelineDraft.Mode.WIZARD) throw new TapstateException(PipelineDraftError.MODE_CONFLICT, Map.of("id", id), null);
        return draft;
    }
    private PipelineDraftService service() { return drafts.getObject(); }
    private Map<String, Object> wizard(PipelineDraft draft) { return AssistantWizardJson.object(PipelineDraftJson.write(json, draft).get("wizard")); }
    private static Map<String, Object> invalid(String code) {
        return Map.of("valid", false, "validation", Map.of("valid", false, "diagnostics", List.of(Map.of("code", code))), "diff", Map.of("changes", List.of()));
    }
    private static void diff(String path, Object before, Object after, List<Map<String, Object>> changes) {
        if (Objects.equals(before, after)) return;
        if (before instanceof Map<?, ?> left && after instanceof Map<?, ?> right) {
            Set<Object> keys = new LinkedHashSet<>(left.keySet()); keys.addAll(right.keySet());
            for (Object key : keys) diff(path + "." + key, left.get(key), right.get(key), changes);
        } else {
            Map<String, Object> change = new LinkedHashMap<>(); change.put("path", path); change.put("before", before); change.put("after", after); changes.add(change);
        }
    }
    private static List<Map<String, String>> messages(Object raw) {
        if (!(raw instanceof List<?> items) || items.isEmpty() || items.size() > 24) throw malformed("messages must contain 1 to 24 turns");
        List<Map<String, String>> messages = new ArrayList<>(); int length = 0;
        for (Object item : items) {
            if (!(item instanceof Map<?, ?>)) throw malformed("messages must contain objects");
            var message = AssistantWizardJson.object(item); String role = AssistantWizardJson.text(message.get("role"));
            String text = AssistantWizardJson.text(message.get("text")); length += text.length();
            if (!List.of("user", "assistant").contains(role) || text.isBlank() || length > 24000) throw malformed("messages contain unsupported or excessive content");
            messages.add(Map.of("role", role, "content", text));
        }
        return messages;
    }
    private static long revision(Object value) {
        if (!(value instanceof Number)) throw malformed("revision must be a positive integer");
        try {
            long revision = new BigDecimal(value.toString()).longValueExact();
            if (revision < 1) {
                throw new ArithmeticException();
            }
            return revision;
        }
        catch (ArithmeticException | NumberFormatException error) { throw malformed("revision must be a positive integer"); }
    }
    private static TapstateException malformed(String reason) { return new TapstateException(ControlError.MALFORMED_REQUEST, Map.of("reason", reason), null); }
    private static void status(OutputStream output, Object streamLock, String state) { event(output, streamLock, Map.of("type", "data-status", "data", Map.of("state", state, "status", state))); }
    private static void event(OutputStream output, Object streamLock, Map<String, Object> event) {
        try { synchronized (streamLock) { output.write(("data: " + JsonWriter.write(event) + "\n\n").getBytes(StandardCharsets.UTF_8)); output.flush(); } }
        catch (IOException error) { throw new UncheckedIOException(error); }
    }
    private static void done(OutputStream output, Object streamLock) {
        try { synchronized (streamLock) { output.write("data: [DONE]\n\n".getBytes(StandardCharsets.UTF_8)); output.flush(); } }
        catch (IOException error) { throw new UncheckedIOException(error); }
    }
}
