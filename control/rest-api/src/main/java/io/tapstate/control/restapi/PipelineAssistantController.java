package io.tapstate.control.restapi;

import io.tapstate.control.core.AiProviderService;
import io.tapstate.control.core.AiProviderClient;
import io.tapstate.control.core.ControlError;
import io.tapstate.control.core.PipelineDraftError;
import io.tapstate.control.core.PipelineDraftService;
import io.tapstate.control.core.SourceProjectionService;
import io.tapstate.control.core.SourceError;
import io.tapstate.control.core.SchemaReport;
import io.tapstate.control.core.SourceSchemaQueryService;
import io.tapstate.control.core.SourceView;
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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Semaphore;

/** Produces transient proposals only; existing draft CAS remains the sole Apply write. */
@RestController
class PipelineAssistantController {
    private static final String ERROR = "error";
    private static final String DATA_STATUS = "data-status";
    private static final String STATE = "state";
    private static final String STATUS = "status";
    private static final String FINISH = "finish";
    private static final String FINISH_REASON = "finishReason";
    private static final String VALID = "valid";
    private static final String VALIDATION = "validation";
    private static final String DIAGNOSTICS = "diagnostics";
    private static final String TRANSFORMS = "transforms";
    private static final String PRE_TRANSFORMS = "preTransforms";
    private static final String FIELDS = "fields";
    private static final String WIZARD = "wizard";
    private static final String NODE_ID = "nodeId";
    private static final String SOURCE_ID = "sourceId";
    private static final String ERROR_TEXT = "errorText";
    private static final String SOURCES = "sources";
    private static final String TABLES = "tables";
    private static final String PROPOSAL_ENABLED = "proposalEnabled";
    private static final int MAX_AUTO_SOURCES = 3;
    private static final int MAX_AUTO_TABLES = 40;
    private static final int MAX_AVAILABLE_SOURCES = 30;
    private static final int MAX_AVAILABLE_TABLES = 20;
    private static final int MAX_CONTEXT_LENGTH = 120000;
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
        var workspace = providers.workspace(request);
        String owner = workspace.owner();
        var settings = providers.service().require(owner);
        workspace.requireActive().run();
        long revision = revision(body.get("revision"));
        PipelineDraft base = current(id, revision);
        List<Map<String, String>> messages = messages(body.get("messages"));
        StreamingResponseBody stream = output -> {
            AssistantStream streamEvents = new AssistantStream(output);
            if (!CAPACITY.tryAcquire()) {
                streamEvents.event(Map.of("type", ERROR, ERROR_TEXT, ControlError.AI_OVERLOADED.code()));
                streamEvents.event(Map.of("type", DATA_STATUS, "data", Map.of(STATE, ERROR, STATUS, ERROR,
                        "code", ControlError.AI_OVERLOADED.code())));
                streamEvents.event(Map.of("type", FINISH, FINISH_REASON, ERROR));
                streamEvents.done();
                return;
            }
            String textId = UUID.randomUUID().toString();
            Thread worker = Thread.currentThread();
            var heartbeat = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(Thread.ofVirtual().factory());
            heartbeat.scheduleAtFixedRate(() -> {
                try { streamEvents.heartbeat(); }
                catch (UncheckedIOException disconnected) { worker.interrupt(); }
            }, 5, 5, java.util.concurrent.TimeUnit.SECONDS);
            try {
                workspace.requireActive().run();
                streamEvents.event(Map.of("type", "start", "messageId", UUID.randomUUID().toString()));
                streamEvents.status("connecting");
                Map<String, Object> context = context(base);
                Map<String, Object> focus = focus(base, body.get("focus"));
                if (!focus.isEmpty()) context.put("focus", focus);
                streamEvents.event(Map.of("type", "text-start", "id", textId));
                streamEvents.status("streaming");
                var candidate = client.propose(settings, messages, context, delta -> {
                    workspace.requireActive().run();
                    streamEvents.event(Map.of("type", "text-delta", "id", textId, "delta", delta));
                });
                workspace.requireActive().run();
                streamEvents.event(Map.of("type", "text-end", "id", textId));
                if (candidate.isEmpty()) {
                    streamEvents.status("complete");
                    streamEvents.event(Map.of("type", FINISH, FINISH_REASON, "stop"));
                    streamEvents.done();
                    return;
                }
                streamEvents.status("validating");
                Map<String, Object> validated = validate(base, id, revision, candidate.orElseThrow(), context);
                workspace.requireActive().run();
                if (!Boolean.TRUE.equals(validated.get(VALID))) {
                    List<Map<String, Object>> diagnostics = diagnostics(validated);
                    String code = diagnostics.isEmpty()
                            ? ControlError.AI_INVALID_PROPOSAL.code()
                            : AssistantWizardJson.text(diagnostics.getFirst().get("code"));
                    streamEvents.needsInput(code, diagnostics);
                    return;
                }
                Map<String, Object> proposal = new LinkedHashMap<>(validated);
                proposal.put("baseRevision", revision);
                workspace.requireActive().run();
                streamEvents.event(Map.of("type", "data-proposal", "data", proposal));
                streamEvents.status("ready");
                streamEvents.event(Map.of("type", FINISH, FINISH_REASON, "stop"));
                streamEvents.done();
            } catch (UncheckedIOException disconnected) {
                // Client cancellation closes the provider body through the streaming consumer.
            } catch (TapstateException failure) {
                streamEvents.failure(failure.code().code());
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
        var workspace = providers.workspace(request);
        Object raw = body.get("candidateWizard");
        if (!(raw instanceof Map<?, ?>)) throw malformed("candidateWizard must be an object");
        var result = validate(id, revision(body.get("revision")), AssistantWizardJson.object(raw));
        workspace.requireActive().run();
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(result);
    }
    Map<String, Object> validate(String id, long revision, Map<String, Object> view) {
        PipelineDraft base = current(id, revision);
        return validate(base, id, revision, view, null);
    }
    private Map<String, Object> validate(PipelineDraft base, String id, long revision,
            Map<String, Object> view, Map<String, Object> scopeContext) {
        try {
            if (!view.keySet().containsAll(Set.of("root", "relations", TRANSFORMS, "destinationTable", "destinationSourceId", "destinationWriteMode"))) {
                throw new IllegalArgumentException("candidateWizard is incomplete");
            }
            AssistantWizardJson.validateViewShape(view);
            Map<String, Object> before = wizard(base);
            Map<String, Object> wire = AssistantWizardJson.wire(view, before);
            enforceScope(base, wire, scopeContext == null ? context(base) : scopeContext);
            Map<String, Object> input = new LinkedHashMap<>(PipelineDraftJson.write(json, base));
            input.put(WIZARD, wire);
            PipelineDraft candidate = PipelineDraftJson.read(json, input, id, revision, base.updatedBy());
            String afterDsl = new CanonicalWriter().write(service().previewCandidate(id, candidate));
            String beforeDsl = previewBefore(id, revision);
            List<Map<String, Object>> changes = new ArrayList<>();
            diff(WIZARD, before, wizard(candidate), changes);
            current(id, revision);
            return Map.of(VALID, true, "candidateWizard", AssistantWizardJson.view(wizard(candidate)),
                    VALIDATION, Map.of(VALID, true, DIAGNOSTICS, List.of()),
                    "diff", Map.of("before", beforeDsl, "after", afterDsl, "changes", changes));
        } catch (IllegalArgumentException malformed) {
            return invalid(ControlError.AI_INVALID_PROPOSAL.code(), Map.of());
        } catch (TapstateException invalid) {
            if (invalid.code() == PipelineDraftError.REVISION_CONFLICT || invalid.code() == PipelineDraftError.NOT_FOUND) throw invalid;
            TapstateException diagnostic = nestedDiagnostic(invalid);
            return invalid(diagnostic.code().code(), safeDiagnosticParams(diagnostic.args()));
        }
    }
    private String previewBefore(String id, long revision) {
        try {
            return new CanonicalWriter().write(service().preview(id, revision));
        } catch (TapstateException incompleteBase) {
            return "";
        }
    }
    static Map<String, Object> focus(PipelineDraft base, Object raw) {
        if (!(raw instanceof Map<?, ?> input)) return Map.of();
        String kind = AssistantWizardJson.text(input.get("kind"));
        if (!Set.of("root", "relation", TRANSFORMS).contains(kind)) return Map.of();
        String node = AssistantWizardJson.text(input.get(NODE_ID));
        if (node.length() > 200) return Map.of();
        if (kind.equals("root") && node.equals("root")) return Map.of("kind", kind, NODE_ID, node);
        if (kind.equals(TRANSFORMS) && node.equals("doc_process")) return Map.of("kind", kind, NODE_ID, node);
        if (kind.equals("relation") && base.wizard().related().stream().anyMatch(related -> related.id().equals(node))) {
            return Map.of("kind", kind, NODE_ID, node);
        }
        return Map.of("kind", kind);
    }
    private Map<String, Object> context(PipelineDraft base) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("currentWizard", AssistantWizardJson.view(wizard(base)));
        SelectedInputScope selection = selectedInputScope(base);
        boolean inputSelectionPending = !selection.rootTableSelected();
        result.put("inputSelectionPending", inputSelectionPending);
        Set<String> sourceIds = inputSources(selection);
        if (selection.sourceIds().isEmpty() && sourceIds.size() > MAX_AUTO_SOURCES) {
            requireSourceSelection(result, sourceIds);
            omitOversizedSchema(result, List.of());
            return result;
        }
        List<SourceSchemaInventory> discovered = discoverSchemas(sourceIds);
        long tableCount = discovered.stream().mapToLong(source -> source.schema().tables().size()).sum();
        long selectedTableCount = selection.tables().values().stream().mapToLong(Set::size).sum();
        if (selectedTableCount > MAX_AUTO_TABLES
                || (inputSelectionPending && tableCount > MAX_AUTO_TABLES)) {
            requireSourceSelection(result, discovered);
            omitOversizedSchema(result, discovered);
            return result;
        }
        boolean selectedOnly = tableCount > MAX_AUTO_TABLES;
        List<Map<String, Object>> inventory = sourceInventory(discovered, selection.tables(), selectedOnly);
        result.put(SOURCES, inventory);
        result.put(PROPOSAL_ENABLED, !inventory.isEmpty());
        if (JsonWriter.write(result).length() > MAX_CONTEXT_LENGTH && !selection.tables().isEmpty()) {
            inventory = sourceInventory(discovered, selection.tables(), true);
            result.put(SOURCES, inventory);
            result.put(PROPOSAL_ENABLED, !inventory.isEmpty());
        }
        omitOversizedSchema(result, discovered);
        return result;
    }

    private record SourceSchemaInventory(String sourceId, String connector, SchemaReport schema) { }

    private record SelectedInputScope(Set<String> sourceIds, Map<String, Set<String>> tables,
            boolean rootTableSelected) { }

    private List<SourceSchemaInventory> discoverSchemas(Set<String> sourceIds) {
        List<SourceSchemaInventory> discovered = new ArrayList<>();
        if (schemas.getIfAvailable() == null) return discovered;
        for (String sourceId : sourceIds) {
            var schema = findSchemaIfSourceExists(sourceId).orElse(null);
            if (schema == null || schema.tables().isEmpty()) continue;
            String connector = schema.connectorId().toLowerCase(java.util.Locale.ROOT);
            if (!connector.contains("postgres") && !connector.contains("mysql")) continue;
            discovered.add(new SourceSchemaInventory(sourceId, schema.connectorId(), schema));
        }
        return discovered;
    }

    private static List<Map<String, Object>> sourceInventory(List<SourceSchemaInventory> discovered,
            Map<String, Set<String>> selectedTables, boolean selectedOnly) {
        List<Map<String, Object>> inventory = new ArrayList<>();
        for (SourceSchemaInventory source : discovered) {
            List<SchemaReport.Table> tables = source.schema().tables();
            if (selectedOnly) {
                Set<String> selected = selectedTables.getOrDefault(source.sourceId(), Set.of());
                tables = tables.stream().filter(table -> selected.contains(table.name())).toList();
            }
            if (tables.isEmpty()) continue;
            List<Map<String, Object>> tableInventory = tables.stream().map(table -> Map.<String, Object>of(
                    "name", table.name(), "primaryKey", table.primaryKey(), "fields", table.fields().stream()
                            .map(field -> Map.of("name", field.name(), "type", field.type() == null ? "unknown" : field.type())).toList())).toList();
            inventory.add(Map.of(SOURCE_ID, source.sourceId(), "connector", source.connector(), TABLES, tableInventory));
        }
        return inventory;
    }

    private static void requireSourceSelection(Map<String, Object> context, Set<String> sourceIds) {
        context.put("availableSources", sourceIds.stream().limit(MAX_AVAILABLE_SOURCES)
                .map(sourceId -> Map.of(SOURCE_ID, sourceId)).toList());
        disableProposalForScopeSelection(context);
    }

    private static void requireSourceSelection(Map<String, Object> context, List<SourceSchemaInventory> discovered) {
        context.put("availableSources", discovered.stream().limit(MAX_AVAILABLE_SOURCES).map(source -> Map.of(
                SOURCE_ID, source.sourceId(), "tableCount", source.schema().tables().size(),
                TABLES, source.schema().tables().stream().limit(MAX_AVAILABLE_TABLES)
                        .map(SchemaReport.Table::name).toList())).toList());
        disableProposalForScopeSelection(context);
    }

    private static void disableProposalForScopeSelection(Map<String, Object> context) {
        context.put(SOURCES, List.of());
        context.put(PROPOSAL_ENABLED, false);
        context.put("scopeSelectionRequired", true);
    }

    private static void omitOversizedSchema(Map<String, Object> context, List<SourceSchemaInventory> discovered) {
        if (JsonWriter.write(context).length() <= MAX_CONTEXT_LENGTH) return;
        context.put(SOURCES, List.of());
        context.put(PROPOSAL_ENABLED, false);
        context.put("schemaUnavailable", "selected schema is too large to include");
        if (!context.containsKey("availableSources")) requireSourceSelection(context, discovered);
        if (JsonWriter.write(context).length() > MAX_CONTEXT_LENGTH) {
            throw malformed("pipeline context is too large for the assistant");
        }
    }
    private void enforceScope(PipelineDraft base, Map<String, Object> candidate, Map<String, Object> scopeContext) {
        if (!Boolean.TRUE.equals(scopeContext.get(PROPOSAL_ENABLED))) {
            throw malformed("select an input Source and table before proposing a pipeline for a large schema");
        }
        Map<String, Set<String>> allowedTables = sourceTables(scopeContext);
        List<Map<String, Object>> inputs = new ArrayList<>(); inputs.add(AssistantWizardJson.object(candidate.get("root")));
        inputs.addAll(AssistantWizardJson.list(candidate.get("related")).stream().map(AssistantWizardJson::object).toList());
        for (var input : inputs) {
            String source = AssistantWizardJson.text(input.get(SOURCE_ID));
            if (!allowedTables.containsKey(source)) throw malformed("candidate references an unselected Source");
            if (!allowedTables.get(source).contains(AssistantWizardJson.text(input.get("table")))) {
                throw malformed("candidate references an unknown table");
            }
        }
        var beforeOutput = AssistantWizardJson.object(AssistantWizardJson.object(wizard(base).get("output")).get("config"));
        var output = AssistantWizardJson.object(AssistantWizardJson.object(candidate.get("output")).get("config"));
        if (!AssistantWizardJson.text(beforeOutput.get(SOURCE_ID))
                .equals(AssistantWizardJson.text(output.get(SOURCE_ID)))) {
            throw malformed("candidate may not change the selected destination Source");
        }
        validateUnionInputs(base, candidate, allowedTables);
    }

    private static Map<String, Set<String>> sourceTables(Map<String, Object> context) {
        Map<String, Set<String>> result = new LinkedHashMap<>();
        for (Object rawSource : AssistantWizardJson.list(context.get(SOURCES))) {
            Map<String, Object> source = AssistantWizardJson.object(rawSource);
            String sourceId = AssistantWizardJson.text(source.get(SOURCE_ID));
            if (sourceId.isBlank()) continue;
            Set<String> tableNames = result.computeIfAbsent(sourceId, ignored -> new LinkedHashSet<>());
            for (Object rawTable : AssistantWizardJson.list(source.get(TABLES))) {
                String tableName = AssistantWizardJson.text(AssistantWizardJson.object(rawTable).get("name"));
                if (!tableName.isBlank()) tableNames.add(tableName);
            }
        }
        return result;
    }

    private void validateUnionInputs(PipelineDraft base, Map<String, Object> candidate,
            Map<String, Set<String>> allowedTables) {
        List<Map<String, Object>> transforms = new ArrayList<>();
        Map<String, Object> root = AssistantWizardJson.object(candidate.get("root"));
        transforms.addAll(AssistantWizardJson.list(root.get(PRE_TRANSFORMS)).stream()
                .map(AssistantWizardJson::object).toList());
        for (Object rawRelation : AssistantWizardJson.list(candidate.get("related"))) {
            Map<String, Object> relation = AssistantWizardJson.object(rawRelation);
            transforms.addAll(AssistantWizardJson.list(relation.get(PRE_TRANSFORMS)).stream()
                    .map(AssistantWizardJson::object).toList());
        }
        transforms.addAll(AssistantWizardJson.list(candidate.get(TRANSFORMS)).stream()
                .map(AssistantWizardJson::object).toList());
        for (Map<String, Object> transform : transforms) {
            if (!"union".equals(transform.get("type"))) continue;
            Map<String, Object> fields = AssistantWizardJson.object(transform.get(FIELDS));
            Object rawInputs = fields.get("from");
            if (!(rawInputs instanceof List<?> inputs) || inputs.isEmpty()) {
                throw invalidUnionDraft(base, "wizard union requires at least one additional input");
            }
            for (Object rawInput : inputs) {
                if (!(rawInput instanceof String input) || !input.equals(input.trim())) {
                    throw invalidUnionDraft(base,
                            "wizard union input must be a source-qualified table reference");
                }
                int separator = input.indexOf('.');
                if (separator < 1 || separator == input.length() - 1
                        || input.substring(0, separator).isBlank() || input.substring(separator + 1).isBlank()) {
                    throw invalidUnionDraft(base,
                            "wizard union input must be a source-qualified table reference");
                }
                String sourceId = input.substring(0, separator);
                String tableName = input.substring(separator + 1);
                if (!allowedTables.containsKey(sourceId)) {
                    throw invalidUnionDraft(base,
                            "wizard union input must reference a table in a selected Source schema");
                }
                if (!allowedTables.get(sourceId).contains(tableName)) {
                    throw invalidUnionDraft(base,
                            "wizard union input must reference a table in a selected Source schema");
                }
            }
        }
    }

    private static TapstateException invalidUnionDraft(PipelineDraft base, String reason) {
        return new TapstateException(PipelineDraftError.INVALID,
                Map.of("id", base.pipelineId(), "reason", reason), null);
    }
    private Set<String> inputSources(SelectedInputScope selection) {
        if (!selection.sourceIds().isEmpty()) return selection.sourceIds();
        Set<String> available = new LinkedHashSet<>();
        SourceProjectionService projection = sources.getIfAvailable();
        if (projection == null) return available;
        for (SourceView source : projection.list()) {
            String connector = source.connector().toLowerCase(java.util.Locale.ROOT);
            if (!connector.contains("postgres") && !connector.contains("mysql")) continue;
            available.add(source.id());
            if (available.size() == MAX_AVAILABLE_SOURCES) break;
        }
        return available;
    }
    private Optional<SchemaReport> findSchemaIfSourceExists(String sourceId) {
        try {
            return schemas.getObject().find(sourceId);
        } catch (TapstateException missing) {
            if (missing.code() == SourceError.NOT_FOUND) return Optional.empty();
            throw missing;
        }
    }
    private static SelectedInputScope selectedInputScope(PipelineDraft draft) {
        Set<String> sourceIds = new LinkedHashSet<>();
        Map<String, Set<String>> tables = new LinkedHashMap<>();
        boolean rootTableSelected = false;
        PipelineDraft.Root root = draft.wizard().root();
        if (root != null) {
            addSelectedInput(root.sourceId(), root.table(), sourceIds, tables);
            rootTableSelected = hasText(root.sourceId()) && hasText(root.table());
            addUnionTables(root.preTransforms(), sourceIds, tables);
        }
        for (var related : draft.wizard().related()) {
            addSelectedInput(related.sourceId(), related.table(), sourceIds, tables);
            addUnionTables(related.preTransforms(), sourceIds, tables);
        }
        addUnionTables(draft.wizard().transforms(), sourceIds, tables);
        return new SelectedInputScope(sourceIds, tables, rootTableSelected);
    }

    private static void addSelectedInput(String sourceId, String tableName,
            Set<String> sourceIds, Map<String, Set<String>> tables) {
        if (!hasText(sourceId)) return;
        sourceIds.add(sourceId);
        if (hasText(tableName)) tables.computeIfAbsent(sourceId, ignored -> new LinkedHashSet<>()).add(tableName);
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private static void addUnionTables(List<PipelineDraft.Transform> transforms,
            Set<String> sourceIds, Map<String, Set<String>> tables) {
        for (PipelineDraft.Transform transform : transforms) {
            if (!"union".equals(transform.type())) continue;
            Object rawInputs = transform.fields().get("from");
            if (!(rawInputs instanceof List<?> inputs)) continue;
            for (Object rawInput : inputs) {
                if (!(rawInput instanceof String input) || !input.equals(input.trim())) continue;
                int separator = input.indexOf('.');
                if (separator < 1 || separator == input.length() - 1) continue;
                String sourceId = input.substring(0, separator);
                String tableName = input.substring(separator + 1);
                if (!sourceId.isBlank() && !tableName.isBlank()) {
                    addSelectedInput(sourceId, tableName, sourceIds, tables);
                }
            }
        }
    }
    private PipelineDraft current(String id, long revision) {
        PipelineDraft draft = service().find(id).orElseThrow(() -> new TapstateException(PipelineDraftError.NOT_FOUND, Map.of("id", id), null));
        if (draft.revision() != revision) throw new TapstateException(PipelineDraftError.REVISION_CONFLICT, Map.of("id", id), null);
        if (draft.mode() != PipelineDraft.Mode.WIZARD) throw new TapstateException(PipelineDraftError.MODE_CONFLICT, Map.of("id", id), null);
        return draft;
    }
    private PipelineDraftService service() { return drafts.getObject(); }
    private Map<String, Object> wizard(PipelineDraft draft) { return AssistantWizardJson.object(PipelineDraftJson.write(json, draft).get(WIZARD)); }
    private static Map<String, Object> invalid(String code, Map<String, Object> params) {
        return Map.of(VALID, false, VALIDATION, Map.of(VALID, false,
                DIAGNOSTICS, List.of(Map.of("code", code, "params", params))), "diff", Map.of("changes", List.of()));
    }
    private static List<Map<String, Object>> diagnostics(Map<String, Object> validationResult) {
        Map<String, Object> validation = AssistantWizardJson.object(validationResult.get(VALIDATION));
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object raw : AssistantWizardJson.list(validation.get(DIAGNOSTICS))) {
            if (raw instanceof Map<?, ?>) result.add(AssistantWizardJson.object(raw));
        }
        return result;
    }
    private static TapstateException nestedDiagnostic(TapstateException failure) {
        Throwable cause = failure.getCause();
        while (cause != null) {
            if (cause instanceof TapstateException diagnostic) return diagnostic;
            cause = cause.getCause();
        }
        return failure;
    }
    private static Map<String, Object> safeDiagnosticParams(Map<String, Object> params) {
        Set<String> safeNames = Set.of("path", "field", "ref", "source", "table");
        Map<String, Object> safe = new LinkedHashMap<>();
        for (String name : safeNames) {
            Object value = params.get(name);
            if (value instanceof String text && !text.isBlank() && text.length() <= 256) safe.put(name, text);
        }
        Object reason = params.get("reason");
        if (reason instanceof String text && Set.of(
                "wizard union requires at least one additional input",
                "wizard union input must be a source-qualified table reference",
                "wizard union inputs must be unique",
                "wizard union input duplicates an existing pipeline input",
                "wizard union input must reference a table in a selected Source schema").contains(text)) {
            safe.put("reason", text);
        }
        return Map.copyOf(safe);
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
    private static final class AssistantStream {
        private final OutputStream output;
        private final Object lock = new Object();

        private AssistantStream(OutputStream output) {
            this.output = output;
        }

        private void status(String state) {
            event(Map.of("type", DATA_STATUS, "data", Map.of(STATE, state, STATUS, state)));
        }

        private void event(Map<String, Object> value) {
            write("data: " + JsonWriter.write(value) + "\n\n");
        }

        private void done() {
            write("data: [DONE]\n\n");
        }

        private void heartbeat() {
            write(": heartbeat\n\n");
        }

        private void needsInput(String code, List<Map<String, Object>> diagnostics) {
            event(Map.of("type", DATA_STATUS, "data", Map.of(STATE, "needs-input", STATUS, "needs-input",
                    "code", code, DIAGNOSTICS, diagnostics)));
            event(Map.of("type", FINISH, FINISH_REASON, "stop"));
            done();
        }

        private void failure(String code) {
            try {
                event(Map.of("type", DATA_STATUS, "data", Map.of(STATE, ERROR, STATUS, ERROR, "code", code)));
                event(Map.of("type", ERROR, ERROR_TEXT, code));
                event(Map.of("type", FINISH, FINISH_REASON, ERROR));
                done();
            } catch (UncheckedIOException disconnected) {
                // A canceled client needs no further terminal event.
            }
        }

        private void write(String frame) {
            try {
                synchronized (lock) {
                    output.write(frame.getBytes(StandardCharsets.UTF_8));
                    output.flush();
                }
            } catch (IOException error) {
                throw new UncheckedIOException(error);
            }
        }
    }
}
