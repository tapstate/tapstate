package io.tapstate.control.core;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** JSON Schema document for control-operation parameters and results. */
public final class ControlApiSchema {

    private static final Map<String, SchemaRef> REFS = refs();
    private static final Map<String, Object> DEFINITIONS = definitions();

    private ControlApiSchema() {
    }

    /** Returns the request/result references for an MCP operation. */
    public static SchemaRef ref(String operationId) {
        SchemaRef ref = REFS.get(operationId);
        if (ref == null) {
            throw new IllegalArgumentException("no control schema for operation: " + operationId);
        }
        return ref;
    }

    /** Returns the complete deterministic Draft 2020-12 schema document. */
    public static Map<String, Object> document() {
        Map<String, Object> document = new LinkedHashMap<>();
        document.put("$schema", "https://json-schema.org/draft/2020-12/schema");
        document.put("$id", "https://tapstate.io/schema/control/v1");
        document.put("$defs", DEFINITIONS);
        return immutableMap(document);
    }

    /** Resolves a local definition reference for protocol adapters. */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> resolve(String ref) {
        String prefix = "#/$defs/";
        if (ref == null || !ref.startsWith(prefix)) {
            throw new IllegalArgumentException("unsupported control schema ref: " + ref);
        }
        Object definition = DEFINITIONS.get(ref.substring(prefix.length()));
        if (!(definition instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException("unknown control schema ref: " + ref);
        }
        return (Map<String, Object>) map;
    }

    private static Map<String, SchemaRef> refs() {
        Map<String, SchemaRef> refs = new LinkedHashMap<>();
        bind(refs, "system.version", "SystemVersion");
        bind(refs, "connector.list", "ConnectorList");
        bind(refs, "connector.get", "ConnectorGet");
        bind(refs, "source.list", "SourceList");
        bind(refs, "source.get", "SourceGet");
        bind(refs, "source.create", "SourceCreate");
        bind(refs, "source.draft", "SourceDraft");
        bind(refs, "connection.test", "ConnectionTest");
        bind(refs, "connection.test-result", "ConnectionTestResult");
        bind(refs, "connection.discover-schema", "ConnectionDiscoverSchema");
        bind(refs, "connection.schema", "ConnectionSchema");
        bind(refs, "artifact.validate", "ArtifactValidate");
        bind(refs, "artifact.apply", "ArtifactApply");
        bind(refs, "artifact.delete", "ArtifactDelete");
        bind(refs, "artifact.get", "ArtifactGet");
        bind(refs, "pipeline.start", "PipelineStart");
        bind(refs, "pipeline.stop", "PipelineStop");
        bind(refs, "pipeline.status", "PipelineStatus");
        bind(refs, "pipeline.metrics", "PipelineMetrics");
        bind(refs, "pipeline.snapshot", "PipelineSnapshot");
        bind(refs, "pipeline.logs", "PipelineLogs");
        bind(refs, "data-browser.collections", "DataBrowserCollections");
        bind(refs, "data-browser.find", "DataBrowserFind");
        bind(refs, "data-browser.stats", "DataBrowserStats");
        return immutableMap(refs);
    }

    private static void bind(Map<String, SchemaRef> refs, String operationId, String stem) {
        refs.put(operationId, new SchemaRef("#/$defs/" + stem + "Request", "#/$defs/" + stem + "Result"));
    }

    private static Map<String, Object> definitions() {
        Map<String, Object> defs = new LinkedHashMap<>();
        Map<String, Object> empty = object(List.of(), Map.of(), false);
        Map<String, Object> opaque = object(List.of(), Map.of(), true);
        Map<String, Object> id = string("Tapstate resource identifier");

        // Open rather than closed: the two reserved fields of the version answer are empty until what
        // fills them lands, and a document that typed them would have to describe an absent value.
        pair(defs, "SystemVersion", empty, object(
                List.of("version"),
                Map.of("version", string("Product release this server was built from")),
                true));
        pair(defs, "ConnectorList", empty, opaque);
        pair(defs, "ConnectorGet", object(List.of("id"), Map.of("id", id), false), opaque);
        pair(defs, "SourceList", empty, opaque);
        pair(defs, "SourceGet", object(List.of("id"), Map.of("id", id), false), opaque);

        Map<String, Object> sourceProperties = new LinkedHashMap<>();
        sourceProperties.put("id", id);
        sourceProperties.put("metadata", object(List.of(), Map.of(
                "labels", Map.of("type", "object", "additionalProperties", Map.of("type", "string")),
                "description", string("Free-text Source description")), false));
        sourceProperties.put("connector", string("Registered connector id"));
        sourceProperties.put("config", object(List.of(), Map.of(), true));
        sourceProperties.put("mode", enumString("snapshot", "cdc", "stream", "file", "api"));
        Map<String, Object> table = object(List.of("type"), Map.of(
                "type", enumString("literal", "regex", "spec"),
                "name", string("Literal table name"),
                "pattern", string("Regular expression selecting tables"),
                "filter", string("Row filter expression"),
                "pk", array(string("Primary-key field name")),
                "options", opaque), false);
        sourceProperties.put("tables", array(table));
        sourceProperties.put("options", opaque);
        sourceProperties.put("srs", object(List.of(), Map.of(
                "key", string("Shared Record Store key"),
                "retention", string("Shared Record Store retention duration"),
                "schemaEvolution", enumString("track", "ignore"),
                "queryable", Map.of("type", "boolean"),
                "enabled", Map.of("type", "boolean")), false));
        sourceProperties.put("experimental", opaque);
        sourceProperties.put("clearSecrets", array(string("Config secret field to clear")));
        Map<String, Object> sourceRequest =
                object(List.of("id", "connector", "config"), sourceProperties, false);
        pair(defs, "SourceCreate", sourceRequest, opaque);
        pair(defs, "SourceDraft", sourceRequest, object(
                List.of("yaml"),
                Map.of("yaml", string("Canonical tapstate/v1 Source YAML")),
                false));

        Map<String, Object> connectionProperties = new LinkedHashMap<>();
        connectionProperties.put("id", id);
        connectionProperties.put("connectorId", string("Connector id"));
        connectionProperties.put("settings", object(List.of(), Map.of(), true));
        Map<String, Object> connectionRequest = object(
                List.of("id", "connectorId", "settings"), connectionProperties, false);
        pair(defs, "ConnectionTest", connectionRequest, opaque);
        pair(defs, "ConnectionDiscoverSchema", connectionRequest, opaque);
        pair(defs, "ConnectionTestResult", object(List.of("id"), Map.of("id", id), false), opaque);
        pair(defs, "ConnectionSchema", object(List.of("id"), Map.of("id", id), false), opaque);

        // The precondition is declared here, not merely tolerated by the server: this object refuses
        // undeclared properties, so a field the schema omits is one a schema-checking caller is told never
        // to send. It stays out of the required list — a draft without one applies the way it always did.
        Map<String, Object> draft = object(
                List.of("content"),
                Map.of(
                        "source", string("Origin label for diagnostics"),
                        "content", string("tapstate/v1 YAML"),
                        "expectedContentHash",
                        string("Content hash of the stored version this draft edits, as returned by a read "
                                + "of it; omit to apply unconditionally")),
                false);
        Map<String, Object> artifactRequest = object(
                List.of("drafts"), Map.of("drafts", array(draft)), false);
        pair(defs, "ArtifactValidate", artifactRequest, opaque);
        pair(defs, "ArtifactApply", artifactRequest, opaque);

        // Both arguments are required. A delete carries an id and nothing else, so without the hash the
        // caller would be discarding a version it never read; making it optional is the same thing for any
        // caller that omits what it may omit.
        Map<String, Object> deleteRequest = object(
                List.of("id", "expectedContentHash"),
                Map.of(
                        "id", id,
                        "expectedContentHash",
                        string("Content hash of the version being removed, as returned by a read of it")),
                false);
        pair(defs, "ArtifactDelete", deleteRequest, opaque);

        // The read's result is spelled out rather than left opaque: a caller reads this schema to learn
        // that a hash comes back at all, and that hash is the only route to the precondition the removal
        // above demands. An opaque result would leave the two halves connected only by prose.
        Map<String, Object> artifactResult = object(
                List.of("id", "kind", "canonicalForm", "contentHash"),
                Map.of(
                        "id", id,
                        "kind", string("Resource kind: source, pipeline, transform, view or serve"),
                        "canonicalForm", string("Canonical tapstate/v1 YAML as held by the store"),
                        "contentHash",
                        string("Content hash of those canonical bytes; pass back as a precondition")),
                false);
        pair(defs, "ArtifactGet", object(List.of("id"), Map.of("id", id), false), artifactResult);

        Map<String, Object> pipelineId = object(List.of("id"), Map.of("id", id), false);
        pair(defs, "PipelineStart", pipelineId, opaque);
        pair(defs, "PipelineStop", pipelineId, opaque);
        pair(defs, "PipelineStatus", pipelineId, opaque);
        pair(defs, "PipelineMetrics", pipelineId, opaque);
        pair(defs, "PipelineSnapshot", pipelineId, opaque);
        Map<String, Object> logsRequest = object(
                List.of("id"),
                Map.of("id", id, "limit", integer(1, 200, "Maximum lines, capped by the server")),
                false);
        pair(defs, "PipelineLogs", logsRequest, opaque);

        Map<String, Object> sourceId = string("Declared Source whose own database is read");
        Map<String, Object> collection = string("Collection in that source's database");
        pair(defs, "DataBrowserCollections",
                object(List.of("sourceId"), Map.of("sourceId", sourceId), false),
                object(List.of("collections"),
                        Map.of("collections", array(collectionEntry())), false));
        pair(defs, "DataBrowserStats",
                object(List.of("sourceId", "collection"),
                        Map.of("sourceId", sourceId, "collection", collection), false),
                opaque);

        Map<String, Object> findProperties = new LinkedHashMap<>();
        findProperties.put("sourceId", sourceId);
        findProperties.put("collection", collection);
        findProperties.put("filter", filter());
        findProperties.put("sort", object(
                List.of("field", "dir"),
                Map.of("field", string(
                                "Field to order by. A nested one is reached by pathing to it. A field "
                                        + "whose own name holds a dot cannot be ordered by at all and "
                                        + "is refused rather than served in an order nobody applied."),
                        "dir", enumString("asc", "desc")),
                false));
        findProperties.put("limit", integer(1, DataBrowserService.MAX_LIMIT,
                "Rows to read, capped by the server. Absent reads " + DataBrowserService.DEFAULT_LIMIT
                        + ". This is a preview of the first rows, not a page: there is no way to ask "
                        + "for the next ones."));
        pair(defs, "DataBrowserFind",
                object(List.of("sourceId", "collection"), findProperties, false),
                opaque);
        return immutableMap(defs);
    }

    /**
     * One collection of a listing. Three of the four are answered only when something answered them,
     * and the schema is where a caller with no person behind it is told what their absence means —
     * read as an empty answer, an absent field list becomes "this collection has no fields", which is
     * a claim nobody made and which stops the caller from looking any further.
     */
    private static Map<String, Object> collectionEntry() {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("name", string("Collection name, as the connector reports it"));
        properties.put("kind", Map.of(
                "type", "string",
                "enum", List.of("view"),
                "description", "What this collection is, for the collections this workspace declares. "
                        + "Absent for a collection no declaration covers — the listing shows "
                        + "everything the database holds, and a database holds more than a workspace "
                        + "authored, so an absent kind means this one was not made here rather than "
                        + "that it is of some unknown class. Other kinds join this list as they "
                        + "arrive."));
        properties.put("fields", Map.of(
                "type", "array",
                "items", Map.of("type", "string"),
                "description", "The collection's top-level fields, array fields included. Absent when "
                        + "nothing has been discovered on this source's connection, or when the latest "
                        + "discovery never named this collection — which is not the same as the "
                        + "collection having no fields. To learn the shape anyway, read the "
                        + "collection's first page and look at a row."));
        properties.put("description", string(
                "What whoever declared this collection wrote about it. Absent for a collection no view "
                        + "declares, which is most of them — a database holds far more than a workspace "
                        + "authored."));
        return object(List.of("name"), properties, false);
    }

    /**
     * A read's filter: one term, or one combination of them. Written out rather than referenced so the
     * document stands alone wherever a protocol adapter hands it over — a local reference would dangle
     * once the surrounding definitions are left behind.
     *
     * <p>The nesting stops where the vocabulary stops: a conjunction holds terms and alternatives, an
     * alternative holds terms, and there is no third level to write.
     */
    private static Map<String, Object> filter() {
        Map<String, Object> term = term();
        Map<String, Object> alternative = object(
                List.of("any"), Map.of("any", array(term)), false);
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.putAll(termProperties());
        properties.put("all", array(oneOf(term, alternative)));
        properties.put("any", array(term));
        Map<String, Object> filter = new LinkedHashMap<>(object(List.of(), properties, false));
        filter.put("description", "Either one term (`field`, `op`, `value`) or one combination of them "
                + "(`all` or `any`), never both in the same object. This is the whole vocabulary; a "
                + "query document in the database's own language is refused rather than forwarded.");
        return immutableMap(filter);
    }

    private static Map<String, Object> term() {
        return object(List.of("field", "op", "value"), termProperties(), false);
    }

    private static Map<String, Object> termProperties() {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("field", string(
                "Field to test. A dot steps into a nested document, so a field whose own name holds "
                        + "one is named by escaping it: `price\\.usd`. Written here that is "
                        + "\"price\\\\.usd\", because a lone backslash is not an escape JSON allows."
                        + " Matching on such a field reads every row: an index is written the same way a "
                        + "path is, so a name holding a dot cannot have one."));
        properties.put("op", enumString(
                "eq", "ne", "gt", "gte", "lt", "lte", "in", "exists", "contains"));
        properties.put("value", Map.of("description",
                "What to test against. `in` takes a non-empty list, `exists` takes true or false, "
                        + "`contains` takes text; the rest take the value itself."));
        return properties;
    }

    private static Map<String, Object> oneOf(Map<String, Object> left, Map<String, Object> right) {
        return Map.of("oneOf", List.of(left, right));
    }

    private static void pair(
            Map<String, Object> defs, String stem, Map<String, Object> request, Map<String, Object> result) {
        defs.put(stem + "Request", request);
        defs.put(stem + "Result", result);
    }

    private static Map<String, Object> object(
            List<String> required, Map<String, Object> properties, boolean additionalProperties) {
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("type", "object");
        schema.put("properties", immutableMap(properties));
        schema.put("additionalProperties", additionalProperties);
        if (!required.isEmpty()) {
            schema.put("required", List.copyOf(required));
        }
        return immutableMap(schema);
    }

    private static Map<String, Object> string(String description) {
        return Map.of("type", "string", "minLength", 1, "description", description);
    }

    private static Map<String, Object> enumString(String... values) {
        return Map.of("type", "string", "enum", List.of(values));
    }

    private static Map<String, Object> array(Map<String, Object> items) {
        return Map.of("type", "array", "items", items);
    }

    private static Map<String, Object> integer(int minimum, int maximum, String description) {
        return Map.of("type", "integer", "minimum", minimum, "maximum", maximum, "description", description);
    }

    private static <K, V> Map<K, V> immutableMap(Map<K, V> source) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(source));
    }
}
