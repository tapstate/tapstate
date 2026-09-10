package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.DdlPolicy;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.ErrorPolicy;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
import io.tapstate.core.model.JoinEngine;
import io.tapstate.core.model.NestOrder;
import io.tapstate.core.model.NestRoot;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.PushElement;
import io.tapstate.core.model.PushFormat;
import io.tapstate.core.model.QueryElement;
import io.tapstate.core.model.QueryType;
import io.tapstate.core.model.ReadMode;
import io.tapstate.core.model.RenameCase;
import io.tapstate.core.model.RenameSpec;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.ServeBlock;
import io.tapstate.core.model.Settings;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.Step;
import io.tapstate.core.model.Storage;
import io.tapstate.core.model.SyncElement;
import io.tapstate.core.model.TransformBody;
import io.tapstate.core.model.ViewBlock;
import io.tapstate.core.model.ViewSchema;
import io.tapstate.core.model.WriteMode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/** Projects a canonical Pipeline artifact and its resolved Sources into a structured view. */
public final class PipelineRepresentation {

    private final PipelineDagProjection dagProjection = new PipelineDagProjection();

    /** Builds a Pipeline view while preserving the declared Source reference order. */
    public PipelineView toView(
            PipelineResource pipeline, String contentHash, List<PipelineSourceSummary> sourceSummaries) {
        return toView(pipeline, contentHash, sourceSummaries, null);
    }

    /** Builds a Pipeline view and attaches the latest optional runtime observation. */
    public PipelineView toView(
            PipelineResource pipeline,
            String contentHash,
            List<PipelineSourceSummary> sourceSummaries,
            PipelineStatus status) {
        Objects.requireNonNull(pipeline, "pipeline");
        Objects.requireNonNull(contentHash, "contentHash");
        Objects.requireNonNull(sourceSummaries, "sourceSummaries");
        if (!pipeline.sources().stream().map(SourceRef::id).toList()
                .equals(sourceSummaries.stream().map(PipelineSourceSummary::id).toList())) {
            throw new IllegalArgumentException("source summaries must match declared pipeline source references");
        }
        return new PipelineView(
                pipeline.id(),
                pipeline.metadata(),
                sourceSummaries,
                transformViews(pipeline.transforms()),
                viewValue(pipeline.view()),
                serveValue(pipeline.serve()),
                settingsValue(pipeline.settings()),
                pipeline.experimental(),
                dagProjection.project(pipeline, sourceSummaries),
                contentHash,
                status);
    }

    /** Maps the structured editor payload back to the canonical Pipeline model. */
    public PipelineResource toModel(PipelineInput input, PipelineResource existing) {
        Objects.requireNonNull(input, "input");
        requireText(input.id(), "id");
        if (input.sources() == null) {
            throw malformed("sources must be provided");
        }
        try {
            return new PipelineResource(
                    input.id(),
                    input.metadata(),
                    sourceIds(input.sources()).stream()
                            .map(id -> (SourceRef) SourceRef.bare(id))
                            .toList(),
                    transforms(input.transforms()),
                    view(input.view()),
                    serve(input.serve()),
                    settings(input.settings()),
                    copyJson(input.experimental()));
        } catch (RuntimeException error) {
            if (error instanceof TapstateException diagnostic) {
                throw diagnostic;
            }
            throw malformed(error.getMessage());
        }
    }

    private static List<String> sourceIds(List<Object> values) {
        List<String> ids = new ArrayList<>(values.size());
        for (int index = 0; index < values.size(); index++) {
            Object value = values.get(index);
            if (value instanceof PipelineSourceSummary summary) {
                ids.add(summary.id());
            } else if (value instanceof Map<?, ?>) {
                ids.add(requiredText(object(value, "sources[" + index + "]"), "id", "sources[" + index + "]"));
            } else {
                ids.add(requiredString(value, "sources[" + index + "]"));
            }
        }
        return List.copyOf(ids);
    }

    private static List<Step> transforms(List<Map<String, Object>> values) {
        if (values == null) {
            return null;
        }
        List<Step> result = new ArrayList<>(values.size());
        for (int index = 0; index < values.size(); index++) {
            result.add(transform(values.get(index), "transforms[" + index + "]"));
        }
        return List.copyOf(result);
    }

    private static Step transform(Map<String, Object> value, String path) {
        Map<String, Object> step = object(value, path);
        requireNoOptions(step, path);
        String id = text(step.get("id"), path + ".id");
        Map<String, Object> body = objectOrNull(step.get("body"), path + ".body");
        Map<String, Object> payload = new LinkedHashMap<>();
        if (body != null) {
            payload.putAll(body);
        }
        for (Map.Entry<String, Object> entry : step.entrySet()) {
            if (!SetOf.STEP_META.contains(entry.getKey())) {
                payload.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }
        String use = textOrNull(step.get("use"), path + ".use");
        FromClause from = fromClause(step.get("from"), path + ".from");
        if (use != null) {
            return Step.use(id, use, from);
        }
        String type = transformType(step.get("type"), body, path);
        TransformBody transform = body(type, payload, path);
        return Step.inline(id, from, transform,
                copyJson(objectOrNull(step.get("experimental"), path + ".experimental")));
    }

    private static String transformType(Object raw, Map<String, Object> body, String path) {
        if (raw != null) {
            return text(raw, path + ".type");
        }
        if (body == null) {
            throw malformed(path + ".type is required");
        }
        if (body.containsKey("script")) {
            return "js";
        }
        if (body.containsKey("fields")) {
            return "map";
        }
        if (body.containsKey("expr")) {
            return "filter";
        }
        if (body.containsKey("root")) {
            return "nest";
        }
        if (body.containsKey("sql") || body.containsKey("engine")) {
            return "join";
        }
        if (body.isEmpty()) {
            return "union";
        }
        throw malformed(path + ".type is required");
    }

    private static TransformBody body(String type, Map<String, Object> payload, String path) {
        return switch (type) {
            case "js" -> new TransformBody.Js(requiredText(payload, "script", path));
            case "map" -> new TransformBody.MapProjection(
                    fieldRules(requiredObject(payload, "fields", path), path + ".fields"));
            case "filter" -> new TransformBody.Filter(requiredText(payload, "expr", path));
            case "union" -> new TransformBody.Union();
            case "nest" -> new TransformBody.Nest(
                    textOrNull(value(payload, "primary_key", "primaryKey"), path + ".primary_key"),
                    enumValue(value(payload, "order"), NestOrder.values(), NestOrder::yaml, path + ".order"),
                    integerOrNull(value(payload, "entries_in_memory", "entriesInMemory"), path + ".entries_in_memory"),
                    integerOrNull(value(payload, "max_elements_per_document", "maxElementsPerDocument"),
                            path + ".max_elements_per_document"),
                    nestRoot(requiredObject(payload, "root", path), path + ".root"));
            case "join" -> new TransformBody.Join(
                    enumValue(value(payload, "engine"), JoinEngine.values(), JoinEngine::yaml, path + ".engine"),
                    requiredText(payload, "sql", path));
            default -> throw malformed(path + ".type has unsupported transform type " + type);
        };
    }

    private static Map<String, FieldRule> fieldRules(Map<String, Object> values, String path) {
        Map<String, FieldRule> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            Object value = entry.getValue();
            if (Boolean.FALSE.equals(value)) {
                result.put(entry.getKey(), FieldRule.drop());
            } else if (value instanceof String string && string.startsWith("$")) {
                result.put(entry.getKey(), FieldRule.rename(string.substring(1)));
            } else if (value instanceof String string && string.startsWith("=")) {
                result.put(entry.getKey(), FieldRule.computed(string.substring(1)));
            } else if (value instanceof Map<?, ?> map) {
                Map<String, Object> typed = object(map, path + "." + entry.getKey());
                if (typed.isEmpty()) {
                    result.put(entry.getKey(), FieldRule.drop());
                } else if (typed.size() == 1 && typed.containsKey("sourceField")) {
                    result.put(entry.getKey(), FieldRule.rename(
                            requiredString(typed.get("sourceField"), path + "." + entry.getKey())));
                } else if (typed.size() == 1 && typed.containsKey("celExpr")) {
                    result.put(entry.getKey(), FieldRule.computed(
                            requiredString(typed.get("celExpr"), path + "." + entry.getKey())));
                } else if (typed.size() == 1 && typed.containsKey("value")) {
                    result.put(entry.getKey(), FieldRule.literal(copyJsonValue(typed.get("value"))));
                } else {
                    result.put(entry.getKey(), FieldRule.literal(copyJsonValue(value)));
                }
            } else if (value != null) {
                result.put(entry.getKey(), FieldRule.literal(copyJsonValue(value)));
            } else {
                throw malformed(path + "." + entry.getKey() + " cannot be null");
            }
        }
        return Collections.unmodifiableMap(result);
    }

    private static List<Map<String, Object>> transformViews(List<Step> steps) {
        if (steps == null) {
            return null;
        }
        return steps.stream().map(PipelineRepresentation::transformView).toList();
    }

    private static Map<String, Object> transformView(Step step) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("id", step.id());
        value.put("from", fromValue(step.from()));
        if (step instanceof Step.Use use) {
            value.put("use", use.use());
        } else if (step instanceof Step.Inline inline) {
            value.put("type", inline.body().type());
            value.putAll(bodyValue(inline.body()));
            value.put("experimental", copyJson(inline.experimental()));
        }
        return Collections.unmodifiableMap(value);
    }

    private static Map<String, Object> bodyValue(TransformBody body) {
        Map<String, Object> value = new LinkedHashMap<>();
        switch (body) {
            case TransformBody.Js js -> value.put("script", js.script());
            case TransformBody.MapProjection map -> value.put("fields", fieldRuleValues(map.fields()));
            case TransformBody.Filter filter -> value.put("expr", filter.expr());
            // Every key, including the ones the author omitted: this face reports what a pipeline
            // is, and a key left out of the answer is indistinguishable from a key this face does
            // not know about - which is the reading a reader of an unfamiliar type arrives with.
            case TransformBody.Unwind unwind -> {
                value.put("path", unwind.path());
                value.put("includeArrayIndex", unwind.includeArrayIndex());
                value.put("preserveNullAndEmptyArrays", unwind.preserveNullAndEmptyArrays());
                value.put("elementKey", unwind.elementKey());
                value.put("elementType", unwind.elementType());
            }
            case TransformBody.Union ignored -> {
            }
            case TransformBody.Nest nest -> {
                value.put("primaryKey", nest.primaryKey());
                value.put("order", nest.order() == null ? null : nest.order().name());
                value.put("entriesInMemory", nest.entriesInMemory());
                value.put("maxElementsPerDocument", nest.maxElementsPerDocument());
                value.put("root", nestRootValue(nest.root()));
            }
            case TransformBody.Join join -> {
                value.put("engine", join.engine().name());
                value.put("sql", join.sql());
            }
        }
        return value;
    }

    private static Map<String, Object> nestRootValue(NestRoot root) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("from", root.from());
        value.put("key", root.key());
        value.put("mode", root.mode());
        value.put("trackKeyChanges", root.trackKeyChanges());
        value.put("embed", embedValues(root.embed()));
        return Collections.unmodifiableMap(value);
    }

    private static List<Map<String, Object>> embedValues(List<Embed> embeds) {
        if (embeds == null) {
            return null;
        }
        return embeds.stream().map(embed -> {
            Map<String, Object> value = new LinkedHashMap<>();
            value.put("from", embed.from());
            value.put("on", embed.on());
            value.put("as", embed.as() == null ? null : embed.as().name());
            value.put("path", embed.path());
            value.put("arrayKey", embed.arrayKey());
            value.put("ignoreUpdates", embed.ignoreUpdates());
            value.put("trackKeyChanges", embed.trackKeyChanges());
            value.put("embed", embedValues(embed.embed()));
            return Collections.unmodifiableMap(value);
        }).toList();
    }

    private static Map<String, Object> fieldRuleValues(Map<String, FieldRule> rules) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (Map.Entry<String, FieldRule> entry : rules.entrySet()) {
            values.put(entry.getKey(), switch (entry.getValue()) {
                case FieldRule.Drop ignored -> false;
                case FieldRule.Rename rename -> "$" + rename.sourceField();
                case FieldRule.Computed computed -> "=" + computed.celExpr();
                case FieldRule.Literal literal -> copyJsonValue(literal.value());
            });
        }
        return Collections.unmodifiableMap(values);
    }

    private static Object fromValue(FromClause from) {
        return switch (from) {
            case FromClause.Flow flow -> flow.refs().stream().map(PipelineRepresentation::fromRefValue).toList();
            case FromClause.Aliases aliases -> {
                Map<String, Object> values = new LinkedHashMap<>();
                aliases.aliases().forEach((alias, ref) -> values.put(alias, fromRefValue(ref)));
                yield Collections.unmodifiableMap(values);
            }
        };
    }

    private static String fromRefValue(FromRef ref) {
        return switch (ref) {
            case FromRef.Literal literal -> literal.ref();
            case FromRef.Regex regex -> "/" + regex.pattern() + "/";
        };
    }

    private static Map<String, Object> viewValue(ViewBlock view) {
        if (view == null) {
            return null;
        }
        Map<String, Object> value = new LinkedHashMap<>();
        if (view instanceof ViewBlock.Use use) {
            value.put("id", use.id());
            value.put("from", fromRefValue(use.from()));
            value.put("use", use.use());
        } else if (view instanceof ViewBlock.Inline inline) {
            value.put("id", inline.id());
            value.put("from", fromRefValue(inline.from()));
            value.put("primaryKey", inline.primaryKey());
            value.put("storage", storageValue(inline.storage()));
            value.put("schema", viewSchemaValue(inline.schema()));
        }
        return Collections.unmodifiableMap(value);
    }

    private static Map<String, Object> storageValue(Storage storage) {
        if (storage == null) {
            return null;
        }
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("hot", storage.hot() == null ? null : Map.of("ttl", storage.hot().ttl()));
        if (storage.warm() == null) {
            value.put("warm", null);
        } else {
            Map<String, Object> warm = new LinkedHashMap<>();
            warm.put("collection", storage.warm().collection());
            warm.put("indexes", storage.warm().indexes());
            value.put("warm", Collections.unmodifiableMap(warm));
        }
        if (storage.cold() == null) {
            value.put("cold", null);
        } else {
            value.put("cold", Collections.singletonMap("partitionBy", storage.cold().partitionBy()));
        }
        return Collections.unmodifiableMap(value);
    }

    private static Map<String, Object> viewSchemaValue(ViewSchema schema) {
        if (schema == null) {
            return null;
        }
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("enforce", schema.enforce());
        value.put("evolution", schema.evolution());
        return Collections.unmodifiableMap(value);
    }

    private static Map<String, Object> serveValue(ServeBlock serve) {
        if (serve == null) {
            return null;
        }
        Map<String, Object> value = new LinkedHashMap<>();
        if (serve instanceof ServeBlock.Use use) {
            value.put("id", use.id());
            value.put("from", fromValue(use.from()));
            value.put("use", use.use());
        } else if (serve instanceof ServeBlock.Inline inline) {
            value.put("id", inline.id());
            value.put("from", fromValue(inline.from()));
            value.put("sync", syncValues(inline.sync()));
            value.put("query", queryValues(inline.query()));
            value.put("push", pushValues(inline.push()));
        }
        return Collections.unmodifiableMap(value);
    }

    private static List<Map<String, Object>> syncValues(List<SyncElement> elements) {
        if (elements == null) {
            return null;
        }
        return elements.stream().map(element -> {
            Map<String, Object> value = new LinkedHashMap<>();
            value.put("id", element.id());
            value.put("source", element.source());
            value.put("writeMode", element.writeMode() == null ? null : element.writeMode().name());
            value.put("rename", renameValue(element.rename()));
            value.put("ddl", element.ddl() == null ? null : element.ddl().name());
            return Collections.unmodifiableMap(value);
        }).toList();
    }

    private static Map<String, Object> renameValue(RenameSpec rename) {
        if (rename == null) {
            return null;
        }
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("map", rename.map());
        value.put("case", rename.caseMode() == null ? null : rename.caseMode().name());
        value.put("prefix", rename.prefix());
        value.put("suffix", rename.suffix());
        return Collections.unmodifiableMap(value);
    }

    private static List<Map<String, Object>> queryValues(List<QueryElement> elements) {
        if (elements == null) {
            return null;
        }
        return elements.stream().map(element -> {
            Map<String, Object> value = new LinkedHashMap<>();
            value.put("type", element.type().name());
            value.put("backend", element.backend());
            return Collections.unmodifiableMap(value);
        }).toList();
    }

    private static List<Map<String, Object>> pushValues(List<PushElement> elements) {
        if (elements == null) {
            return null;
        }
        return elements.stream().map(element -> {
            Map<String, Object> value = new LinkedHashMap<>();
            value.put("id", element.id());
            value.put("source", element.source());
            value.put("topic", element.topic());
            value.put("format", pushFormatValue(element.format()));
            return Collections.unmodifiableMap(value);
        }).toList();
    }

    private static Object pushFormatValue(PushFormat format) {
        return switch (format) {
            case null -> null;
            case PushFormat.Cel cel -> "=" + cel.expr();
            case PushFormat.Fields fields -> fieldRuleValues(fields.fields());
        };
    }

    private static Map<String, Object> settingsValue(Settings settings) {
        if (settings == null) {
            return null;
        }
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("errorPolicy", settings.errorPolicy() == null ? null : settings.errorPolicy().name());
        value.put("batchSize", settings.batchSize());
        value.put("parallelism", settings.parallelism());
        value.put("schedule", settings.schedule());
        value.put("readMode", settings.readMode() == null ? null : settings.readMode().name());
        value.put("startFrom", settings.startFrom());
        return Collections.unmodifiableMap(value);
    }

    private static NestRoot nestRoot(Map<String, Object> value, String path) {
        return new NestRoot(
                requiredText(value, "from", path),
                stringsOrNull(value(value, "key"), path + ".key"),
                textOrNull(value(value, "mode"), path + ".mode"),
                booleanOrNull(value(value, "trackKeyChanges", "track_key_changes"), path + ".trackKeyChanges"),
                embeds(listOrNull(value(value, "embed"), path + ".embed"), path + ".embed"));
    }

    private static List<Embed> embeds(List<?> values, String path) {
        if (values == null) {
            return null;
        }
        List<Embed> result = new ArrayList<>(values.size());
        for (int index = 0; index < values.size(); index++) {
            Map<String, Object> value = object(values.get(index), path + "[" + index + "]");
            result.add(new Embed(
                    requiredText(value, "from", path),
                    stringMap(requiredObject(value, "on", path), path + ".on"),
                    enumValue(value.get("as"), EmbedAs.values(), EmbedAs::yaml, path + ".as"),
                    requiredText(value, "path", path),
                    stringsOrNull(value(value, "arrayKey", "array_key"), path + ".arrayKey"),
                    booleanOrNull(value(value, "ignoreUpdates", "ignore_updates"), path + ".ignoreUpdates"),
                    booleanOrNull(value(value, "trackKeyChanges", "track_key_changes"), path + ".trackKeyChanges"),
                    embeds(listOrNull(value(value, "embed"), path + ".embed"), path + ".embed")));
        }
        return List.copyOf(result);
    }

    private static ViewBlock view(Map<String, Object> value) {
        if (value == null) {
            return null;
        }
        String path = "view";
        String use = textOrNull(value.get("use"), path + ".use");
        FromRef from = fromRef(value(value, "from"), path + ".from");
        String id = textOrNull(value.get("id"), path + ".id");
        if (use != null) {
            return new ViewBlock.Use(id, use, from);
        }
        return new ViewBlock.Inline(
                id == null ? "view" : id,
                from,
                textOrNull(value(value, "primary_key", "primaryKey"), path + ".primary_key"),
                storage(objectOrNull(value.get("storage"), path + ".storage")),
                viewSchema(objectOrNull(value.get("schema"), path + ".schema")));
    }

    private static ServeBlock serve(Map<String, Object> value) {
        if (value == null) {
            return null;
        }
        String path = "serve";
        String use = textOrNull(value.get("use"), path + ".use");
        FromClause from = flowFrom(value(value, "from"), path + ".from");
        String id = textOrNull(value.get("id"), path + ".id");
        if (use != null) {
            return new ServeBlock.Use(id, use, from);
        }
        return new ServeBlock.Inline(
                id == null ? "serve" : id,
                from,
                sync(value.get("sync"), path + ".sync"),
                queries(value.get("query"), path + ".query"),
                pushes(value.get("push"), path + ".push"));
    }

    private static List<SyncElement> sync(Object raw, String path) {
        List<?> values = listOrNull(raw, path);
        if (values == null) {
            return null;
        }
        List<SyncElement> result = new ArrayList<>(values.size());
        for (int index = 0; index < values.size(); index++) {
            Map<String, Object> value = object(values.get(index), path + "[" + index + "]");
            requireNoOptions(value, path + "[" + index + "]");
            result.add(new SyncElement(
                    textOrNull(value.get("id"), path + ".id"),
                    requiredText(value, "source", path),
                    enumValue(value(value, "write_mode", "writeMode"), WriteMode.values(), WriteMode::yaml,
                            path + ".writeMode"),
                    rename(objectOrNull(value.get("rename"), path + ".rename")),
                    enumValue(value.get("ddl"), DdlPolicy.values(), DdlPolicy::yaml, path + ".ddl")));
        }
        return List.copyOf(result);
    }

    private static List<QueryElement> queries(Object raw, String path) {
        List<?> values = listOrNull(raw, path);
        if (values == null) {
            return null;
        }
        List<QueryElement> result = new ArrayList<>(values.size());
        for (int index = 0; index < values.size(); index++) {
            Map<String, Object> value = object(values.get(index), path + "[" + index + "]");
            result.add(new QueryElement(
                    enumValue(value.get("type"), QueryType.values(), QueryType::yaml, path + ".type"),
                    textOrNull(value.get("backend"), path + ".backend")));
        }
        return List.copyOf(result);
    }

    private static List<PushElement> pushes(Object raw, String path) {
        List<?> values = listOrNull(raw, path);
        if (values == null) {
            return null;
        }
        List<PushElement> result = new ArrayList<>(values.size());
        for (int index = 0; index < values.size(); index++) {
            Map<String, Object> value = object(values.get(index), path + "[" + index + "]");
            requireNoOptions(value, path + "[" + index + "]");
            result.add(new PushElement(
                    textOrNull(value.get("id"), path + ".id"),
                    requiredText(value, "source", path),
                    textOrNull(value.get("topic"), path + ".topic"),
                    pushFormat(value.get("format"), path + ".format")));
        }
        return List.copyOf(result);
    }

    private static PushFormat pushFormat(Object raw, String path) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof String expression) {
            return PushFormat.cel(expression.startsWith("=") ? expression.substring(1) : expression);
        }
        return PushFormat.fields(fieldRules(object(raw, path), path));
    }

    private static RenameSpec rename(Map<String, Object> value) {
        if (value == null) {
            return null;
        }
        return new RenameSpec(
                stringMap(objectOrNull(value.get("map"), "rename.map"), "rename.map"),
                enumValue(value.get("case"), RenameCase.values(), RenameCase::yaml, "rename.case"),
                textOrNull(value.get("prefix"), "rename.prefix"),
                textOrNull(value.get("suffix"), "rename.suffix"));
    }

    private static Storage storage(Map<String, Object> value) {
        if (value == null) {
            return null;
        }
        Map<String, Object> hot = objectOrNull(value.get("hot"), "storage.hot");
        Map<String, Object> warm = objectOrNull(value.get("warm"), "storage.warm");
        Map<String, Object> cold = objectOrNull(value.get("cold"), "storage.cold");
        return new Storage(
                hot == null ? null : new Storage.Hot(requiredText(hot, "ttl", "storage.hot")),
                warm == null ? null : new Storage.Warm(
                        requiredText(warm, "collection", "storage.warm"),
                        stringsOrNull(warm.get("indexes"), "storage.warm.indexes")),
                cold == null ? null : new Storage.Cold(
                        stringsOrNull(value(cold, "partition_by", "partitionBy"), "storage.cold.partitionBy")));
    }

    private static ViewSchema viewSchema(Map<String, Object> value) {
        return value == null ? null : new ViewSchema(
                booleanOrNull(value.get("enforce"), "schema.enforce"),
                textOrNull(value.get("evolution"), "schema.evolution"));
    }

    private static Settings settings(Map<String, Object> value) {
        if (value == null) {
            return null;
        }
        return new Settings(
                enumValue(value(value, "error_policy", "errorPolicy"), ErrorPolicy.values(), ErrorPolicy::yaml,
                        "settings.errorPolicy"),
                integerOrNull(value(value, "batch_size", "batchSize"), "settings.batchSize"),
                integerOrNull(value.get("parallelism"), "settings.parallelism"),
                textOrNull(value.get("schedule"), "settings.schedule"),
                enumValue(value(value, "read_mode", "readMode"), ReadMode.values(), ReadMode::yaml,
                        "settings.readMode"),
                textOrNull(value(value, "start_from", "startFrom"), "settings.startFrom"));
    }

    private static FromClause fromClause(Object raw, String path) {
        if (raw instanceof String) {
            return FromClause.list(fromRef(raw, path));
        }
        if (raw instanceof List<?> list) {
            List<FromRef> refs = new ArrayList<>(list.size());
            for (int index = 0; index < list.size(); index++) {
                refs.add(fromRef(list.get(index), path + "[" + index + "]"));
            }
            return new FromClause.Flow(refs);
        }
        if (raw instanceof Map<?, ?>) {
            Map<String, Object> aliases = object(raw, path);
            // Jackson's representation of the typed FromClause.Flow is {"refs":[...]}; accept
            // that shape as well as the editor's compact string/list/alias-map form.
            if (aliases.size() == 1 && aliases.containsKey("refs")) {
                return fromClause(aliases.get("refs"), path + ".refs");
            }
            Map<String, FromRef> refs = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : aliases.entrySet()) {
                refs.put(entry.getKey(), fromRef(entry.getValue(), path + "." + entry.getKey()));
            }
            return FromClause.aliases(refs);
        }
        throw malformed(path + " must be a string, list, or alias map");
    }

    private static FromClause flowFrom(Object raw, String path) {
        FromClause from = fromClause(raw, path);
        if (from instanceof FromClause.Flow) {
            return from;
        }
        throw malformed(path + " must be a string or list");
    }

    private static FromRef fromRef(Object raw, String path) {
        if (raw instanceof Map<?, ?>) {
            Map<String, Object> value = object(raw, path);
            if (value.size() == 1 && value.containsKey("ref")) {
                return fromRef(value.get("ref"), path + ".ref");
            }
            if (value.size() == 1 && value.containsKey("pattern")) {
                String pattern = requiredString(value.get("pattern"), path + ".pattern");
                return FromRef.regex(pattern);
            }
        }
        String value = requiredString(raw, path);
        return value.length() >= 2 && value.startsWith("/") && value.endsWith("/")
                ? FromRef.regex(value.substring(1, value.length() - 1))
                : FromRef.literal(value);
    }

    private static Map<String, String> stringMap(Map<String, Object> value, String path) {
        if (value == null) {
            return null;
        }
        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            result.put(entry.getKey(), requiredString(entry.getValue(), path + "." + entry.getKey()));
        }
        return result;
    }

    private static List<String> stringsOrNull(Object raw, String path) {
        if (raw == null) {
            return null;
        }
        if (!(raw instanceof List<?> values)) {
            throw malformed(path + " must be a list of strings");
        }
        List<String> result = new ArrayList<>(values.size());
        for (int index = 0; index < values.size(); index++) {
            result.add(requiredString(values.get(index), path + "[" + index + "]"));
        }
        return List.copyOf(result);
    }

    private static List<?> listOrNull(Object value, String path) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof List<?> list)) {
            throw malformed(path + " must be a list");
        }
        return list;
    }

    private static Map<String, Object> requiredObject(Map<String, Object> value, String key, String path) {
        return object(value.get(key), path + "." + key);
    }

    private static Map<String, Object> objectOrNull(Object value, String path) {
        return value == null ? null : object(value, path);
    }

    private static Map<String, Object> object(Object value, String path) {
        if (!(value instanceof Map<?, ?> map)) {
            throw malformed(path + " must be an object");
        }
        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            if (!(entry.getKey() instanceof String key)) {
                throw malformed(path + " contains a non-string key");
            }
            result.put(key, entry.getValue());
        }
        return result;
    }

    private static Object value(Map<String, Object> map, String... keys) {
        for (String key : keys) {
            if (map.containsKey(key)) {
                return map.get(key);
            }
        }
        return null;
    }

    private static String requiredText(Map<String, Object> map, String key, String path) {
        return requiredString(map.get(key), path + "." + key);
    }

    private static String requiredString(Object value, String path) {
        String result = textOrNull(value, path);
        if (result == null || result.isBlank()) {
            throw malformed(path + " is required");
        }
        return result;
    }

    private static String text(Object value, String path) {
        return requiredString(value, path);
    }

    private static String textOrNull(Object value, String path) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof String result)) {
            throw malformed(path + " must be a string");
        }
        return result;
    }

    private static Integer integerOrNull(Object value, String path) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof Number number) || number.doubleValue() != number.intValue()) {
            throw malformed(path + " must be an integer");
        }
        return number.intValue();
    }

    private static Boolean booleanOrNull(Object value, String path) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof Boolean result)) {
            throw malformed(path + " must be a boolean");
        }
        return result;
    }

    private static <E> E enumValue(Object value, E[] candidates, Function<E, String> spelling, String path) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof String text)) {
            throw malformed(path + " must be a string");
        }
        for (E candidate : candidates) {
            if (spelling.apply(candidate).equals(text)
                    || ((Enum<?>) candidate).name().equalsIgnoreCase(text)) {
                return candidate;
            }
        }
        throw malformed(path + " has unsupported value " + text);
    }

    private static Map<String, Object> copyJson(Map<String, Object> value) {
        return SourceDraft.copyJsonMap(value, true);
    }

    private static Object copyJsonValue(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> result = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!(entry.getKey() instanceof String key)) {
                    throw malformed("JSON object contains a non-string key");
                }
                result.put(key, copyJsonValue(entry.getValue()));
            }
            return result;
        }
        if (value instanceof List<?> list) {
            return list.stream().map(PipelineRepresentation::copyJsonValue).toList();
        }
        return value;
    }

    private static void requireText(String value, String path) {
        requiredString(value, path);
    }

    /**
     * Options are the engine's own configuration and its vocabulary is empty today, so the model has
     * nowhere to put one. Refusing here rather than dropping it silently: a request that carries an
     * option and loses it on the way in reads as accepted and configures nothing. The source face
     * refuses the same key, and so does the authoring grammar; this face used to be the one that
     * took it and said nothing.
     */
    private static void requireNoOptions(Map<String, Object> value, String path) {
        Object options = value.get("options");
        if (options == null || (options instanceof Map<?, ?> map && map.isEmpty())) {
            return;
        }
        throw malformed(path + ".options carries no engine option today; remove the field");
    }

    private static TapstateException malformed(String reason) {
        String detail = reason == null || reason.isBlank() ? "invalid pipeline payload" : reason;
        return new TapstateException(ControlError.MALFORMED_REQUEST, Map.of("reason", detail), null);
    }

    private static final class SetOf {
        /**
         * The keys a step carries in its own right. Everything else on a step is transform payload,
         * so a key dropped from here is not removed - it is re-read as payload. "options" stays for
         * that reason: the refusal above lets an empty one through, and without this entry that
         * empty map would arrive in the transform body.
         */
        private static final java.util.Set<String> STEP_META = java.util.Set.of(
                "id", "from", "type", "use", "options", "experimental", "body");

        private SetOf() {
        }
    }
}
