package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.DdlPolicy;
import io.tapstate.core.model.Embed;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.ErrorPolicy;
import io.tapstate.core.model.FieldRule;
import io.tapstate.core.model.FromClause;
import io.tapstate.core.model.FromRef;
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
        if (!pipeline.sources().equals(sourceSummaries.stream().map(PipelineSourceSummary::id).toList())) {
            throw new IllegalArgumentException("source summaries must match declared pipeline source references");
        }
        return new PipelineView(
                pipeline.id(),
                pipeline.metadata(),
                sourceSummaries,
                pipeline.transforms(),
                pipeline.view(),
                pipeline.serve(),
                pipeline.settings(),
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
        return new PipelineResource(
                input.id(),
                input.metadata(),
                copyStrings(input.sources(), "sources"),
                transforms(input.transforms()),
                view(input.view()),
                serve(input.serve()),
                settings(input.settings()),
                copyJson(input.experimental()));
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
        Map<String, Object> options = copyJson(objectOrNull(step.get("options"), path + ".options"));
        if (use != null) {
            return Step.use(id, use, from, options);
        }
        String type = text(step.get("type"), path + ".type");
        TransformBody transform = body(type, payload, path);
        return Step.inline(id, from, transform, options,
                copyJson(objectOrNull(step.get("experimental"), path + ".experimental")));
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
                    requiredText(payload, "engine", path), requiredText(payload, "sql", path));
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
            } else if (value != null) {
                result.put(entry.getKey(), FieldRule.literal(copyJsonValue(value)));
            } else {
                throw malformed(path + "." + entry.getKey() + " cannot be null");
            }
        }
        return Collections.unmodifiableMap(result);
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
        FromClause from = fromClause(value(value, "from"), path + ".from");
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
            result.add(new SyncElement(
                    textOrNull(value.get("id"), path + ".id"),
                    requiredText(value, "source", path),
                    enumValue(value(value, "write_mode", "writeMode"), WriteMode.values(), WriteMode::yaml,
                            path + ".writeMode"),
                    rename(objectOrNull(value.get("rename"), path + ".rename")),
                    enumValue(value.get("ddl"), DdlPolicy.values(), DdlPolicy::yaml, path + ".ddl"),
                    copyJson(objectOrNull(value.get("options"), path + ".options"))));
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
            result.add(new PushElement(
                    textOrNull(value.get("id"), path + ".id"),
                    requiredText(value, "source", path),
                    textOrNull(value.get("topic"), path + ".topic"),
                    pushFormat(value.get("format"), path + ".format"),
                    copyJson(objectOrNull(value.get("options"), path + ".options"))));
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

    private static List<String> copyStrings(List<String> values, String path) {
        List<String> result = new ArrayList<>(values.size());
        for (int index = 0; index < values.size(); index++) {
            result.add(requiredString(values.get(index), path + "[" + index + "]"));
        }
        return List.copyOf(result);
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

    private static TapstateException malformed(String reason) {
        return new TapstateException(ControlError.MALFORMED_REQUEST, Map.of("reason", reason), null);
    }

    private static final class SetOf {
        private static final java.util.Set<String> STEP_META = java.util.Set.of(
                "id", "from", "type", "use", "options", "experimental", "body");

        private SetOf() {
        }
    }
}
