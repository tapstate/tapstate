package io.tapstate.control.restapi;

import io.tapstate.control.core.PipelineDraftCompiler;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Explicit translation between the Builder view and durable Wizard wire shapes. */
final class AssistantWizardJson {
    private static final String OUTPUT = "output";
    private static final String CONFIG = "config";
    private static final String SOURCE = "source";
    private static final String SOURCE_ID = "sourceId";
    private static final String TABLE = "table";
    private static final String PRIMARY_KEY = "primaryKey";
    private static final String TRANSFORMS = "transforms";
    private static final String PRE_TRANSFORMS = "preTransforms";
    private static final String FLATTEN = "flatten";
    private static final String SUB_ARRAY = "sub_array";
    private static final String ARRAY_KEY = "arrayKey";
    private static final String CHILD_FIELD = "childField";
    private static final String PARENT_KEY = "parentKey";
    private static final String PARENT_FIELD = "parentField";
    private static final String CONDITIONS = "conditions";
    private static final String CHILD_KEY = "childKey";
    private static final String FIELDS = "fields";
    private AssistantWizardJson() { }
    static void validateViewShape(Map<String, Object> view) {
        requireList(view, "relations");
        requireList(view, TRANSFORMS);
        Map<String, Object> root = requireObject(view.get("root"));
        requireList(root, "keys");
        requireList(root, "columns");
        validateTransforms(requireList(root, TRANSFORMS));
        validateRelations(requireList(view, "relations"), 0, new int[1]);
        validateTransforms(requireList(view, TRANSFORMS));
    }
    private static void validateRelations(List<?> relations, int depth, int[] count) {
        if (depth > PipelineDraftCompiler.MAX_WIZARD_RELATION_DEPTH && !relations.isEmpty()) {
            throw new IllegalArgumentException("relation nesting is too deep");
        }
        count[0] += relations.size();
        if (count[0] > PipelineDraftCompiler.MAX_WIZARD_RELATIONS) throw new IllegalArgumentException("too many relations");
        for (Object raw : relations) {
            Map<String, Object> relation = requireObject(raw);
            requireList(relation, "primaryKey");
            requireList(relation, ARRAY_KEY);
            if (relation.containsKey(CONDITIONS)) requireList(relation, CONDITIONS);
            validateTransforms(requireList(relation, TRANSFORMS));
            validateRelations(requireList(relation, "children"), depth + 1, count);
        }
    }
    private static void validateTransforms(List<?> transforms) {
        for (Object raw : transforms) {
            Map<String, Object> transform = requireObject(raw);
            if (!"map".equals(transform.get("type"))) continue;
            Map<String, Object> config = requireObject(transform.get(CONFIG));
            Map<String, Object> map = requireObject(config.get("map"));
            requireList(map, FIELDS);
        }
    }
    private static List<?> requireList(Map<String, Object> value, String field) {
        Object raw = value.get(field);
        if (!(raw instanceof List<?> list)) throw new IllegalArgumentException("expected array: " + field);
        return list;
    }
    @SuppressWarnings("unchecked") private static Map<String, Object> requireObject(Object raw) {
        if (!(raw instanceof Map<?, ?>)) throw new IllegalArgumentException("expected object");
        return (Map<String, Object>) raw;
    }
    static Map<String, Object> view(Map<String, Object> wire) {
        Map<String, Object> root = object(wire.get("root"));
        Map<String, Object> config = object(object(wire.get(OUTPUT)).get(CONFIG));
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("root", root.isEmpty() ? null : Map.of(SOURCE, text(root.get(SOURCE_ID)),
                TABLE, text(root.get(TABLE)), "keys", list(root.get("key")),
                PRIMARY_KEY, String.join(", ", list(root.get("key")).stream().map(Object::toString).toList()),
                "rowCount", "", "columns", List.of(), TRANSFORMS, readTransforms(root.get(PRE_TRANSFORMS))));
        List<Map<String, Object>> rows = list(wire.get("related")).stream().map(AssistantWizardJson::object).toList();
        value.put("relations", readRelated(rows, "root", text(root.get(TABLE)), 0));
        value.put(TRANSFORMS, readTransforms(wire.get(TRANSFORMS)));
        value.put("destinationSourceId", text(config.get(SOURCE_ID)));
        value.put("destinationTable", text(config.get(TABLE)));
        value.put("destinationWriteMode", text(config.getOrDefault("writeMode", "upsert")));
        return value;
    }
    private static List<Map<String, Object>> readRelated(List<Map<String, Object>> rows, String parent, String table, int depth) {
        if (depth > PipelineDraftCompiler.MAX_WIZARD_RELATION_DEPTH
                && rows.stream().anyMatch(row -> parent.equals(row.get("parentId")))) {
            throw new IllegalArgumentException("relation nesting is too deep");
        }
        if (rows.size() > PipelineDraftCompiler.MAX_WIZARD_RELATIONS) throw new IllegalArgumentException("too many relations");
        List<Map<String, Object>> result = new ArrayList<>();
        for (var row : rows) {
            if (!parent.equals(row.get("parentId"))) continue;
            Map<String, Object> relation = object(row.get("relation"));
            List<Object> on = list(relation.get("on"));
            Map<String, Object> first = on.isEmpty() ? Map.of() : object(on.getFirst());
            Map<String, Object> node = new LinkedHashMap<>();
            node.put("id", row.get("id")); node.put(SOURCE, row.get(SOURCE_ID)); node.put(TABLE, row.get(TABLE));
            node.put("type", switch (text(relation.get("shape"))) { case "flat" -> FLATTEN; case "array" -> SUB_ARRAY; default -> "sub_doc"; });
            node.put(PRIMARY_KEY, list(relation.get("key"))); node.put(ARRAY_KEY, list(relation.get(ARRAY_KEY)));
            node.put("targetField", text(relation.get("path")));
            node.put("key", text(first.get(CHILD_FIELD))); node.put(PARENT_KEY, text(first.get(PARENT_FIELD)));
            node.put("parentTable", table); node.put("preProcessing", List.of());
            node.put(CONDITIONS, on.stream().map(AssistantWizardJson::object).map(pair -> Map.of(
                    CHILD_KEY, text(pair.get(CHILD_FIELD)), PARENT_KEY, text(pair.get(PARENT_FIELD)), "parentTable", table, "operator", "=")).toList());
            node.put(TRANSFORMS, readTransforms(row.get(PRE_TRANSFORMS)));
            node.put("children", readRelated(rows, text(row.get("id")), text(row.get(TABLE)) + " · " + row.get("id"), depth + 1));
            result.add(node);
        }
        return result;
    }
    static Map<String, Object> wire(Map<String, Object> view, Map<String, Object> original) {
        Map<String, Object> root = object(view.get("root"));
        List<Map<String, Object>> related = new ArrayList<>();
        writeRelated(list(view.get("relations")), "root", related, 0);
        Map<String, Object> oldOutput = object(original.get(OUTPUT));
        Map<String, Object> output = new LinkedHashMap<>(object(oldOutput.get(CONFIG)));
        output.put(SOURCE_ID, text(view.get("destinationSourceId"))); output.put(TABLE, text(view.get("destinationTable")));
        output.put("inputTable", text(root.get(TABLE))); output.put("writeMode", text(view.get("destinationWriteMode")));
        return Map.of("root", Map.of("id", "root", SOURCE_ID, text(root.get(SOURCE)),
                TABLE, text(root.get(TABLE)), "key", list(root.get("keys")), PRE_TRANSFORMS, writeTransforms(root.get(TRANSFORMS))),
                "related", related, TRANSFORMS, writeTransforms(view.get(TRANSFORMS)),
                OUTPUT, Map.of("kind", oldOutput.getOrDefault("kind", "atlas"), CONFIG, output));
    }
    private static void writeRelated(List<Object> nodes, String parent, List<Map<String, Object>> rows, int depth) {
        if (depth > PipelineDraftCompiler.MAX_WIZARD_RELATION_DEPTH && !nodes.isEmpty()) {
            throw new IllegalArgumentException("relation nesting is too deep");
        }
        for (Object raw : nodes) writeRelatedNode(object(raw), parent, rows, depth);
    }
    private static void writeRelatedNode(Map<String, Object> node, String parent, List<Map<String, Object>> rows, int depth) {
        if (rows.size() >= PipelineDraftCompiler.MAX_WIZARD_RELATIONS) throw new IllegalArgumentException("too many relations");
        String type = text(node.get("type"));
        if (!List.of(FLATTEN, SUB_ARRAY, "sub_doc").contains(type)) throw new IllegalArgumentException("invalid relation kind");
        String id = candidateId(node.get("id"));
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", id);
        row.put("parentId", parent);
        row.put(SOURCE_ID, text(node.get(SOURCE)));
        row.put(TABLE, text(node.get(TABLE)));
        row.put("relation", relation(node, type));
        row.put(PRE_TRANSFORMS, writeTransforms(node.get(TRANSFORMS)));
        rows.add(row);
        writeRelated(list(node.get("children")), id, rows, depth + 1);
    }
    private static Map<String, Object> relation(Map<String, Object> node, String type) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("shape", relationShape(type));
        value.put("path", type.equals(FLATTEN) ? null : text(node.get("targetField")));
        value.put("key", list(node.get(PRIMARY_KEY)));
        value.put(ARRAY_KEY, type.equals(SUB_ARRAY) ? list(node.get(ARRAY_KEY)) : List.of());
        value.put("on", relationConditions(node).stream().map(AssistantWizardJson::object)
                .map(condition -> Map.of(CHILD_FIELD, text(condition.get(CHILD_KEY)),
                        PARENT_FIELD, text(condition.get(PARENT_KEY)))).toList());
        return value;
    }
    private static String relationShape(String type) {
        return switch (type) {
            case FLATTEN -> "flat";
            case SUB_ARRAY -> "array";
            default -> "object";
        };
    }
    private static List<Object> relationConditions(Map<String, Object> node) {
        if (node.containsKey(CONDITIONS)) return list(node.get(CONDITIONS));
        return List.of(Map.of(CHILD_KEY, text(node.get("key")), PARENT_KEY, text(node.get(PARENT_KEY))));
    }
    static List<Map<String, Object>> readTransforms(Object raw) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object value : list(raw)) result.add(readTransform(object(value)));
        return result;
    }
    private static Map<String, Object> readTransform(Map<String, Object> step) {
        String type = text(step.get("type"));
        Map<String, Object> fields = object(step.get(FIELDS));
        Map<String, Object> config = "map".equals(type)
                ? Map.of(FIELDS, readMapRules(step.get("id"), fields), "passthroughUnlisted", true) : fields;
        return Map.of("id", text(step.get("id")), "name", type, "type", type, "enabled", true,
                CONFIG, Map.of(type, config));
    }
    private static List<Map<String, Object>> readMapRules(Object id, Map<String, Object> fields) {
        List<Map<String, Object>> rules = new ArrayList<>();
        int index = 0;
        for (var entry : fields.entrySet()) {
            Object value = entry.getValue();
            String text = value instanceof String string ? string : "";
            rules.add(Map.of("id", text(id) + "-" + index++, "outputField", entry.getKey(),
                    "ruleType", readMapRuleType(value, text), "value", readMapRuleValue(value, text)));
        }
        return rules;
    }
    private static String readMapRuleType(Object value, String text) {
        if (Boolean.FALSE.equals(value)) return "delete";
        if (text.startsWith("$")) return "rename";
        if (text.startsWith("=")) return "compute";
        return "literal";
    }
    private static String readMapRuleValue(Object value, String text) {
        return text.startsWith("$") || text.startsWith("=") ? text : JsonWriter.write(value);
    }
    static List<Map<String, Object>> writeTransforms(Object raw) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object value : list(raw)) result.add(writeTransform(object(value)));
        return result;
    }
    private static Map<String, Object> writeTransform(Map<String, Object> step) {
        String type = text(step.get("type"));
        if (!List.of("filter", "map", "unwind", "js").contains(type)) throw new IllegalArgumentException("unsupported transform kind");
        Map<String, Object> config = object(object(step.get(CONFIG)).get(type));
        return Map.of("id", candidateId(step.get("id")), "type", type, FIELDS, writeTransformFields(type, config));
    }
    private static String candidateId(Object value) {
        String id = text(value);
        return id.isBlank() ? UUID.randomUUID().toString() : id;
    }
    private static Map<String, Object> writeTransformFields(String type, Map<String, Object> config) {
        return switch (type) {
            case "map" -> writeMapFields(config);
            case "filter" -> Map.of("expr", text(config.get("expr")));
            case "js" -> Map.of("script", text(config.get("script")));
            default -> new LinkedHashMap<>(config);
        };
    }
    private static Map<String, Object> writeMapFields(Map<String, Object> config) {
        Map<String, Object> fields = new LinkedHashMap<>();
        for (Object rawRule : list(config.get(FIELDS))) {
            Map<String, Object> rule = object(rawRule);
            fields.put(text(rule.get("outputField")), writeMapValue(rule));
        }
        return fields;
    }
    private static Object writeMapValue(Map<String, Object> rule) {
        String value = text(rule.get("value"));
        return switch (text(rule.get("ruleType"))) {
            case "delete" -> false;
            case "rename" -> value.startsWith("$") ? value : "$" + value;
            case "compute" -> value.startsWith("=") ? value : "=" + value;
            case "literal" -> parseLiteral(value);
            default -> throw new IllegalArgumentException("invalid map field rule");
        };
    }
    private static Object parseLiteral(String value) {
        try {
            return JsonReader.parse(value);
        } catch (IllegalArgumentException ignored) {
            return value;
        }
    }
    @SuppressWarnings("unchecked") static Map<String, Object> object(Object value) {
        if (value == null) return Map.of();
        if (!(value instanceof Map<?, ?>)) throw new IllegalArgumentException("expected object");
        return (Map<String, Object>) value;
    }
    @SuppressWarnings("unchecked") static List<Object> list(Object value) {
        if (value == null) return List.of();
        if (!(value instanceof List<?>)) throw new IllegalArgumentException("expected list");
        return (List<Object>) value;
    }
    static String text(Object value) { return value instanceof String s ? s : ""; }
}
