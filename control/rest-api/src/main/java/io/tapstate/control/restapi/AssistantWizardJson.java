package io.tapstate.control.restapi;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Explicit translation between the Builder view and durable Wizard wire shapes. */
final class AssistantWizardJson {
    private AssistantWizardJson() { }
    static Map<String, Object> view(Map<String, Object> wire) {
        Map<String, Object> root = object(wire.get("root"));
        Map<String, Object> config = object(object(wire.get("output")).get("config"));
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("root", root.isEmpty() ? null : Map.of("source", text(root.get("sourceId")),
                "table", text(root.get("table")), "keys", list(root.get("key")),
                "primaryKey", String.join(", ", list(root.get("key")).stream().map(Object::toString).toList()),
                "rowCount", "", "columns", List.of(), "transforms", readTransforms(root.get("preTransforms"))));
        List<Map<String, Object>> rows = list(wire.get("related")).stream().map(AssistantWizardJson::object).toList();
        value.put("relations", readRelated(rows, "root", text(root.get("table")), 0));
        value.put("transforms", readTransforms(wire.get("transforms")));
        value.put("destinationSourceId", text(config.get("sourceId")));
        value.put("destinationTable", text(config.get("table")));
        value.put("destinationWriteMode", text(config.getOrDefault("writeMode", "upsert")));
        return value;
    }
    private static List<Map<String, Object>> readRelated(List<Map<String, Object>> rows, String parent, String table, int depth) {
        if (depth > 32) throw new IllegalArgumentException("relation nesting is too deep");
        List<Map<String, Object>> result = new ArrayList<>();
        for (var row : rows) {
            if (!parent.equals(row.get("parentId"))) continue;
            Map<String, Object> relation = object(row.get("relation"));
            List<Object> on = list(relation.get("on"));
            Map<String, Object> first = on.isEmpty() ? Map.of() : object(on.getFirst());
            Map<String, Object> node = new LinkedHashMap<>();
            node.put("id", row.get("id")); node.put("source", row.get("sourceId")); node.put("table", row.get("table"));
            node.put("type", switch (text(relation.get("shape"))) { case "flat" -> "flatten"; case "array" -> "sub_array"; default -> "sub_doc"; });
            node.put("primaryKey", list(relation.get("key"))); node.put("arrayKey", list(relation.get("arrayKey")));
            node.put("targetField", text(relation.get("path")));
            node.put("key", text(first.get("childField"))); node.put("parentKey", text(first.get("parentField")));
            node.put("parentTable", table); node.put("preProcessing", List.of());
            node.put("conditions", on.stream().map(AssistantWizardJson::object).map(pair -> Map.of(
                    "childKey", text(pair.get("childField")), "parentKey", text(pair.get("parentField")), "parentTable", table, "operator", "=")).toList());
            node.put("transforms", readTransforms(row.get("preTransforms")));
            node.put("children", readRelated(rows, text(row.get("id")), text(row.get("table")) + " · " + row.get("id"), depth + 1));
            result.add(node);
        }
        return result;
    }
    static Map<String, Object> wire(Map<String, Object> view, Map<String, Object> original) {
        Map<String, Object> root = object(view.get("root"));
        List<Map<String, Object>> related = new ArrayList<>();
        writeRelated(list(view.get("relations")), "root", related, 0);
        Map<String, Object> oldOutput = object(original.get("output"));
        Map<String, Object> output = new LinkedHashMap<>(object(oldOutput.get("config")));
        output.put("sourceId", text(view.get("destinationSourceId"))); output.put("table", text(view.get("destinationTable")));
        output.put("inputTable", text(root.get("table"))); output.put("writeMode", text(view.get("destinationWriteMode")));
        return Map.of("root", Map.of("id", "root", "sourceId", text(root.get("source")),
                "table", text(root.get("table")), "key", list(root.get("keys")), "preTransforms", writeTransforms(root.get("transforms"))),
                "related", related, "transforms", writeTransforms(view.get("transforms")),
                "output", Map.of("kind", oldOutput.getOrDefault("kind", "atlas"), "config", output));
    }
    private static void writeRelated(List<Object> nodes, String parent, List<Map<String, Object>> rows, int depth) {
        if (depth > 32 || rows.size() > 100) throw new IllegalArgumentException("too many relations");
        for (Object raw : nodes) writeRelatedNode(object(raw), parent, rows, depth);
    }
    private static void writeRelatedNode(Map<String, Object> node, String parent, List<Map<String, Object>> rows, int depth) {
        String type = text(node.get("type"));
        if (!List.of("flatten", "sub_array", "sub_doc").contains(type)) throw new IllegalArgumentException("invalid relation kind");
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", text(node.get("id")));
        row.put("parentId", parent);
        row.put("sourceId", text(node.get("source")));
        row.put("table", text(node.get("table")));
        row.put("relation", relation(node, type));
        row.put("preTransforms", writeTransforms(node.get("transforms")));
        rows.add(row);
        writeRelated(list(node.get("children")), text(node.get("id")), rows, depth + 1);
    }
    private static Map<String, Object> relation(Map<String, Object> node, String type) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("shape", relationShape(type));
        value.put("path", type.equals("flatten") ? null : text(node.get("targetField")));
        value.put("key", list(node.get("primaryKey")));
        value.put("arrayKey", type.equals("sub_array") ? list(node.get("arrayKey")) : List.of());
        value.put("on", relationConditions(node).stream().map(AssistantWizardJson::object)
                .map(condition -> Map.of("childField", text(condition.get("childKey")),
                        "parentField", text(condition.get("parentKey")))).toList());
        return value;
    }
    private static String relationShape(String type) {
        return switch (type) {
            case "flatten" -> "flat";
            case "sub_array" -> "array";
            default -> "object";
        };
    }
    private static List<Object> relationConditions(Map<String, Object> node) {
        if (node.containsKey("conditions")) return list(node.get("conditions"));
        return List.of(Map.of("childKey", text(node.get("key")), "parentKey", text(node.get("parentKey"))));
    }
    static List<Map<String, Object>> readTransforms(Object raw) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object value : list(raw)) result.add(readTransform(object(value)));
        return result;
    }
    private static Map<String, Object> readTransform(Map<String, Object> step) {
        String type = text(step.get("type"));
        Map<String, Object> fields = object(step.get("fields"));
        Map<String, Object> config = "map".equals(type)
                ? Map.of("fields", readMapRules(step.get("id"), fields), "passthroughUnlisted", true) : fields;
        return Map.of("id", text(step.get("id")), "name", type, "type", type, "enabled", true,
                "config", Map.of(type, config));
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
        Map<String, Object> config = object(object(step.get("config")).get(type));
        return Map.of("id", text(step.get("id")), "type", type, "fields", writeTransformFields(type, config));
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
        for (Object rawRule : list(config.get("fields"))) {
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
