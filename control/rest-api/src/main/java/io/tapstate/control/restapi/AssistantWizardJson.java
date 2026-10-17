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
        for (Object raw : nodes) {
            var node = object(raw); String type = text(node.get("type"));
            if (!List.of("flatten", "sub_array", "sub_doc").contains(type)) throw new IllegalArgumentException("invalid relation kind");
            List<Object> conditions = node.containsKey("conditions") ? list(node.get("conditions"))
                    : List.of(Map.of("childKey", text(node.get("key")), "parentKey", text(node.get("parentKey"))));
            Map<String, Object> relation = new LinkedHashMap<>();
            relation.put("shape", type.equals("flatten") ? "flat" : type.equals("sub_array") ? "array" : "object");
            relation.put("path", type.equals("flatten") ? null : text(node.get("targetField")));
            relation.put("key", list(node.get("primaryKey")));
            relation.put("arrayKey", type.equals("sub_array") ? list(node.get("arrayKey")) : List.of());
            relation.put("on", conditions.stream().map(AssistantWizardJson::object)
                    .map(c -> Map.of("childField", text(c.get("childKey")), "parentField", text(c.get("parentKey")))).toList());
            rows.add(Map.of("id", text(node.get("id")), "parentId", parent, "sourceId", text(node.get("source")),
                    "table", text(node.get("table")), "relation", relation, "preTransforms", writeTransforms(node.get("transforms"))));
            writeRelated(list(node.get("children")), text(node.get("id")), rows, depth + 1);
        }
    }
    static List<Map<String, Object>> readTransforms(Object raw) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object value : list(raw)) {
            var step = object(value); var fields = object(step.get("fields")); String type = text(step.get("type"));
            Map<String, Object> config;
            if ("map".equals(type)) {
                List<Map<String, Object>> rules = new ArrayList<>(); int i = 0;
                for (var entry : fields.entrySet()) {
                    Object v = entry.getValue(); String str = v instanceof String s ? s : "";
                    String rule = Boolean.FALSE.equals(v) ? "delete" : str.startsWith("$") ? "rename" : str.startsWith("=") ? "compute" : "literal";
                    rules.add(Map.of("id", text(step.get("id")) + "-" + i++, "outputField", entry.getKey(), "ruleType", rule,
                            "value", str.startsWith("$") || str.startsWith("=") ? str : JsonWriter.write(v)));
                }
                config = Map.of("fields", rules, "passthroughUnlisted", true);
            } else config = fields;
            result.add(Map.of("id", text(step.get("id")), "name", type, "type", type, "enabled", true,
                    "config", Map.of(type, config)));
        }
        return result;
    }
    static List<Map<String, Object>> writeTransforms(Object raw) {
        List<Map<String, Object>> result = new ArrayList<>();
        for (Object value : list(raw)) {
            var step = object(value); String type = text(step.get("type"));
            if (!List.of("filter", "map", "unwind", "js").contains(type)) throw new IllegalArgumentException("unsupported transform kind");
            Map<String, Object> config = object(object(step.get("config")).get(type));
            Map<String, Object> fields = new LinkedHashMap<>();
            if ("map".equals(type)) {
                for (Object rawRule : list(config.get("fields"))) {
                    var rule = object(rawRule); String text = text(rule.get("value")); Object resolved = text;
                    switch (text(rule.get("ruleType"))) {
                        case "delete" -> resolved = false;
                        case "rename" -> resolved = text.startsWith("$") ? text : "$" + text;
                        case "compute" -> resolved = text.startsWith("=") ? text : "=" + text;
                        case "literal" -> { try { resolved = JsonReader.parse(text); } catch (IllegalArgumentException ignored) { resolved = text; } }
                        default -> throw new IllegalArgumentException("invalid map field rule");
                    }
                    fields.put(text(rule.get("outputField")), resolved);
                }
            } else if ("filter".equals(type)) fields.put("expr", text(config.get("expr")));
            else if ("js".equals(type)) fields.put("script", text(config.get("script")));
            else fields.putAll(config);
            result.add(Map.of("id", text(step.get("id")), "type", type, "fields", fields));
        }
        return result;
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
