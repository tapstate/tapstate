package io.tapstate.core.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The optional, already-redacted diagnostic carried alongside an operation's existing result.
 * Wire consumers can discard an invalid extension while keeping the operation's original fields.
 */
public final class DiagnosticProjection {

    public static final int MAX_DEPTH = 32;
    private static final List<String> TEXT_FIELDS = List.of("code", "message", "solution", "rawMessage", "stackTrace");

    private DiagnosticProjection() {
    }

    public static Map<String, Object> read(Object input) {
        return read(input, 1);
    }

    private static Map<String, Object> read(Object input, int depth) {
        if (!(input instanceof Map<?, ?> source) || depth > MAX_DEPTH) {
            return Map.of();
        }
        Map<String, Object> result = new LinkedHashMap<>();
        for (String key : TEXT_FIELDS) {
            if (source.containsKey(key)) {
                if (!(source.get(key) instanceof String value) || value.isBlank()) {
                    return Map.of();
                }
                result.put(key, value);
            }
        }
        if (source.containsKey("truncated")) {
            if (!Boolean.TRUE.equals(source.get("truncated"))) {
                return Map.of();
            }
            result.put("truncated", true);
        }
        if (source.containsKey("definitionStatus")) {
            if (!"unverified".equals(source.get("definitionStatus"))) return Map.of();
            result.put("definitionStatus", "unverified");
        }
        if (!copyParameters(source, result) || !copyCauses(source, result, depth)
                || !copyReferences(source, result)) {
            return Map.of();
        }
        return Collections.unmodifiableMap(result);
    }

    private static boolean copyParameters(Map<?, ?> source, Map<String, Object> result) {
        if (source.containsKey("params")) {
            if (source.containsKey("arguments") || !(source.get("params") instanceof Map<?, ?> raw)) {
                return false;
            }
            Map<String, Object> params = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : raw.entrySet()) {
                Object value = entry.getValue();
                if (!(entry.getKey() instanceof String key) || !isParameter(value)) {
                    return false;
                }
                params.put(key, value);
            }
            result.put("params", Collections.unmodifiableMap(params));
        }
        if (source.containsKey("arguments")) {
            if (!(source.get("arguments") instanceof List<?> args)
                    || !args.stream().allMatch(String.class::isInstance)) {
                return false;
            }
            result.put("arguments", List.copyOf(args));
        }
        return true;
    }

    private static boolean isParameter(Object value) {
        if (value instanceof Double decimal) {
            return Double.isFinite(decimal);
        }
        if (value instanceof Float decimal) {
            return Float.isFinite(decimal);
        }
        return value == null || value instanceof String || value instanceof Boolean || value instanceof Number;
    }

    private static boolean copyCauses(Map<?, ?> source, Map<String, Object> result, int depth) {
        if (source.containsKey("cause")) {
            if (source.containsKey("causes")) {
                return false;
            }
            Map<String, Object> child = read(source.get("cause"), depth + 1);
            if (child.isEmpty()) {
                return false;
            }
            result.put("cause", child);
        }
        if (source.containsKey("causes")) {
            if (!(source.get("causes") instanceof List<?> raw)
                    || (raw.size() < 2 && !Boolean.TRUE.equals(source.get("truncated")))) {
                return false;
            }
            List<Map<String, Object>> children = new ArrayList<>();
            for (Object nested : raw) {
                Map<String, Object> child = read(nested, depth + 1);
                if (child.isEmpty()) {
                    return false;
                }
                children.add(child);
            }
            result.put("causes", List.copyOf(children));
        }
        return true;
    }

    private static boolean copyReferences(Map<?, ?> source, Map<String, Object> result) {
        if (!source.containsKey("references")) {
            return true;
        }
        if (!(source.get("references") instanceof List<?> raw)) {
            return false;
        }
        List<Map<String, String>> references = new ArrayList<>();
        for (Object entry : raw) {
            if (!(entry instanceof Map<?, ?> reference)
                    || !(reference.get("type") instanceof String type) || type.isEmpty()
                    || !(reference.get("id") instanceof String id) || id.isEmpty()) {
                return false;
            }
            if (reference.containsKey("pipelineId")) {
                if (!(reference.get("pipelineId") instanceof String pipelineId) || pipelineId.isEmpty()) {
                    return false;
                }
                references.add(Map.of("type", type, "id", id, "pipelineId", pipelineId));
            } else {
                if ("pipeline-node".equals(type)) {
                    return false;
                }
                references.add(Map.of("type", type, "id", id));
            }
        }
        if (!references.isEmpty()) {
            result.put("references", List.copyOf(references));
        }
        return true;
    }
}
