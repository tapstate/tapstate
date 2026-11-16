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
    private static final String TRUNCATED = "truncated";
    private static final String DEFINITION_STATUS = "definitionStatus";
    private static final String UNVERIFIED = "unverified";
    private static final String PARAMETERS = "params";
    private static final String ARGUMENTS = "arguments";
    private static final String CAUSE = "cause";
    private static final String CAUSES = "causes";
    private static final String REFERENCES = "references";
    private static final String PIPELINE_ID = "pipelineId";
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
        if (!copyRootFields(source, result) || !copyParameters(source, result) || !copyCauses(source, result, depth)
                || !copyReferences(source, result)) {
            return Map.of();
        }
        return Collections.unmodifiableMap(result);
    }

    private static boolean copyRootFields(Map<?, ?> source, Map<String, Object> result) {
        for (String key : TEXT_FIELDS) {
            if (source.containsKey(key)) {
                if (!(source.get(key) instanceof String value) || value.isBlank()) {
                    return false;
                }
                result.put(key, value);
            }
        }
        if (source.containsKey(TRUNCATED)) {
            if (!Boolean.TRUE.equals(source.get(TRUNCATED))) {
                return false;
            }
            result.put(TRUNCATED, true);
        }
        if (source.containsKey(DEFINITION_STATUS)) {
            if (!UNVERIFIED.equals(source.get(DEFINITION_STATUS))) return false;
            result.put(DEFINITION_STATUS, UNVERIFIED);
        }
        return true;
    }

    private static boolean copyParameters(Map<?, ?> source, Map<String, Object> result) {
        if (source.containsKey(PARAMETERS)) {
            if (source.containsKey(ARGUMENTS) || !(source.get(PARAMETERS) instanceof Map<?, ?> raw)) {
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
            result.put(PARAMETERS, Collections.unmodifiableMap(params));
        }
        if (source.containsKey(ARGUMENTS)) {
            if (!(source.get(ARGUMENTS) instanceof List<?> args)
                    || !args.stream().allMatch(String.class::isInstance)) {
                return false;
            }
            result.put(ARGUMENTS, List.copyOf(args));
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
        if (source.containsKey(CAUSE)) {
            if (source.containsKey(CAUSES)) {
                return false;
            }
            Map<String, Object> child = read(source.get(CAUSE), depth + 1);
            if (child.isEmpty()) {
                return false;
            }
            result.put(CAUSE, child);
        }
        if (source.containsKey(CAUSES)) {
            if (!(source.get(CAUSES) instanceof List<?> raw)
                    || (raw.size() < 2 && !Boolean.TRUE.equals(source.get(TRUNCATED)))) {
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
            result.put(CAUSES, List.copyOf(children));
        }
        return true;
    }

    private static boolean copyReferences(Map<?, ?> source, Map<String, Object> result) {
        if (!source.containsKey(REFERENCES)) {
            return true;
        }
        if (!(source.get(REFERENCES) instanceof List<?> raw)) {
            return false;
        }
        List<Map<String, String>> references = new ArrayList<>();
        for (Object entry : raw) {
            Map<String, String> reference = readReference(entry);
            if (reference.isEmpty()) return false;
            references.add(reference);
        }
        if (!references.isEmpty()) {
            result.put(REFERENCES, List.copyOf(references));
        }
        return true;
    }

    private static Map<String, String> readReference(Object entry) {
        if (!(entry instanceof Map<?, ?> reference)
                || !(reference.get("type") instanceof String type) || type.isEmpty()
                || !(reference.get("id") instanceof String id) || id.isEmpty()) {
            return Map.of();
        }
        if (reference.containsKey(PIPELINE_ID)) {
            return reference.get(PIPELINE_ID) instanceof String pipelineId && !pipelineId.isEmpty()
                    ? Map.of("type", type, "id", id, PIPELINE_ID, pipelineId) : Map.of();
        }
        return "pipeline-node".equals(type) ? Map.of() : Map.of("type", type, "id", id);
    }
}
