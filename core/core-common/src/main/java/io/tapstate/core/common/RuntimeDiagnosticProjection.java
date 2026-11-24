package io.tapstate.core.common;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Structural budgets for an optional, already-redacted runtime diagnostic. Not a sanitizer. */
public final class RuntimeDiagnosticProjection {
    public static final int MAX_NODES = 64;
    public static final int MAX_PARAMETERS = 32;
    public static final int MAX_TEXT = 4096;
    public static final int MAX_STACK_FRAMES = 32;
    public static final int MAX_REFERENCES = 128;

    private RuntimeDiagnosticProjection() {}

    /** Invalid or oversized input is not allowed to replace the original operation's failure. */
    public static Map<String, Object> fit(Map<String, Object> input) {
        try {
            if (input == null || input.isEmpty()) return Map.of();
            var result = read(input);
            return result.isEmpty() ? Map.of("truncated", true) : result;
        } catch (RuntimeException | LinkageError unavailable) {
            return Map.of("truncated", true);
        }
    }

    /** Strict readers reject invalid attachments rather than inventing or repairing stored facts. */
    public static Map<String, Object> read(Object input) {
        try {
            if (!(input instanceof Map<?, ?> map) || !new Budget().node(map, 1)) return Map.of();
            var result = DiagnosticProjection.read(input);
            return result.equals(input) ? result : Map.of();
        } catch (RuntimeException | LinkageError unavailable) {
            return Map.of();
        }
    }

    private static final class Budget {
        private final Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        private int nodes;
        private int references;

        boolean node(Map<?, ?> node, int depth) {
            if (depth > DiagnosticProjection.MAX_DEPTH || ++nodes > MAX_NODES || !seen.add(node)) return false;
            for (var entry : node.entrySet()) {
                if (!(entry.getKey() instanceof String key) || !text(key)) return false;
                Object value = entry.getValue();
                if (value instanceof String string && !text(string)) return false;
                switch (key) {
                    case "params" -> {
                        if (!(value instanceof Map<?, ?> params) || params.size() > MAX_PARAMETERS) return false;
                        for (var param : params.entrySet()) {
                            if (!(param.getKey() instanceof String name) || !text(name)) return false;
                            if (param.getValue() instanceof String string && !text(string)) return false;
                        }
                    }
                    case "arguments" -> {
                        if (!(value instanceof List<?> args) || args.size() > MAX_PARAMETERS) return false;
                        for (Object arg : args) if (!(arg instanceof String string) || !text(string)) return false;
                    }
                    case "stackTrace" -> {
                        if (!(value instanceof String stack) || stack.lines().count() > MAX_STACK_FRAMES) return false;
                    }
                    case "references" -> {
                        if (!(value instanceof List<?> refs) || refs.size() > MAX_REFERENCES - references) return false;
                        references += refs.size();
                        for (Object ref : refs) {
                            if (!(ref instanceof Map<?, ?> fields)) return false;
                            for (var field : fields.entrySet()) {
                                if (!(field.getKey() instanceof String name) || !text(name)
                                        || !(field.getValue() instanceof String string) || !text(string)) return false;
                            }
                        }
                    }
                    case "cause" -> {
                        if (!(value instanceof Map<?, ?> child) || !node(child, depth + 1)) return false;
                    }
                    case "causes" -> {
                        if (!(value instanceof List<?> children) || children.size() > MAX_NODES - nodes) return false;
                        for (Object child : children) {
                            if (!(child instanceof Map<?, ?> fields) || !node(fields, depth + 1)) return false;
                        }
                    }
                    default -> { }
                }
            }
            return true;
        }

        private static boolean text(String text) {
            return text.length() <= MAX_TEXT;
        }
    }
}
