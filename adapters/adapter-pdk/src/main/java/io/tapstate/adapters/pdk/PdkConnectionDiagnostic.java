package io.tapstate.adapters.pdk;

import io.tapdata.exception.TapCodeException;
import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.apis.exception.TapTestItemException;
import io.tapstate.core.common.DiagnosticProjection;
import io.tapstate.core.common.DiagnosticDefinitionLookup;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/** Captures connector-owned exception facts before the result leaves the PDK adapter. */
final class PdkConnectionDiagnostic {
    private static final int TEXT_LIMIT = 4096;
    private static final int STACK_FRAMES = 32;
    private static final int ARGUMENT_LIMIT = 32;
    private static final int NODE_LIMIT = 64;

    private PdkConnectionDiagnostic() {
    }

    static Map<String, Object> capture(TapTestItemException failure, String message, String solution, String code,
            PdkDiagnosticCatalog catalog, Locale locale) {
        var node = new LinkedHashMap<String, Object>();
        put(node, "code", code);
        put(node, "message", message);
        put(node, "solution", solution);
        if (node.containsKey("code")) node.put("definitionStatus", "unverified");
        if (failure == null) return DiagnosticProjection.read(node);
        try {
            Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
            seen.add(failure);
            arguments(node, failure.getDynamicDescriptionParameters());
            resolveDefinition(node, code, catalog, locale);
            stack(node, failure, failure.getStack());
            addCause(node, failure, seen, 1, catalog, locale);
            return DiagnosticProjection.read(node);
        } catch (RuntimeException | LinkageError unavailable) {
            // A secondary projection getter must never replace the check's original result.
            return DiagnosticProjection.read(node);
        }
    }

    private static void addCause(Map<String, Object> node, Throwable failure, Set<Throwable> seen, int depth,
            PdkDiagnosticCatalog catalog, Locale locale) {
        Throwable cause = failure.getCause();
        List<CoreException> additional = failure instanceof CoreException grouped ? grouped.getMoreExceptions() : null;
        if (additional != null && !additional.isEmpty() && (cause != null || additional.size() > 1)) {
            List<Map<String, Object>> children = new ArrayList<>();
            if (cause != null) addIfPresent(children, child(node, cause, seen, depth, catalog, locale));
            for (var more : additional) {
                if (seen.size() >= NODE_LIMIT || depth >= DiagnosticProjection.MAX_DEPTH) {
                    node.put("truncated", true);
                    break;
                }
                if (more != null) addIfPresent(children, child(node, more, seen, depth, catalog, locale));
            }
            if (children.size() < 2) node.put("truncated", true);
            node.put("causes", List.copyOf(children));
        } else if (cause != null) {
            var child = child(node, cause, seen, depth, catalog, locale);
            if (!child.isEmpty()) node.put("cause", child);
        } else if (additional != null && additional.size() == 1) {
            var more = additional.getFirst();
            if (more != null && more.getMessage() != null) {
                Object original = node.get("rawMessage");
                put(node, "rawMessage", original == null ? more.getMessage() : original + "\n" + more.getMessage());
            }
        }
    }

    private static void addIfPresent(List<Map<String, Object>> children, Map<String, Object> child) {
        if (!child.isEmpty()) children.add(child);
    }

    private static Map<String, Object> child(Map<String, Object> node, Throwable cause, Set<Throwable> seen, int depth,
            PdkDiagnosticCatalog catalog, Locale locale) {
        if (depth >= DiagnosticProjection.MAX_DEPTH || seen.size() >= NODE_LIMIT || !seen.add(cause)) {
            node.put("truncated", true);
            return Map.of();
        }
        var child = new LinkedHashMap<String, Object>();
        if (cause instanceof TapCodeException coded) {
            put(child, "code", coded.getCode());
            arguments(child, coded.getDynamicDescriptionParameters());
            if (child.containsKey("code")) child.put("definitionStatus", "unverified");
            resolveDefinition(child, coded.getCode(), catalog, locale);
        } else if (cause instanceof SQLException sql) {
            put(child, "code", sql.getSQLState());
        } else if (cause instanceof CoreException grouped && grouped.getCode() != 0) {
            put(child, "code", Integer.toString(grouped.getCode()));
            child.put("definitionStatus", "unverified");
        }
        put(child, "rawMessage", cause.getMessage());
        stack(child, cause, null);
        addCause(child, cause, seen, depth + 1, catalog, locale);
        return child;
    }

    private static void resolveDefinition(Map<String, Object> node, String code, DiagnosticDefinitionLookup catalog, Locale locale) {
        List<String> args = node.get("arguments") instanceof List<?> values
                ? values.stream().map(String.class::cast).toList() : List.of();
        var definition = catalog.resolve(code, Map.of(), args, locale).orElse(null);
        if (definition == null || definition.code().length() > TEXT_LIMIT) return;
        node.put("code", definition.code());
        node.remove("definitionStatus");
        put(node, "message", definition.message());
        put(node, "solution", definition.solution());
    }

    private static void stack(Map<String, Object> node, Throwable failure, String reported) {
        if (reported != null && !reported.isBlank()) {
            put(node, "stackTrace", reported);
            return;
        }
        StackTraceElement[] frames = failure.getStackTrace();
        Throwable[] suppressed = failure.getSuppressed();
        if (frames.length == 0 && suppressed.length == 0) return;
        var stack = new StringBuilder(failure.getClass().getName());
        for (int index = 0; index < Math.min(frames.length, STACK_FRAMES); index++) {
            stack.append("\n\tat ").append(frames[index]);
        }
        for (int index = 0; index < Math.min(suppressed.length, STACK_FRAMES); index++) {
            Throwable extra = suppressed[index];
            stack.append("\nSuppressed: ").append(extra.getClass().getName());
            String detail = extra.getMessage();
            if (detail != null && detail.length() <= TEXT_LIMIT) stack.append(": ").append(detail);
            else if (detail != null) node.put("truncated", true);
            if (stack.length() >= TEXT_LIMIT) break;
        }
        if (frames.length > STACK_FRAMES || suppressed.length > STACK_FRAMES) node.put("truncated", true);
        put(node, "stackTrace", stack.toString());
    }

    private static void arguments(Map<String, Object> node, String[] values) {
        if (values == null || values.length == 0) return;
        // A missing positional value must not shift any argument into a different slot.
        if (values.length > ARGUMENT_LIMIT) {
            node.put("truncated", true);
            return;
        }
        var args = new ArrayList<String>(values.length);
        for (String value : values) {
            if (value == null || value.length() > TEXT_LIMIT) {
                node.put("truncated", true);
                return;
            }
            args.add(value);
        }
        node.put("arguments", List.copyOf(args));
    }

    private static void put(Map<String, Object> node, String field, String text) {
        if (text == null || text.isBlank()) return;
        // Omitting an oversized field avoids publishing a partial credential before redaction.
        if (text.length() > TEXT_LIMIT) {
            node.put("truncated", true);
        } else {
            node.put(field, text);
        }
    }
}
