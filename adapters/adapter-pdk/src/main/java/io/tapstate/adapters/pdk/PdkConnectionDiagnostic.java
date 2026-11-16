package io.tapstate.adapters.pdk;

import io.tapdata.exception.TapCodeException;
import io.tapdata.pdk.apis.exception.TapTestItemException;
import io.tapstate.core.common.DiagnosticProjection;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Captures connector-owned exception facts before the result leaves the PDK adapter. */
final class PdkConnectionDiagnostic {
    private static final int TEXT_LIMIT = 4096;
    private static final int STACK_FRAMES = 32;
    private static final int ARGUMENT_LIMIT = 32;

    private PdkConnectionDiagnostic() {
    }

    static Map<String, Object> capture(TapTestItemException failure, String message, String solution, String code) {
        var node = new LinkedHashMap<String, Object>();
        put(node, "code", code);
        put(node, "message", message);
        put(node, "solution", solution);
        if (failure == null) return DiagnosticProjection.read(node);
        try {
            Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
            seen.add(failure);
            arguments(node, failure.getDynamicDescriptionParameters());
            stack(node, failure, failure.getStack());
            addCause(node, failure, seen, 1);
            return DiagnosticProjection.read(node);
        } catch (RuntimeException | LinkageError unavailable) {
            // A secondary projection getter must never replace the check's original result.
            return DiagnosticProjection.read(node);
        }
    }

    private static void addCause(Map<String, Object> node, Throwable failure, Set<Throwable> seen, int depth) {
        Throwable cause = failure.getCause();
        if (cause == null) return;
        if (depth >= DiagnosticProjection.MAX_DEPTH || !seen.add(cause)) {
            node.put("truncated", true);
            return;
        }
        var child = new LinkedHashMap<String, Object>();
        if (cause instanceof TapCodeException coded) {
            put(child, "code", coded.getCode());
            arguments(child, coded.getDynamicDescriptionParameters());
        } else if (cause instanceof SQLException sql) {
            put(child, "code", sql.getSQLState());
        }
        put(child, "rawMessage", cause.getMessage());
        stack(child, cause, null);
        addCause(child, cause, seen, depth + 1);
        if (!child.isEmpty()) node.put("cause", child);
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
