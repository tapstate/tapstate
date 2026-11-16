package io.tapstate.adapters.pdk;

import io.tapdata.exception.TapCodeException;
import io.tapdata.entity.error.CoreException;
import io.tapdata.pdk.apis.exception.TapTestItemException;
import io.tapstate.core.common.DiagnosticProjection;
import io.tapstate.core.common.DiagnosticCodeSource;

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
    private static final String RAW_MESSAGE = "rawMessage";
    private static final String TRUNCATED = "truncated";
    private static final String UNVERIFIED = "unverified";
    private static final String DEFINITION_STATUS = "definitionStatus";

    private PdkConnectionDiagnostic() {
    }

    static Map<String, Object> capture(Throwable failure, String message, String solution, String code,
            PdkDiagnosticCatalog catalog, Locale locale) {
        var node = new LinkedHashMap<String, Object>();
        put(node, "code", code);
        put(node, "message", message);
        put(node, "solution", solution);
        if (node.containsKey("code") && !(failure instanceof SQLException)) node.put(DEFINITION_STATUS, UNVERIFIED);
        if (failure == null) return DiagnosticProjection.read(node);
        try {
            Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
            seen.add(failure);
            String reportedStack = null;
            if (failure instanceof TapTestItemException item) {
                arguments(node, item.getDynamicDescriptionParameters());
                reportedStack = item.getStack();
            } else {
                put(node, RAW_MESSAGE, failure.getMessage());
                if (failure instanceof TapCodeException coded) arguments(node, coded.getDynamicDescriptionParameters());
            }
            identifyCode(node, failure, catalog, locale);
            stack(node, failure, reportedStack);
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
            appendGroup(node, cause, additional, seen, depth, catalog, locale);
        } else if (cause != null) {
            var child = child(node, cause, seen, depth, catalog, locale);
            if (!child.isEmpty()) node.put("cause", child);
        } else if (additional != null && additional.size() == 1) {
            appendRawDetail(node, additional.getFirst());
        }
    }

    private static void appendGroup(Map<String, Object> node, Throwable cause, List<CoreException> additional,
            Set<Throwable> seen, int depth, PdkDiagnosticCatalog catalog, Locale locale) {
        List<Map<String, Object>> children = new ArrayList<>();
        if (cause != null) addIfPresent(children, child(node, cause, seen, depth, catalog, locale));
        for (var more : additional) {
            if (seen.size() >= NODE_LIMIT || depth >= DiagnosticProjection.MAX_DEPTH) {
                node.put(TRUNCATED, true);
                break;
            }
            if (more != null) addIfPresent(children, child(node, more, seen, depth, catalog, locale));
        }
        if (children.size() < 2) node.put(TRUNCATED, true);
        node.put("causes", List.copyOf(children));
    }

    private static void appendRawDetail(Map<String, Object> node, CoreException detail) {
        String message = detail == null ? null : detail.getMessage();
        if (message == null) return;
        Object original = node.get(RAW_MESSAGE);
        put(node, RAW_MESSAGE, original == null ? message : original + "\n" + message);
    }

    private static void addIfPresent(List<Map<String, Object>> children, Map<String, Object> child) {
        if (!child.isEmpty()) children.add(child);
    }

    private static Map<String, Object> child(Map<String, Object> node, Throwable cause, Set<Throwable> seen, int depth,
            PdkDiagnosticCatalog catalog, Locale locale) {
        if (depth >= DiagnosticProjection.MAX_DEPTH || seen.size() >= NODE_LIMIT || !seen.add(cause)) {
            node.put(TRUNCATED, true);
            return Map.of();
        }
        var child = new LinkedHashMap<String, Object>();
        if (cause instanceof TapCodeException coded) {
            put(child, "code", coded.getCode());
            arguments(child, coded.getDynamicDescriptionParameters());
            if (child.containsKey("code")) child.put(DEFINITION_STATUS, UNVERIFIED);
        } else if (cause instanceof SQLException sql) {
            put(child, "code", sql.getSQLState());
        } else if (cause instanceof CoreException grouped && grouped.getCode() != 0) {
            put(child, "code", Integer.toString(grouped.getCode()));
            child.put(DEFINITION_STATUS, UNVERIFIED);
        }
        identifyCode(child, cause, catalog, locale);
        put(child, RAW_MESSAGE, cause.getMessage());
        stack(child, cause, null);
        addCause(child, cause, seen, depth + 1, catalog, locale);
        return child;
    }

    private static void identifyCode(Map<String, Object> node, Throwable failure,
            PdkDiagnosticCatalog catalog, Locale locale) {
        if (!(node.get("code") instanceof String code)) return;
        if (failure instanceof SQLException) {
            node.put(DiagnosticCodeSource.FIELD, DiagnosticCodeSource.JDBC_SQLSTATE);
        } else if (isSharedPdkCode(failure)) {
            node.put(DiagnosticCodeSource.FIELD, DiagnosticCodeSource.PDK);
        } else if (failure instanceof TapCodeException || failure instanceof TapTestItemException) {
            resolveDefinition(node, code, catalog, locale);
        }
    }

    private static boolean isSharedPdkCode(Throwable failure) {
        if (!(failure instanceof TapCodeException)) return false;
        Class<?> concrete = failure.getClass();
        if (concrete.getClassLoader() != TapCodeException.class.getClassLoader()) return false;
        // The runtime PDK exceptions are outside the adapter's compile-time API. A plugin can
        // subclass the shared base with a code of its own, so require the same defining domain.
        for (Class<?> type = concrete; type != TapCodeException.class; type = type.getSuperclass()) {
            if (type.getName().equals("io.tapdata.exception.TapPdkBaseException")) {
                return concrete.getProtectionDomain() == type.getProtectionDomain();
            }
        }
        return false;
    }

    private static void resolveDefinition(Map<String, Object> node, String code, PdkDiagnosticCatalog catalog, Locale locale) {
        List<String> args = node.get("arguments") instanceof List<?> values
                ? values.stream().map(String.class::cast).toList() : List.of();
        var definition = catalog.resolve(code, Map.of(), args, locale).orElse(null);
        if (definition == null || definition.code().length() > TEXT_LIMIT) return;
        node.put("code", definition.code());
        node.put(DiagnosticCodeSource.FIELD, DiagnosticCodeSource.CONNECTOR);
        node.remove(DEFINITION_STATUS);
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
            else if (detail != null) node.put(TRUNCATED, true);
            if (stack.length() >= TEXT_LIMIT) break;
        }
        if (frames.length > STACK_FRAMES || suppressed.length > STACK_FRAMES) node.put(TRUNCATED, true);
        put(node, "stackTrace", stack.toString());
    }

    private static void arguments(Map<String, Object> node, String[] values) {
        if (values == null || values.length == 0) return;
        // A missing positional value must not shift any argument into a different slot.
        if (values.length > ARGUMENT_LIMIT) {
            node.put(TRUNCATED, true);
            return;
        }
        var args = new ArrayList<String>(values.length);
        for (String value : values) {
            if (value == null || value.length() > TEXT_LIMIT) {
                node.put(TRUNCATED, true);
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
            node.put(TRUNCATED, true);
        } else {
            node.put(field, text);
        }
    }
}
