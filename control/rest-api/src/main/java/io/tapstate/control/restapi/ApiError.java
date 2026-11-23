package io.tapstate.control.restapi;

import com.fasterxml.jackson.annotation.JsonInclude;

import io.tapstate.control.core.PipelineCatalogItem;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.messages.MessageCatalog;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/**
 * The structured body of a coded error response: the canonical code string, the named arguments that are
 * the single source of variable data, and the message rendered from those arguments through the shared
 * catalog. The code crosses the wire as its stable string identity — the enum never leaves the process.
 * A record so the shape is explicit and can grow (e.g. a solution hint) without changing the fields a
 * client already reads.
 */
public record ApiError(String code, Map<String, Object> params, String message,
        @JsonInclude(JsonInclude.Include.NON_EMPTY) String solution,
        @JsonInclude(JsonInclude.Include.NON_EMPTY) Map<String, Object> diagnostic) {
    public ApiError {
        diagnostic = diagnostic == null ? Map.of() : Map.copyOf(diagnostic);
    }

    static Map<String, Object> localizeDiagnostic(Map<String, Object> diagnostic, MessageCatalog catalog, Locale locale) {
        if (diagnostic == null || diagnostic.isEmpty()) return diagnostic;
        var result = new TreeMap<>(diagnostic);
        if (diagnostic.get("cause") instanceof Map<?, ?> cause) result.put("cause", localizeChild(cause, catalog, locale));
        if (diagnostic.get("causes") instanceof java.util.List<?> causes) {
            result.put("causes", causes.stream().map(cause -> cause instanceof Map<?, ?> child
                    ? localizeChild(child, catalog, locale) : cause).toList());
        }
        if (!"tapstate-standard".equals(diagnostic.get("codeSource"))
                || "unverified".equals(diagnostic.get("definitionStatus"))
                || !(diagnostic.get("code") instanceof String code)) return result;
        Map<String, Object> params = new TreeMap<>();
        if (diagnostic.get("params") instanceof Map<?, ?> values) {
            for (var entry : values.entrySet()) {
                if (!(entry.getKey() instanceof String key)) return diagnostic;
                params.put(key, entry.getValue());
            }
        }
        var available = catalog.renderAvailable(code, params, locale);
        if (available.isEmpty()) return result;
        var rendered = available.orElseThrow();
        if (diagnostic.get("message") instanceof String original && !original.equals(rendered.message())) {
            result.putIfAbsent("rawMessage", original);
        }
        result.put("message", rendered.message());
        if (rendered.solution() != null) result.put("solution", rendered.solution());
        return result;
    }

    private static Map<?, ?> localizeChild(Map<?, ?> child, MessageCatalog catalog, Locale locale) {
        Map<String, Object> fields = new TreeMap<>();
        for (var entry : child.entrySet()) {
            if (!(entry.getKey() instanceof String key)) return child;
            fields.put(key, entry.getValue());
        }
        return localizeDiagnostic(fields, catalog, locale);
    }

    static ApiError render(TapstateErrorCode code, Map<String, Object> params, Map<String, Object> diagnostic,
            MessageCatalog catalog, Locale locale) {
        var rendered = catalog.render(code, params, locale);
        return new ApiError(code.code(), new TreeMap<>(params), rendered.message(), rendered.solution(), localizeDiagnostic(diagnostic, catalog, locale));
    }

    static ApiError render(String code, Map<String, ?> parameters, Map<String, Object> diagnostic,
            MessageCatalog catalog, Locale locale) {
        Map<String, Object> params = new TreeMap<>(parameters);
        var rendered = catalog.render(code, params, locale);
        return new ApiError(code, params, rendered.message(), rendered.solution(), localizeDiagnostic(diagnostic, catalog, locale));
    }

    static ApiError from(TapstateException failure, MessageCatalog catalog, Locale locale) {
        var diagnostic = failure instanceof io.tapstate.control.core.ConnectionDiagnosticRefusal refusal
                ? refusal.diagnostic() : Map.<String, Object>of();
        return render(failure.code(), failure.args(), diagnostic, catalog, locale);
    }

    Map<String, Object> fields() {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("code", code);
        fields.put("params", params);
        fields.put("message", message);
        if (solution != null && !solution.isEmpty()) fields.put("solution", solution);
        if (!diagnostic.isEmpty()) fields.put("diagnostic", diagnostic);
        return fields;
    }

    static ApiError from(ObservationFailure failure, MessageCatalog catalog, Locale locale) {
        return failure == null ? null : render(failure.code(), failure.params(), failure.diagnostic(), catalog, locale);
    }

    static ApiError from(PipelineCatalogItem.IntentReadFailure failure, MessageCatalog catalog, Locale locale) {
        return failure == null ? null : render(failure.code(), failure.params(), Map.of(), catalog, locale);
    }

    public ApiError(String code, Map<String, Object> params, String message) {
        this(code, params, message, null, Map.of());
    }

    public ApiError(String code, Map<String, Object> params, String message, Map<String, Object> diagnostic) {
        this(code, params, message, null, diagnostic);
    }
}
