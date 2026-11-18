package io.tapstate.control.restapi;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Map;

/**
 * The structured body of a coded error response: the canonical code string, the named arguments that are
 * the single source of variable data, and the message rendered from those arguments through the shared
 * catalog. The code crosses the wire as its stable string identity — the enum never leaves the process.
 * A record so the shape is explicit and can grow (e.g. a solution hint) without changing the fields a
 * client already reads.
 */
public record ApiError(String code, Map<String, Object> params, String message,
        @JsonInclude(JsonInclude.Include.NON_EMPTY) Map<String, Object> diagnostic) {
    public ApiError {
        diagnostic = diagnostic == null ? Map.of() : Map.copyOf(diagnostic);
    }

    public ApiError(String code, Map<String, Object> params, String message) {
        this(code, params, message, Map.of());
    }
}
