package io.tapstate.mcp;

import io.tapstate.control.client.ControlResponse;
import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.messages.MessageCatalog;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Structured tool result, including MCP's protocol-level error bit. */
record McpResult(boolean error, Map<String, Object> body) {

    private static final MessageCatalog MESSAGES = MessageCatalog.bundled();

    McpResult {
        body = Collections.unmodifiableMap(new LinkedHashMap<>(body));
    }

    static McpResult from(ControlResponse response) {
        return switch (response) {
            case ControlResponse.Success success -> success(success.body());
            case ControlResponse.Rejected rejected -> rejected(rejected);
            case ControlResponse.Unreachable ignored -> coded(ControlError.UNREACHABLE, Map.of());
        };
    }

    static McpResult success(Object value) {
        if (value instanceof Map<?, ?> map) {
            return new McpResult(false, stringKeys(map));
        }
        return new McpResult(false, value == null ? Map.of() : Map.of("value", value));
    }

    static McpResult coded(TapstateErrorCode code, Map<String, Object> params) {
        MessageCatalog.Rendered rendered = MESSAGES.render(code, params);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("code", code.code());
        body.put("message", rendered.message());
        body.put("params", params);
        if (rendered.solution() != null) {
            body.put("solution", rendered.solution());
        }
        return new McpResult(true, body);
    }

    private static McpResult rejected(ControlResponse.Rejected rejected) {
        if (rejected.code().isBlank()) {
            return coded(McpError.SERVER_REJECTED, Map.of("status", rejected.status()));
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("code", rejected.code());
        body.put("message", rejected.message());
        body.put("params", rejected.params());
        body.put("status", rejected.status());
        // whatever else the refusal carried, as it carried it: a start its checks stopped carries their report,
        // which is what the caller has to put to a person, and four fields would leave it out
        rejected.detail().forEach(body::putIfAbsent);
        return new McpResult(true, body);
    }

    private static Map<String, Object> stringKeys(Map<?, ?> source) {
        Map<String, Object> result = new LinkedHashMap<>();
        source.forEach((key, value) -> result.put(String.valueOf(key), value));
        return result;
    }
}
