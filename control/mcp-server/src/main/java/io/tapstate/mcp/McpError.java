package io.tapstate.mcp;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;

import java.util.Set;

/** Stable coded failures owned by the local MCP presentation adapter. */
enum McpError implements TapstateErrorCode {

    CONNECTOR_SPEC_UNAVAILABLE("mcp.connector-spec-unavailable", Set.of("connector")),
    ENVIRONMENT_MISSING("mcp.environment-missing", Set.of("variable")),
    INVALID_SERVER_RESPONSE("mcp.invalid-server-response", Set.of("operation")),
    SERVER_REJECTED("mcp.server-rejected", Set.of("status"));

    private final String code;
    private final Set<String> placeholders;

    McpError(String code, Set<String> placeholders) {
        this.code = code;
        this.placeholders = placeholders;
    }

    @Override
    public String code() {
        return code;
    }

    @Override
    public Severity severity() {
        return Severity.ERROR;
    }

    @Override
    public Set<String> placeholders() {
        return placeholders;
    }
}
