package io.tapstate.mcp;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.McpErrorDefinitions;

import java.util.Set;

/** Stable coded failures owned by the local MCP presentation adapter. */
enum McpError implements TapstateErrorCode {

    CONNECTOR_SPEC_UNAVAILABLE(McpErrorDefinitions.CONNECTOR_SPEC_UNAVAILABLE),
    ENVIRONMENT_MISSING(McpErrorDefinitions.ENVIRONMENT_MISSING),
    INVALID_SERVER_RESPONSE(McpErrorDefinitions.INVALID_SERVER_RESPONSE),
    SERVER_REJECTED(McpErrorDefinitions.SERVER_REJECTED);

    private final ProductErrorDefinition definition;

    McpError(ProductErrorDefinition definition) {
        this.definition = definition;
    }

    @Override
    public String code() {
        return definition.code();
    }

    @Override
    public Severity severity() {
        return definition.severity();
    }

    @Override
    public Set<String> placeholders() {
        return definition.parameters();
    }
}
