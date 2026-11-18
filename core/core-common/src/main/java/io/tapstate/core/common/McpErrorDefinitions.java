package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Local MCP adapter failures; structured remote errors retain their original identity. */
public final class McpErrorDefinitions {
    public static final ProductErrorDefinition CONNECTOR_SPEC_UNAVAILABLE =
            new ProductErrorDefinition(ProductErrorModule.MCP, 1, Severity.ERROR, Set.of("connector"));
    public static final ProductErrorDefinition ENVIRONMENT_MISSING =
            new ProductErrorDefinition(ProductErrorModule.MCP, 2, Severity.ERROR, Set.of("variable"));
    public static final ProductErrorDefinition INVALID_SERVER_RESPONSE =
            new ProductErrorDefinition(ProductErrorModule.MCP, 3, Severity.ERROR, Set.of("operation"));
    public static final ProductErrorDefinition SERVER_REJECTED =
            new ProductErrorDefinition(ProductErrorModule.MCP, 4, Severity.ERROR, Set.of("status"));

    private McpErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(CONNECTOR_SPEC_UNAVAILABLE, ENVIRONMENT_MISSING, INVALID_SERVER_RESPONSE, SERVER_REJECTED);
    }
}
