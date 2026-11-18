package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Local MCP adapter failures; structured remote errors retain their original identity. */
public final class McpErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.MCP);

    public static final ProductErrorDefinition CONNECTOR_SPEC_UNAVAILABLE =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("connector"));
    public static final ProductErrorDefinition ENVIRONMENT_MISSING =
            DEFINITIONS.define(2, Severity.ERROR, Set.of("variable"));
    public static final ProductErrorDefinition INVALID_SERVER_RESPONSE =
            DEFINITIONS.define(3, Severity.ERROR, Set.of("operation"));
    public static final ProductErrorDefinition SERVER_REJECTED =
            DEFINITIONS.define(4, Severity.ERROR, Set.of("status"));

    private McpErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
