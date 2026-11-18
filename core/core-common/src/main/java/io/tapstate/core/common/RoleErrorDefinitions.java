package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Startup diagnostics retain the original configuration and process boundaries. */
public final class RoleErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.ROLE);

    public static final ProductErrorDefinition UNSUPPORTED =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("role"));

    private RoleErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
