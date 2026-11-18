package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Startup diagnostics retain the original configuration and process boundaries. */
public final class RoleErrorDefinitions {
    public static final ProductErrorDefinition UNSUPPORTED =
            new ProductErrorDefinition(ProductErrorModule.ROLE, 1, Severity.ERROR, Set.of("role"));

    private RoleErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(UNSUPPORTED);
    }
}
