package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class MonitorErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.MONITOR);

    public static final ProductErrorDefinition NO_OBSERVATION =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition INVALID_CURSOR =
            DEFINITIONS.define(2, Severity.ERROR, Set.of("operation", "reason"));
    public static final ProductErrorDefinition CURSOR_EXPIRED =
            DEFINITIONS.define(3, Severity.ERROR, Set.of("operation"));
    public static final ProductErrorDefinition QUERY_BUDGET_EXCEEDED =
            DEFINITIONS.define(4, Severity.ERROR, Set.of("operation", "budget", "limit"));

    private MonitorErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
