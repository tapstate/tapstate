package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class MonitorErrorDefinitions {
    public static final ProductErrorDefinition NO_OBSERVATION =
            new ProductErrorDefinition(ProductErrorModule.MONITOR, 1, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition INVALID_CURSOR =
            new ProductErrorDefinition(ProductErrorModule.MONITOR, 2, Severity.ERROR, Set.of("operation", "reason"));
    public static final ProductErrorDefinition CURSOR_EXPIRED =
            new ProductErrorDefinition(ProductErrorModule.MONITOR, 3, Severity.ERROR, Set.of("operation"));
    public static final ProductErrorDefinition QUERY_BUDGET_EXCEEDED =
            new ProductErrorDefinition(ProductErrorModule.MONITOR, 4, Severity.ERROR, Set.of("operation", "budget", "limit"));

    private MonitorErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(NO_OBSERVATION, INVALID_CURSOR, CURSOR_EXPIRED, QUERY_BUDGET_EXCEEDED);
    }
}
