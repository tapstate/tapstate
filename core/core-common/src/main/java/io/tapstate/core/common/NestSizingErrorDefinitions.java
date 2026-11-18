package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class NestSizingErrorDefinitions {
    public static final ProductErrorDefinition STATE_FAR_EXCEEDS_MEMORY_BUDGET =
            new ProductErrorDefinition(ProductErrorModule.NEST_SIZING, 1, Severity.WARNING, Set.of("pipeline", "embedPath", "estimatedEntries", "budget", "multiple"));
    public static final ProductErrorDefinition CAPACITY_ESTIMATE_INCOMPLETE =
            new ProductErrorDefinition(ProductErrorModule.NEST_SIZING, 2, Severity.WARNING, Set.of("pipeline", "embedPath", "counted", "total"));

    private NestSizingErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(STATE_FAR_EXCEEDS_MEMORY_BUDGET, CAPACITY_ESTIMATE_INCOMPLETE);
    }
}
