package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class NestSizingErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.NEST_SIZING);

    public static final ProductErrorDefinition STATE_FAR_EXCEEDS_MEMORY_BUDGET =
            DEFINITIONS.define(1, Severity.WARNING, Set.of("pipeline", "embedPath", "estimatedEntries", "budget", "multiple"));
    public static final ProductErrorDefinition CAPACITY_ESTIMATE_INCOMPLETE =
            DEFINITIONS.define(2, Severity.WARNING, Set.of("pipeline", "embedPath", "counted", "total"));

    private NestSizingErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
