package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class ClusterErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.CLUSTER);

    public static final ProductErrorDefinition MEMBERSHIP_UNREADABLE =
            DEFINITIONS.define(1, Severity.ERROR, Set.of());

    private ClusterErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
