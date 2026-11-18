package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class ClusterErrorDefinitions {
    public static final ProductErrorDefinition MEMBERSHIP_UNREADABLE =
            new ProductErrorDefinition(ProductErrorModule.CLUSTER, 1, Severity.ERROR, Set.of());

    private ClusterErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(MEMBERSHIP_UNREADABLE);
    }
}
