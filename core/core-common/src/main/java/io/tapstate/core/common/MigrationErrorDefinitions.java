package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class MigrationErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.MIGRATION);

    public static final ProductErrorDefinition DATA_NEWER_THAN_BINARY =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("installed", "supported"));
    public static final ProductErrorDefinition LOCK_TIMEOUT =
            DEFINITIONS.define(2, Severity.ERROR, Set.of("holder", "since"));
    public static final ProductErrorDefinition CHANGESET_FAILED =
            DEFINITIONS.define(3, Severity.ERROR, Set.of("changeset", "cause"));

    private MigrationErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
