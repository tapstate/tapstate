package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class MigrationErrorDefinitions {
    public static final ProductErrorDefinition DATA_NEWER_THAN_BINARY =
            new ProductErrorDefinition(ProductErrorModule.MIGRATION, 1, Severity.ERROR, Set.of("installed", "supported"));
    public static final ProductErrorDefinition LOCK_TIMEOUT =
            new ProductErrorDefinition(ProductErrorModule.MIGRATION, 2, Severity.ERROR, Set.of("holder", "since"));
    public static final ProductErrorDefinition CHANGESET_FAILED =
            new ProductErrorDefinition(ProductErrorModule.MIGRATION, 3, Severity.ERROR, Set.of("changeset", "cause"));

    private MigrationErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(DATA_NEWER_THAN_BINARY, LOCK_TIMEOUT, CHANGESET_FAILED);
    }
}
