package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class RowValueErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.ROW_VALUE);

    public static final ProductErrorDefinition NON_STRING_MAP_KEY =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("path", "keyType"));

    private RowValueErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
