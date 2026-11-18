package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class RowValueErrorDefinitions {
    public static final ProductErrorDefinition NON_STRING_MAP_KEY =
            new ProductErrorDefinition(ProductErrorModule.ROW_VALUE, 1, Severity.ERROR, Set.of("path", "keyType"));

    private RowValueErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(NON_STRING_MAP_KEY);
    }
}
