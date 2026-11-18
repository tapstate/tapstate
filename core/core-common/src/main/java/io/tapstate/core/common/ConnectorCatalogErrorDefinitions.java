package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class ConnectorCatalogErrorDefinitions {
    public static final ProductErrorDefinition NOT_FOUND =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR_CATALOG, 1, Severity.ERROR, Set.of("connector"));

    private ConnectorCatalogErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(NOT_FOUND);
    }
}
