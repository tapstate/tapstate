package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class ConnectorCatalogErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.CONNECTOR_CATALOG);

    public static final ProductErrorDefinition NOT_FOUND =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("connector"));

    private ConnectorCatalogErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
