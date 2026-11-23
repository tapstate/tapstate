package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class DataBrowserErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.DATA_BROWSER);

    public static final ProductErrorDefinition UNKNOWN_COLLECTION =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("source", "collection"));
    public static final ProductErrorDefinition INVALID_LIMIT =
            DEFINITIONS.define(2, Severity.ERROR, Set.of("limit", "max"));
    public static final ProductErrorDefinition UNORDERABLE_FIELD =
            DEFINITIONS.define(3, Severity.ERROR, Set.of("field"));
    public static final ProductErrorDefinition CONNECTOR_NOT_BROWSABLE =
            DEFINITIONS.define(4, Severity.ERROR, Set.of("connector", "browsable"));
    public static final ProductErrorDefinition SOURCE_DELETED =
            DEFINITIONS.define(5, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition FOLLOW_STOPPED =
            DEFINITIONS.define(6, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition FOLLOW_IDLE =
            DEFINITIONS.define(7, Severity.ERROR, Set.of());

    private DataBrowserErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
