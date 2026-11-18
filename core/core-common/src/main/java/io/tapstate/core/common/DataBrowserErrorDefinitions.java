package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class DataBrowserErrorDefinitions {
    public static final ProductErrorDefinition UNKNOWN_COLLECTION =
            new ProductErrorDefinition(ProductErrorModule.DATA_BROWSER, 1, Severity.ERROR, Set.of("source", "collection"));
    public static final ProductErrorDefinition INVALID_LIMIT =
            new ProductErrorDefinition(ProductErrorModule.DATA_BROWSER, 2, Severity.ERROR, Set.of("limit", "max"));
    public static final ProductErrorDefinition UNORDERABLE_FIELD =
            new ProductErrorDefinition(ProductErrorModule.DATA_BROWSER, 3, Severity.ERROR, Set.of("field"));
    public static final ProductErrorDefinition CONNECTOR_NOT_BROWSABLE =
            new ProductErrorDefinition(ProductErrorModule.DATA_BROWSER, 4, Severity.ERROR, Set.of("connector", "browsable"));
    public static final ProductErrorDefinition SOURCE_DELETED =
            new ProductErrorDefinition(ProductErrorModule.DATA_BROWSER, 5, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition FOLLOW_STOPPED =
            new ProductErrorDefinition(ProductErrorModule.DATA_BROWSER, 6, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition FOLLOW_IDLE =
            new ProductErrorDefinition(ProductErrorModule.DATA_BROWSER, 7, Severity.ERROR, Set.of());

    private DataBrowserErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(UNKNOWN_COLLECTION, INVALID_LIMIT, UNORDERABLE_FIELD, CONNECTOR_NOT_BROWSABLE, SOURCE_DELETED, FOLLOW_STOPPED, FOLLOW_IDLE);
    }
}
