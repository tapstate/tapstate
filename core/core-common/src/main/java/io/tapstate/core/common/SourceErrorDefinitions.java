package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Explicit Source mutation contracts shared without a control-service dependency. */
public final class SourceErrorDefinitions {
    public static final ProductErrorDefinition NOT_FOUND =
            new ProductErrorDefinition(ProductErrorModule.SOURCE, 1, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition ALREADY_EXISTS =
            new ProductErrorDefinition(ProductErrorModule.SOURCE, 2, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition ID_MISMATCH =
            new ProductErrorDefinition(ProductErrorModule.SOURCE, 3, Severity.ERROR, Set.of("pathId", "bodyId"));
    public static final ProductErrorDefinition PRECONDITION_REQUIRED =
            new ProductErrorDefinition(ProductErrorModule.SOURCE, 4, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition VERSION_CONFLICT =
            new ProductErrorDefinition(ProductErrorModule.SOURCE, 5, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition IN_USE =
            new ProductErrorDefinition(ProductErrorModule.SOURCE, 6, Severity.ERROR, Set.of("id", "referrers"));
    public static final ProductErrorDefinition SRS_CHANGE_WHILE_RUNNING =
            new ProductErrorDefinition(ProductErrorModule.SOURCE, 7, Severity.ERROR, Set.of("id", "pipelines"));

    private SourceErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(NOT_FOUND, ALREADY_EXISTS, ID_MISMATCH, PRECONDITION_REQUIRED, VERSION_CONFLICT,
                IN_USE, SRS_CHANGE_WHILE_RUNNING);
    }
}
