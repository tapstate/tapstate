package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Pipeline artifact and start-change refusal contracts. */
public final class PipelineErrorDefinitions {
    public static final ProductErrorDefinition ALREADY_EXISTS =
            new ProductErrorDefinition(ProductErrorModule.PIPELINE, 1, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition NOT_FOUND =
            new ProductErrorDefinition(ProductErrorModule.PIPELINE, 2, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition ID_MISMATCH =
            new ProductErrorDefinition(ProductErrorModule.PIPELINE, 3, Severity.ERROR, Set.of("pathId", "bodyId"));
    public static final ProductErrorDefinition PRECONDITION_REQUIRED =
            new ProductErrorDefinition(ProductErrorModule.PIPELINE, 4, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition VERSION_CONFLICT =
            new ProductErrorDefinition(ProductErrorModule.PIPELINE, 5, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition START_CHANGE_NOT_APPLIED =
            new ProductErrorDefinition(ProductErrorModule.PIPELINE, 6, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition DEFINITION_CHANGE_UNAVAILABLE =
            new ProductErrorDefinition(ProductErrorModule.PIPELINE, 7, Severity.ERROR, Set.of("id"));

    private PipelineErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(ALREADY_EXISTS, NOT_FOUND, ID_MISMATCH, PRECONDITION_REQUIRED, VERSION_CONFLICT,
                START_CHANGE_NOT_APPLIED, DEFINITION_CHANGE_UNAVAILABLE);
    }
}
