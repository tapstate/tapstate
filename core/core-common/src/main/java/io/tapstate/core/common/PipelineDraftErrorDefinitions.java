package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Conditional draft mutation contracts; revisions and publication fences are unchanged. */
public final class PipelineDraftErrorDefinitions {
    public static final ProductErrorDefinition NOT_FOUND =
            new ProductErrorDefinition(ProductErrorModule.PIPELINE_DRAFT, 1, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition ALREADY_EXISTS =
            new ProductErrorDefinition(ProductErrorModule.PIPELINE_DRAFT, 2, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition PRECONDITION_REQUIRED =
            new ProductErrorDefinition(ProductErrorModule.PIPELINE_DRAFT, 3, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition REVISION_CONFLICT =
            new ProductErrorDefinition(ProductErrorModule.PIPELINE_DRAFT, 4, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition INVALID =
            new ProductErrorDefinition(ProductErrorModule.PIPELINE_DRAFT, 5, Severity.ERROR, Set.of("id", "reason"));
    public static final ProductErrorDefinition MODE_CONFLICT =
            new ProductErrorDefinition(ProductErrorModule.PIPELINE_DRAFT, 6, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition ARTIFACT_CONFLICT =
            new ProductErrorDefinition(ProductErrorModule.PIPELINE_DRAFT, 7, Severity.ERROR, Set.of("id"));

    private PipelineDraftErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(NOT_FOUND, ALREADY_EXISTS, PRECONDITION_REQUIRED, REVISION_CONFLICT, INVALID,
                MODE_CONFLICT, ARTIFACT_CONFLICT);
    }
}
