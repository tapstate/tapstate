package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Conditional draft mutation contracts; revisions and publication fences are unchanged. */
public final class PipelineDraftErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.PIPELINE_DRAFT);

    public static final ProductErrorDefinition NOT_FOUND =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition ALREADY_EXISTS =
            DEFINITIONS.define(2, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition PRECONDITION_REQUIRED =
            DEFINITIONS.define(3, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition REVISION_CONFLICT =
            DEFINITIONS.define(4, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition INVALID =
            DEFINITIONS.define(5, Severity.ERROR, Set.of("id", "reason"));
    public static final ProductErrorDefinition MODE_CONFLICT =
            DEFINITIONS.define(6, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition ARTIFACT_CONFLICT =
            DEFINITIONS.define(7, Severity.ERROR, Set.of("id"));

    private PipelineDraftErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
