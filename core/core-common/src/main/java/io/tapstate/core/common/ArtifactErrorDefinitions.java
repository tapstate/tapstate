package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Refusal and partial-cleanup outcomes remain distinct across transport surfaces. */
public final class ArtifactErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.ARTIFACT);

    public static final ProductErrorDefinition NOT_FOUND =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition PRECONDITION_REQUIRED =
            DEFINITIONS.define(2, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition VERSION_CONFLICT =
            DEFINITIONS.define(3, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition IN_USE =
            DEFINITIONS.define(4, Severity.ERROR, Set.of("id", "referrers"));
    public static final ProductErrorDefinition PIPELINE_NOT_STOPPED =
            DEFINITIONS.define(5, Severity.ERROR, Set.of("id", "actual", "desired"));
    public static final ProductErrorDefinition RECLAIM_INCOMPLETE =
            DEFINITIONS.define(6, Severity.ERROR, Set.of("id", "reason", "residue"));

    private ArtifactErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
