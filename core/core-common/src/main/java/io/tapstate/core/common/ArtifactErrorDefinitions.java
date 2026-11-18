package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Refusal and partial-cleanup outcomes remain distinct across transport surfaces. */
public final class ArtifactErrorDefinitions {
    public static final ProductErrorDefinition NOT_FOUND =
            new ProductErrorDefinition(ProductErrorModule.ARTIFACT, 1, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition PRECONDITION_REQUIRED =
            new ProductErrorDefinition(ProductErrorModule.ARTIFACT, 2, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition VERSION_CONFLICT =
            new ProductErrorDefinition(ProductErrorModule.ARTIFACT, 3, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition IN_USE =
            new ProductErrorDefinition(ProductErrorModule.ARTIFACT, 4, Severity.ERROR, Set.of("id", "referrers"));
    public static final ProductErrorDefinition PIPELINE_NOT_STOPPED =
            new ProductErrorDefinition(ProductErrorModule.ARTIFACT, 5, Severity.ERROR, Set.of("id", "actual", "desired"));
    public static final ProductErrorDefinition RECLAIM_INCOMPLETE =
            new ProductErrorDefinition(ProductErrorModule.ARTIFACT, 6, Severity.ERROR, Set.of("id", "reason", "residue"));

    private ArtifactErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(NOT_FOUND, PRECONDITION_REQUIRED, VERSION_CONFLICT, IN_USE, PIPELINE_NOT_STOPPED, RECLAIM_INCOMPLETE);
    }
}
