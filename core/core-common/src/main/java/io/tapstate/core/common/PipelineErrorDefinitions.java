package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Pipeline artifact and start-change refusal contracts. */
public final class PipelineErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.PIPELINE);

    public static final ProductErrorDefinition ALREADY_EXISTS =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition NOT_FOUND =
            DEFINITIONS.define(2, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition ID_MISMATCH =
            DEFINITIONS.define(3, Severity.ERROR, Set.of("pathId", "bodyId"));
    public static final ProductErrorDefinition PRECONDITION_REQUIRED =
            DEFINITIONS.define(4, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition VERSION_CONFLICT =
            DEFINITIONS.define(5, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition START_CHANGE_NOT_APPLIED =
            DEFINITIONS.define(6, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition DEFINITION_CHANGE_UNAVAILABLE =
            DEFINITIONS.define(7, Severity.ERROR, Set.of("id"));

    private PipelineErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
