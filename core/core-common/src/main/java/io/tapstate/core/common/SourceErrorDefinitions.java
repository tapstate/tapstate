package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Explicit Source mutation contracts shared without a control-service dependency. */
public final class SourceErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.SOURCE);

    public static final ProductErrorDefinition NOT_FOUND =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition ALREADY_EXISTS =
            DEFINITIONS.define(2, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition ID_MISMATCH =
            DEFINITIONS.define(3, Severity.ERROR, Set.of("pathId", "bodyId"));
    public static final ProductErrorDefinition PRECONDITION_REQUIRED =
            DEFINITIONS.define(4, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition VERSION_CONFLICT =
            DEFINITIONS.define(5, Severity.ERROR, Set.of("id"));
    public static final ProductErrorDefinition IN_USE =
            DEFINITIONS.define(6, Severity.ERROR, Set.of("id", "referrers"));
    public static final ProductErrorDefinition SRS_CHANGE_WHILE_RUNNING =
            DEFINITIONS.define(7, Severity.ERROR, Set.of("id", "pipelines"));

    private SourceErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
