package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Explicit lifecycle refusals; start-check reports and decision semantics remain separate. */
public final class LifecycleErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.LIFECYCLE);

    public static final ProductErrorDefinition ILLEGAL_TRANSITION =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("from", "verb"));
    public static final ProductErrorDefinition INCOMPATIBLE_REVISION =
            DEFINITIONS.define(2, Severity.ERROR, Set.of("requested", "latest"));
    public static final ProductErrorDefinition PURGE_STATE_NOT_STATED =
            DEFINITIONS.define(3, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition PIPELINE_NOT_RUNNABLE =
            DEFINITIONS.define(4, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition UNKNOWN_PIPELINE =
            DEFINITIONS.define(5, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition START_NEEDS_CONFIRMATION =
            DEFINITIONS.define(6, Severity.ERROR, Set.of("pipeline", "count", "check", "subject"));
    public static final ProductErrorDefinition START_BLOCKED =
            DEFINITIONS.define(7, Severity.ERROR, Set.of("pipeline", "count", "check", "subject"));
    public static final ProductErrorDefinition INVALID_START_DECISION =
            DEFINITIONS.define(8, Severity.ERROR, Set.of("pipeline", "finding", "action"));

    private LifecycleErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
