package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Explicit lifecycle refusals; start-check reports and decision semantics remain separate. */
public final class LifecycleErrorDefinitions {
    public static final ProductErrorDefinition ILLEGAL_TRANSITION =
            new ProductErrorDefinition(ProductErrorModule.LIFECYCLE, 1, Severity.ERROR, Set.of("from", "verb"));
    public static final ProductErrorDefinition INCOMPATIBLE_REVISION =
            new ProductErrorDefinition(ProductErrorModule.LIFECYCLE, 2, Severity.ERROR, Set.of("requested", "latest"));
    public static final ProductErrorDefinition PURGE_STATE_NOT_STATED =
            new ProductErrorDefinition(ProductErrorModule.LIFECYCLE, 3, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition PIPELINE_NOT_RUNNABLE =
            new ProductErrorDefinition(ProductErrorModule.LIFECYCLE, 4, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition UNKNOWN_PIPELINE =
            new ProductErrorDefinition(ProductErrorModule.LIFECYCLE, 5, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition START_NEEDS_CONFIRMATION =
            new ProductErrorDefinition(ProductErrorModule.LIFECYCLE, 6, Severity.ERROR, Set.of("pipeline", "count", "check", "subject"));
    public static final ProductErrorDefinition START_BLOCKED =
            new ProductErrorDefinition(ProductErrorModule.LIFECYCLE, 7, Severity.ERROR, Set.of("pipeline", "count", "check", "subject"));
    public static final ProductErrorDefinition INVALID_START_DECISION =
            new ProductErrorDefinition(ProductErrorModule.LIFECYCLE, 8, Severity.ERROR, Set.of("pipeline", "finding", "action"));

    private LifecycleErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(ILLEGAL_TRANSITION, INCOMPATIBLE_REVISION, PURGE_STATE_NOT_STATED, PIPELINE_NOT_RUNNABLE, UNKNOWN_PIPELINE, START_NEEDS_CONFIRMATION, START_BLOCKED, INVALID_START_DECISION);
    }
}
