package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared aggregate identities preserve their existing severity and named-parameter contracts. */
public final class AggregateErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.AGGREGATE);

    public static final ProductErrorDefinition INPUT_NOT_SUPPORTED =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("step", "from", "kind"));
    public static final ProductErrorDefinition UPSTREAM_FILTER_UNSAFE =
            DEFINITIONS.define(2, Severity.ERROR, Set.of("step", "filter", "source"));
    public static final ProductErrorDefinition KEY_REQUIRED =
            DEFINITIONS.define(3, Severity.ERROR, Set.of("step", "from"));
    public static final ProductErrorDefinition NAME_CONFLICT =
            DEFINITIONS.define(4, Severity.ERROR, Set.of("step", "name"));
    public static final ProductErrorDefinition EXPRESSION_INVALID =
            DEFINITIONS.define(5, Severity.ERROR, Set.of("step", "field", "part", "reason"));
    public static final ProductErrorDefinition TYPE_UNKNOWN =
            DEFINITIONS.define(6, Severity.ERROR, Set.of("step", "field", "expression"));
    public static final ProductErrorDefinition EVALUATION_FAILED =
            DEFINITIONS.define(7, Severity.ERROR, Set.of("step", "part", "expression", "reason"));
    public static final ProductErrorDefinition OVERFLOW =
            DEFINITIONS.define(8, Severity.ERROR, Set.of("step", "field", "group", "type"));
    public static final ProductErrorDefinition VALUE_TYPE_MISMATCH =
            DEFINITIONS.define(9, Severity.ERROR, Set.of("step", "field", "group", "declared", "actual"));
    public static final ProductErrorDefinition RESULT_NOT_CONVERTIBLE =
            DEFINITIONS.define(10, Severity.ERROR, Set.of("step", "field", "group", "from", "to"));
    public static final ProductErrorDefinition DEFINITION_CHANGED =
            DEFINITIONS.define(11, Severity.ERROR, Set.of("step", "changed"));
    public static final ProductErrorDefinition REBUILD_NEEDS_CLEAR =
            DEFINITIONS.define(12, Severity.ERROR, Set.of("step", "target", "policy"));
    public static final ProductErrorDefinition TOTAL_MISMATCH =
            DEFINITIONS.define(13, Severity.ERROR, Set.of("step", "group", "field"));
    public static final ProductErrorDefinition STEP_NOT_RUNNING =
            DEFINITIONS.define(14, Severity.ERROR, Set.of("pipeline", "step"));
    public static final ProductErrorDefinition GROUP_INVALID =
            DEFINITIONS.define(15, Severity.ERROR, Set.of("step", "reason"));
    public static final ProductErrorDefinition STEP_BUSY =
            DEFINITIONS.define(16, Severity.ERROR, Set.of("pipeline", "step"));
    public static final ProductErrorDefinition STEP_FAILED =
            DEFINITIONS.define(17, Severity.ERROR, Set.of("pipeline", "step", "reason"));
    public static final ProductErrorDefinition TOO_MANY_FIELDS =
            DEFINITIONS.define(18, Severity.ERROR, Set.of("step", "count", "limit"));
    public static final ProductErrorDefinition STATE_ESTIMATE =
            DEFINITIONS.define(19, Severity.WARNING, Set.of("pipeline", "step", "entriesPerRow", "inputRows", "estimatedEntries"));
    public static final ProductErrorDefinition STATE_ESTIMATE_UNSIZED =
            DEFINITIONS.define(20, Severity.WARNING, Set.of("pipeline", "step", "entriesPerRow"));

    private AggregateErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() { return DEFINITIONS.registered(); }
}
