package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class TransformErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.TRANSFORM);

    public static final ProductErrorDefinition EXPRESSION_FAILED =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("expr", "detail"));
    public static final ProductErrorDefinition SCRIPT_COMPILE_FAILED =
            DEFINITIONS.define(2, Severity.ERROR, Set.of("detail"));
    public static final ProductErrorDefinition SCRIPT_DECIMAL_OUT_OF_RANGE =
            DEFINITIONS.define(3, Severity.ERROR, Set.of("value"));
    public static final ProductErrorDefinition SCRIPT_NO_PROCESS =
            DEFINITIONS.define(4, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition SCRIPT_FAILED =
            DEFINITIONS.define(5, Severity.ERROR, Set.of("detail"));
    public static final ProductErrorDefinition SCRIPT_OUTPUT_INVALID =
            DEFINITIONS.define(6, Severity.ERROR, Set.of("detail"));
    public static final ProductErrorDefinition UNWIND_NEEDS_A_COMPLETE_BEFORE_IMAGE =
            DEFINITIONS.define(7, Severity.ERROR, Set.of("path", "detail"));
    public static final ProductErrorDefinition UNWIND_ROWS_SHARE_A_KEY =
            DEFINITIONS.define(8, Severity.WARNING, Set.of("path", "key"));

    private TransformErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
