package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class TransformErrorDefinitions {
    public static final ProductErrorDefinition EXPRESSION_FAILED =
            new ProductErrorDefinition(ProductErrorModule.TRANSFORM, 1, Severity.ERROR, Set.of("expr", "detail"));
    public static final ProductErrorDefinition SCRIPT_COMPILE_FAILED =
            new ProductErrorDefinition(ProductErrorModule.TRANSFORM, 2, Severity.ERROR, Set.of("detail"));
    public static final ProductErrorDefinition SCRIPT_DECIMAL_OUT_OF_RANGE =
            new ProductErrorDefinition(ProductErrorModule.TRANSFORM, 3, Severity.ERROR, Set.of("value"));
    public static final ProductErrorDefinition SCRIPT_NO_PROCESS =
            new ProductErrorDefinition(ProductErrorModule.TRANSFORM, 4, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition SCRIPT_FAILED =
            new ProductErrorDefinition(ProductErrorModule.TRANSFORM, 5, Severity.ERROR, Set.of("detail"));
    public static final ProductErrorDefinition SCRIPT_OUTPUT_INVALID =
            new ProductErrorDefinition(ProductErrorModule.TRANSFORM, 6, Severity.ERROR, Set.of("detail"));
    public static final ProductErrorDefinition UNWIND_NEEDS_A_COMPLETE_BEFORE_IMAGE =
            new ProductErrorDefinition(ProductErrorModule.TRANSFORM, 7, Severity.ERROR, Set.of("path", "detail"));
    public static final ProductErrorDefinition UNWIND_ROWS_SHARE_A_KEY =
            new ProductErrorDefinition(ProductErrorModule.TRANSFORM, 8, Severity.WARNING, Set.of("path", "key"));

    private TransformErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(EXPRESSION_FAILED, SCRIPT_COMPILE_FAILED, SCRIPT_DECIMAL_OUT_OF_RANGE, SCRIPT_NO_PROCESS, SCRIPT_FAILED, SCRIPT_OUTPUT_INVALID, UNWIND_NEEDS_A_COMPLETE_BEFORE_IMAGE, UNWIND_ROWS_SHARE_A_KEY);
    }
}
