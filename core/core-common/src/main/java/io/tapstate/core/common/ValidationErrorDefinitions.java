package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Validation refusals preserve cancellation, incomplete reads and ownership boundaries. */
public final class ValidationErrorDefinitions {
    public static final ProductErrorDefinition SCHEDULE_FIELD_COUNT =
            new ProductErrorDefinition(ProductErrorModule.VALIDATION, 1, Severity.ERROR, Set.of("count"));
    public static final ProductErrorDefinition SCHEDULE_INVALID =
            new ProductErrorDefinition(ProductErrorModule.VALIDATION, 2, Severity.ERROR, Set.of("field", "position", "value"));
    public static final ProductErrorDefinition SCHEDULE_NO_DATE =
            new ProductErrorDefinition(ProductErrorModule.VALIDATION, 3, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition SCHEDULE_SEARCH_EXHAUSTED =
            new ProductErrorDefinition(ProductErrorModule.VALIDATION, 4, Severity.ERROR, Set.of("limit"));
    public static final ProductErrorDefinition EXECUTION_CANCELLED =
            new ProductErrorDefinition(ProductErrorModule.VALIDATION, 5, Severity.ERROR, Set.of("node"));
    public static final ProductErrorDefinition SOURCE_FIELD_CONFLICT =
            new ProductErrorDefinition(ProductErrorModule.VALIDATION, 6, Severity.ERROR, Set.of("node", "field"));
    public static final ProductErrorDefinition RULE_UNSUPPORTED =
            new ProductErrorDefinition(ProductErrorModule.VALIDATION, 7, Severity.ERROR, Set.of("node", "field", "rule"));
    public static final ProductErrorDefinition INVALID_CONTRACT =
            new ProductErrorDefinition(ProductErrorModule.VALIDATION, 8, Severity.ERROR, Set.of("field"));
    public static final ProductErrorDefinition KEY_LIMIT_EXCEEDED =
            new ProductErrorDefinition(ProductErrorModule.VALIDATION, 9, Severity.ERROR, Set.of("limit"));
    public static final ProductErrorDefinition INCOMPATIBLE_KEY =
            new ProductErrorDefinition(ProductErrorModule.VALIDATION, 10, Severity.ERROR, Set.of("component"));
    public static final ProductErrorDefinition ILLEGAL_TRANSITION =
            new ProductErrorDefinition(ProductErrorModule.VALIDATION, 11, Severity.ERROR, Set.of("from", "to", "mode"));
    public static final ProductErrorDefinition VALUE_LIMIT_EXCEEDED =
            new ProductErrorDefinition(ProductErrorModule.VALIDATION, 12, Severity.ERROR, Set.of("limit"));
    public static final ProductErrorDefinition VALUE_TYPE_UNSUPPORTED =
            new ProductErrorDefinition(ProductErrorModule.VALIDATION, 13, Severity.ERROR, Set.of("type"));
    public static final ProductErrorDefinition OWNER_NOT_AUTHORIZED =
            new ProductErrorDefinition(ProductErrorModule.VALIDATION, 14, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition READ_FAILED =
            new ProductErrorDefinition(ProductErrorModule.VALIDATION, 15, Severity.ERROR, Set.of("table"));
    public static final ProductErrorDefinition READ_PROTOCOL_VIOLATION =
            new ProductErrorDefinition(ProductErrorModule.VALIDATION, 16, Severity.ERROR, Set.of("table"));
    public static final ProductErrorDefinition READ_CANCELLED =
            new ProductErrorDefinition(ProductErrorModule.VALIDATION, 17, Severity.ERROR, Set.of("table"));
    public static final ProductErrorDefinition CODEC_FAILED =
            new ProductErrorDefinition(ProductErrorModule.VALIDATION, 18, Severity.ERROR, Set.of("connector", "field"));
    public static final ProductErrorDefinition CODEC_VALUE_UNAVAILABLE =
            new ProductErrorDefinition(ProductErrorModule.VALIDATION, 19, Severity.ERROR, Set.of("connector", "field"));

    private ValidationErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(SCHEDULE_FIELD_COUNT, SCHEDULE_INVALID, SCHEDULE_NO_DATE, SCHEDULE_SEARCH_EXHAUSTED, EXECUTION_CANCELLED, SOURCE_FIELD_CONFLICT, RULE_UNSUPPORTED, INVALID_CONTRACT, KEY_LIMIT_EXCEEDED, INCOMPATIBLE_KEY, ILLEGAL_TRANSITION, VALUE_LIMIT_EXCEEDED, VALUE_TYPE_UNSUPPORTED, OWNER_NOT_AUTHORIZED, READ_FAILED, READ_PROTOCOL_VIOLATION, READ_CANCELLED, CODEC_FAILED, CODEC_VALUE_UNAVAILABLE);
    }
}
