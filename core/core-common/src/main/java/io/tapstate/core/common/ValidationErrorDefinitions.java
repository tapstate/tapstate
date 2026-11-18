package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Validation refusals preserve cancellation, incomplete reads and ownership boundaries. */
public final class ValidationErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.VALIDATION);

    public static final ProductErrorDefinition SCHEDULE_FIELD_COUNT =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("count"));
    public static final ProductErrorDefinition SCHEDULE_INVALID =
            DEFINITIONS.define(2, Severity.ERROR, Set.of("field", "position", "value"));
    public static final ProductErrorDefinition SCHEDULE_NO_DATE =
            DEFINITIONS.define(3, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition SCHEDULE_SEARCH_EXHAUSTED =
            DEFINITIONS.define(4, Severity.ERROR, Set.of("limit"));
    public static final ProductErrorDefinition EXECUTION_CANCELLED =
            DEFINITIONS.define(5, Severity.ERROR, Set.of("node"));
    public static final ProductErrorDefinition SOURCE_FIELD_CONFLICT =
            DEFINITIONS.define(6, Severity.ERROR, Set.of("node", "field"));
    public static final ProductErrorDefinition RULE_UNSUPPORTED =
            DEFINITIONS.define(7, Severity.ERROR, Set.of("node", "field", "rule"));
    public static final ProductErrorDefinition INVALID_CONTRACT =
            DEFINITIONS.define(8, Severity.ERROR, Set.of("field"));
    public static final ProductErrorDefinition KEY_LIMIT_EXCEEDED =
            DEFINITIONS.define(9, Severity.ERROR, Set.of("limit"));
    public static final ProductErrorDefinition INCOMPATIBLE_KEY =
            DEFINITIONS.define(10, Severity.ERROR, Set.of("component"));
    public static final ProductErrorDefinition ILLEGAL_TRANSITION =
            DEFINITIONS.define(11, Severity.ERROR, Set.of("from", "to", "mode"));
    public static final ProductErrorDefinition VALUE_LIMIT_EXCEEDED =
            DEFINITIONS.define(12, Severity.ERROR, Set.of("limit"));
    public static final ProductErrorDefinition VALUE_TYPE_UNSUPPORTED =
            DEFINITIONS.define(13, Severity.ERROR, Set.of("type"));
    public static final ProductErrorDefinition OWNER_NOT_AUTHORIZED =
            DEFINITIONS.define(14, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition READ_FAILED =
            DEFINITIONS.define(15, Severity.ERROR, Set.of("table"));
    public static final ProductErrorDefinition READ_PROTOCOL_VIOLATION =
            DEFINITIONS.define(16, Severity.ERROR, Set.of("table"));
    public static final ProductErrorDefinition READ_CANCELLED =
            DEFINITIONS.define(17, Severity.ERROR, Set.of("table"));
    public static final ProductErrorDefinition CODEC_FAILED =
            DEFINITIONS.define(18, Severity.ERROR, Set.of("connector", "field"));
    public static final ProductErrorDefinition CODEC_VALUE_UNAVAILABLE =
            DEFINITIONS.define(19, Severity.ERROR, Set.of("connector", "field"));

    private ValidationErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
