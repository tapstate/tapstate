package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Report text only; each finding behavior determines whether a start may proceed. */
public final class StartCheckDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.START_CHECK);

    public static final ProductErrorDefinition TARGET_IDENTITY_AUTOMATIC =
            DEFINITIONS.define(1, Severity.WARNING, Set.of("target", "connection", "field", "preserved", "key"));
    public static final ProductErrorDefinition TARGET_IDENTITY_COMPATIBLE =
            DEFINITIONS.define(2, Severity.WARNING, Set.of("target", "connection", "key"));
    public static final ProductErrorDefinition TARGET_IDENTITY_REFUSED =
            DEFINITIONS.define(3, Severity.WARNING, Set.of("target", "connection", "reason", "field", "preserved"));
    public static final ProductErrorDefinition TARGET_EMPTY =
            DEFINITIONS.define(4, Severity.WARNING, Set.of("target", "connection"));
    public static final ProductErrorDefinition TARGET_SET_TO_CLEAR =
            DEFINITIONS.define(5, Severity.WARNING, Set.of("target", "connection", "holding"));
    public static final ProductErrorDefinition TARGET_NOT_EMPTY =
            DEFINITIONS.define(6, Severity.WARNING, Set.of("target", "connection", "holding"));
    public static final ProductErrorDefinition TARGET_NOT_EMPTY_SHARED =
            DEFINITIONS.define(7, Severity.WARNING, Set.of("target", "connection", "holding", "definition"));
    public static final ProductErrorDefinition TARGET_NOT_EMPTY_REFUSED =
            DEFINITIONS.define(8, Severity.WARNING, Set.of("target", "connection", "holding"));
    public static final ProductErrorDefinition TARGET_ROWS_UNKNOWN =
            DEFINITIONS.define(9, Severity.WARNING, Set.of("target", "connection", "reason"));
    public static final ProductErrorDefinition UNAVAILABLE =
            DEFINITIONS.define(10, Severity.WARNING, Set.of("check", "reason"));
    public static final ProductErrorDefinition CLEAR_BEFORE_FULL_LOAD =
            DEFINITIONS.define(11, Severity.WARNING, Set.of("target", "element"));
    public static final ProductErrorDefinition KEEP_EXISTING_ROWS =
            DEFINITIONS.define(12, Severity.WARNING, Set.of("target"));

    private StartCheckDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
