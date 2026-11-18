package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Report text only; each finding behavior determines whether a start may proceed. */
public final class StartCheckDefinitions {
    public static final ProductErrorDefinition TARGET_IDENTITY_AUTOMATIC =
            new ProductErrorDefinition(ProductErrorModule.START_CHECK, 1, Severity.WARNING, Set.of("target", "connection", "field", "preserved", "key"));
    public static final ProductErrorDefinition TARGET_IDENTITY_COMPATIBLE =
            new ProductErrorDefinition(ProductErrorModule.START_CHECK, 2, Severity.WARNING, Set.of("target", "connection", "key"));
    public static final ProductErrorDefinition TARGET_IDENTITY_REFUSED =
            new ProductErrorDefinition(ProductErrorModule.START_CHECK, 3, Severity.WARNING, Set.of("target", "connection", "reason", "field", "preserved"));
    public static final ProductErrorDefinition TARGET_EMPTY =
            new ProductErrorDefinition(ProductErrorModule.START_CHECK, 4, Severity.WARNING, Set.of("target", "connection"));
    public static final ProductErrorDefinition TARGET_SET_TO_CLEAR =
            new ProductErrorDefinition(ProductErrorModule.START_CHECK, 5, Severity.WARNING, Set.of("target", "connection", "holding"));
    public static final ProductErrorDefinition TARGET_NOT_EMPTY =
            new ProductErrorDefinition(ProductErrorModule.START_CHECK, 6, Severity.WARNING, Set.of("target", "connection", "holding"));
    public static final ProductErrorDefinition TARGET_NOT_EMPTY_SHARED =
            new ProductErrorDefinition(ProductErrorModule.START_CHECK, 7, Severity.WARNING, Set.of("target", "connection", "holding", "definition"));
    public static final ProductErrorDefinition TARGET_NOT_EMPTY_REFUSED =
            new ProductErrorDefinition(ProductErrorModule.START_CHECK, 8, Severity.WARNING, Set.of("target", "connection", "holding"));
    public static final ProductErrorDefinition TARGET_ROWS_UNKNOWN =
            new ProductErrorDefinition(ProductErrorModule.START_CHECK, 9, Severity.WARNING, Set.of("target", "connection", "reason"));
    public static final ProductErrorDefinition UNAVAILABLE =
            new ProductErrorDefinition(ProductErrorModule.START_CHECK, 10, Severity.WARNING, Set.of("check", "reason"));
    public static final ProductErrorDefinition CLEAR_BEFORE_FULL_LOAD =
            new ProductErrorDefinition(ProductErrorModule.START_CHECK, 11, Severity.WARNING, Set.of("target", "element"));
    public static final ProductErrorDefinition KEEP_EXISTING_ROWS =
            new ProductErrorDefinition(ProductErrorModule.START_CHECK, 12, Severity.WARNING, Set.of("target"));

    private StartCheckDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(TARGET_IDENTITY_AUTOMATIC, TARGET_IDENTITY_COMPATIBLE, TARGET_IDENTITY_REFUSED, TARGET_EMPTY, TARGET_SET_TO_CLEAR, TARGET_NOT_EMPTY, TARGET_NOT_EMPTY_SHARED, TARGET_NOT_EMPTY_REFUSED, TARGET_ROWS_UNKNOWN, UNAVAILABLE, CLEAR_BEFORE_FULL_LOAD, KEEP_EXISTING_ROWS);
    }
}
