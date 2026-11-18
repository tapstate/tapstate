package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Runtime diagnostics retain the original outcome and ownership boundaries. */
public final class AlertErrorDefinitions {
    public static final ProductErrorDefinition INVALID_CONFIGURATION =
            new ProductErrorDefinition(ProductErrorModule.ALERT, 1, Severity.ERROR, Set.of("field", "reason"));
    public static final ProductErrorDefinition CHANNEL_NOT_ACCEPTED =
            new ProductErrorDefinition(ProductErrorModule.ALERT, 2, Severity.ERROR, Set.of("channel", "phase"));
    public static final ProductErrorDefinition CHANNEL_RESULT_UNKNOWN =
            new ProductErrorDefinition(ProductErrorModule.ALERT, 3, Severity.ERROR, Set.of("channel", "phase"));
    public static final ProductErrorDefinition CHANNEL_UNAVAILABLE =
            new ProductErrorDefinition(ProductErrorModule.ALERT, 4, Severity.ERROR, Set.of("channel", "phase"));
    public static final ProductErrorDefinition COMMAND_CONFLICT =
            new ProductErrorDefinition(ProductErrorModule.ALERT, 5, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition COMMAND_EXPIRED =
            new ProductErrorDefinition(ProductErrorModule.ALERT, 6, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition COMMAND_NOT_YET_VALID =
            new ProductErrorDefinition(ProductErrorModule.ALERT, 7, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition STALE_ACTION =
            new ProductErrorDefinition(ProductErrorModule.ALERT, 8, Severity.ERROR, Set.of("action"));

    private AlertErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(INVALID_CONFIGURATION, CHANNEL_NOT_ACCEPTED, CHANNEL_RESULT_UNKNOWN, CHANNEL_UNAVAILABLE, COMMAND_CONFLICT, COMMAND_EXPIRED, COMMAND_NOT_YET_VALID, STALE_ACTION);
    }
}
