package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Runtime diagnostics retain the original outcome and ownership boundaries. */
public final class AlertErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.ALERT);

    public static final ProductErrorDefinition INVALID_CONFIGURATION =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("field", "reason"));
    public static final ProductErrorDefinition CHANNEL_NOT_ACCEPTED =
            DEFINITIONS.define(2, Severity.ERROR, Set.of("channel", "phase"));
    public static final ProductErrorDefinition CHANNEL_RESULT_UNKNOWN =
            DEFINITIONS.define(3, Severity.ERROR, Set.of("channel", "phase"));
    public static final ProductErrorDefinition CHANNEL_UNAVAILABLE =
            DEFINITIONS.define(4, Severity.ERROR, Set.of("channel", "phase"));
    public static final ProductErrorDefinition COMMAND_CONFLICT =
            DEFINITIONS.define(5, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition COMMAND_EXPIRED =
            DEFINITIONS.define(6, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition COMMAND_NOT_YET_VALID =
            DEFINITIONS.define(7, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition STALE_ACTION =
            DEFINITIONS.define(8, Severity.ERROR, Set.of("action"));

    private AlertErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
