package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Runtime diagnostics retain the original outcome and ownership boundaries. */
public final class CaptureErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.CAPTURE);

    public static final ProductErrorDefinition OBSERVER_FAILED =
            DEFINITIONS.define(1, Severity.WARNING, Set.of("captureId", "openingId", "phase"));
    public static final ProductErrorDefinition STILL_ENDING =
            DEFINITIONS.define(2, Severity.ERROR, Set.of("captureId"));
    public static final ProductErrorDefinition CLAIM_LOST =
            DEFINITIONS.define(3, Severity.ERROR, Set.of("captureId"));
    public static final ProductErrorDefinition RECOVERY_LOG_GAP =
            DEFINITIONS.define(4, Severity.ERROR, Set.of("ring", "sequence", "reason"));
    public static final ProductErrorDefinition RECOVERY_PROGRESS_UNPROVEN =
            DEFINITIONS.define(5, Severity.ERROR, Set.of("pipeline", "source"));
    public static final ProductErrorDefinition SRS_NOT_RECOVERABLE =
            DEFINITIONS.define(6, Severity.ERROR, Set.of("chain"));
    public static final ProductErrorDefinition NO_RING_TO_ATTACH =
            DEFINITIONS.define(7, Severity.ERROR, Set.of("captureId", "seconds"));
    public static final ProductErrorDefinition CLUSTER_REFUSED_WRITES =
            DEFINITIONS.define(8, Severity.ERROR, Set.of("table", "seconds"));
    public static final ProductErrorDefinition CLUSTER_REFUSED_THE_READ =
            DEFINITIONS.define(9, Severity.ERROR, Set.of("ring", "seconds"));
    public static final ProductErrorDefinition START_FROM_UNPARSABLE =
            DEFINITIONS.define(10, Severity.ERROR, Set.of("value"));
    public static final ProductErrorDefinition START_FROM_OUTSIDE_WINDOW =
            DEFINITIONS.define(11, Severity.ERROR, Set.of("requested", "earliest", "retention"));
    public static final ProductErrorDefinition EVENT_TABLE_NOT_SELECTED =
            DEFINITIONS.define(12, Severity.ERROR, Set.of("table"));
    public static final ProductErrorDefinition SNAPSHOT_REPORTS_NO_SEAM =
            DEFINITIONS.define(13, Severity.ERROR, Set.of("chain"));

    private CaptureErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
