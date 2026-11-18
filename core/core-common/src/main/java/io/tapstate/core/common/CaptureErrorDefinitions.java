package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Runtime diagnostics retain the original outcome and ownership boundaries. */
public final class CaptureErrorDefinitions {
    public static final ProductErrorDefinition OBSERVER_FAILED =
            new ProductErrorDefinition(ProductErrorModule.CAPTURE, 1, Severity.WARNING, Set.of("captureId", "openingId", "phase"));
    public static final ProductErrorDefinition STILL_ENDING =
            new ProductErrorDefinition(ProductErrorModule.CAPTURE, 2, Severity.ERROR, Set.of("captureId"));
    public static final ProductErrorDefinition CLAIM_LOST =
            new ProductErrorDefinition(ProductErrorModule.CAPTURE, 3, Severity.ERROR, Set.of("captureId"));
    public static final ProductErrorDefinition RECOVERY_LOG_GAP =
            new ProductErrorDefinition(ProductErrorModule.CAPTURE, 4, Severity.ERROR, Set.of("ring", "sequence", "reason"));
    public static final ProductErrorDefinition RECOVERY_PROGRESS_UNPROVEN =
            new ProductErrorDefinition(ProductErrorModule.CAPTURE, 5, Severity.ERROR, Set.of("pipeline", "source"));
    public static final ProductErrorDefinition SRS_NOT_RECOVERABLE =
            new ProductErrorDefinition(ProductErrorModule.CAPTURE, 6, Severity.ERROR, Set.of("chain"));
    public static final ProductErrorDefinition NO_RING_TO_ATTACH =
            new ProductErrorDefinition(ProductErrorModule.CAPTURE, 7, Severity.ERROR, Set.of("captureId", "seconds"));
    public static final ProductErrorDefinition CLUSTER_REFUSED_WRITES =
            new ProductErrorDefinition(ProductErrorModule.CAPTURE, 8, Severity.ERROR, Set.of("table", "seconds"));
    public static final ProductErrorDefinition CLUSTER_REFUSED_THE_READ =
            new ProductErrorDefinition(ProductErrorModule.CAPTURE, 9, Severity.ERROR, Set.of("ring", "seconds"));
    public static final ProductErrorDefinition START_FROM_UNPARSABLE =
            new ProductErrorDefinition(ProductErrorModule.CAPTURE, 10, Severity.ERROR, Set.of("value"));
    public static final ProductErrorDefinition START_FROM_OUTSIDE_WINDOW =
            new ProductErrorDefinition(ProductErrorModule.CAPTURE, 11, Severity.ERROR, Set.of("requested", "earliest", "retention"));
    public static final ProductErrorDefinition EVENT_TABLE_NOT_SELECTED =
            new ProductErrorDefinition(ProductErrorModule.CAPTURE, 12, Severity.ERROR, Set.of("table"));
    public static final ProductErrorDefinition SNAPSHOT_REPORTS_NO_SEAM =
            new ProductErrorDefinition(ProductErrorModule.CAPTURE, 13, Severity.ERROR, Set.of("chain"));

    private CaptureErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(OBSERVER_FAILED, STILL_ENDING, CLAIM_LOST, RECOVERY_LOG_GAP, RECOVERY_PROGRESS_UNPROVEN, SRS_NOT_RECOVERABLE, NO_RING_TO_ATTACH, CLUSTER_REFUSED_WRITES, CLUSTER_REFUSED_THE_READ, START_FROM_UNPARSABLE, START_FROM_OUTSIDE_WINDOW, EVENT_TABLE_NOT_SELECTED, SNAPSHOT_REPORTS_NO_SEAM);
    }
}
