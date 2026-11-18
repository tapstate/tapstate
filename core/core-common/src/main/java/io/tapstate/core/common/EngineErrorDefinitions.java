package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class EngineErrorDefinitions {
    public static final ProductErrorDefinition NO_SUCH_JOB =
            new ProductErrorDefinition(ProductErrorModule.ENGINE, 1, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition FRONTIER_ORDER_NOT_ENCODABLE =
            new ProductErrorDefinition(ProductErrorModule.ENGINE, 2, Severity.ERROR, Set.of("chain", "epoch", "seq"));
    public static final ProductErrorDefinition EXECUTION_NOT_AUTHORIZED =
            new ProductErrorDefinition(ProductErrorModule.ENGINE, 3, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition EXECUTION_LEASE_LOST =
            new ProductErrorDefinition(ProductErrorModule.ENGINE, 4, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition EXECUTION_RENEWAL_FAILED =
            new ProductErrorDefinition(ProductErrorModule.ENGINE, 5, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition JOB_STILL_ENDING =
            new ProductErrorDefinition(ProductErrorModule.ENGINE, 6, Severity.ERROR, Set.of("pipeline", "seconds"));
    public static final ProductErrorDefinition VIEW_KEY_MISSING_FROM_BEFORE_IMAGE =
            new ProductErrorDefinition(ProductErrorModule.ENGINE, 7, Severity.ERROR, Set.of("view", "key", "operation"));
    public static final ProductErrorDefinition JOB_FAILED =
            new ProductErrorDefinition(ProductErrorModule.ENGINE, 8, Severity.ERROR, Set.of("pipeline", "cause"));
    public static final ProductErrorDefinition OUT_OF_MEMORY =
            new ProductErrorDefinition(ProductErrorModule.ENGINE, 9, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition FRONTIER_PINNED =
            new ProductErrorDefinition(ProductErrorModule.ENGINE, 10, Severity.WARNING, Set.of("chain", "minutes", "gap", "cause"));
    public static final ProductErrorDefinition JOIN_DIMENSION_ROW_DISPLACED =
            new ProductErrorDefinition(ProductErrorModule.ENGINE, 11, Severity.WARNING, Set.of("source", "key"));
    public static final ProductErrorDefinition ROUTING_KEY_MISSING =
            new ProductErrorDefinition(ProductErrorModule.ENGINE, 12, Severity.ERROR, Set.of("node", "stream", "columns"));
    public static final ProductErrorDefinition KEY_CHANGE_ON_PARALLEL_NODE =
            new ProductErrorDefinition(ProductErrorModule.ENGINE, 13, Severity.ERROR, Set.of("node", "stream"));
    public static final ProductErrorDefinition MEMBERSHIP_CHANGED_BEFORE_START =
            new ProductErrorDefinition(ProductErrorModule.ENGINE, 14, Severity.ERROR, Set.of("pipeline", "planned", "actual"));
    public static final ProductErrorDefinition CLUSTER_REFUSED_THE_STATE =
            new ProductErrorDefinition(ProductErrorModule.ENGINE, 15, Severity.ERROR, Set.of("state", "seconds"));

    private EngineErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(NO_SUCH_JOB, FRONTIER_ORDER_NOT_ENCODABLE, EXECUTION_NOT_AUTHORIZED, EXECUTION_LEASE_LOST, EXECUTION_RENEWAL_FAILED, JOB_STILL_ENDING, VIEW_KEY_MISSING_FROM_BEFORE_IMAGE, JOB_FAILED, OUT_OF_MEMORY, FRONTIER_PINNED, JOIN_DIMENSION_ROW_DISPLACED, ROUTING_KEY_MISSING, KEY_CHANGE_ON_PARALLEL_NODE, MEMBERSHIP_CHANGED_BEFORE_START, CLUSTER_REFUSED_THE_STATE);
    }
}
