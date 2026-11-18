package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class EngineErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.ENGINE);

    public static final ProductErrorDefinition NO_SUCH_JOB =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition FRONTIER_ORDER_NOT_ENCODABLE =
            DEFINITIONS.define(2, Severity.ERROR, Set.of("chain", "epoch", "seq"));
    public static final ProductErrorDefinition EXECUTION_NOT_AUTHORIZED =
            DEFINITIONS.define(3, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition EXECUTION_LEASE_LOST =
            DEFINITIONS.define(4, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition EXECUTION_RENEWAL_FAILED =
            DEFINITIONS.define(5, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition JOB_STILL_ENDING =
            DEFINITIONS.define(6, Severity.ERROR, Set.of("pipeline", "seconds"));
    public static final ProductErrorDefinition VIEW_KEY_MISSING_FROM_BEFORE_IMAGE =
            DEFINITIONS.define(7, Severity.ERROR, Set.of("view", "key", "operation"));
    public static final ProductErrorDefinition JOB_FAILED =
            DEFINITIONS.define(8, Severity.ERROR, Set.of("pipeline", "cause"));
    public static final ProductErrorDefinition OUT_OF_MEMORY =
            DEFINITIONS.define(9, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition FRONTIER_PINNED =
            DEFINITIONS.define(10, Severity.WARNING, Set.of("chain", "minutes", "gap", "cause"));
    public static final ProductErrorDefinition JOIN_DIMENSION_ROW_DISPLACED =
            DEFINITIONS.define(11, Severity.WARNING, Set.of("source", "key"));
    public static final ProductErrorDefinition ROUTING_KEY_MISSING =
            DEFINITIONS.define(12, Severity.ERROR, Set.of("node", "stream", "columns"));
    public static final ProductErrorDefinition KEY_CHANGE_ON_PARALLEL_NODE =
            DEFINITIONS.define(13, Severity.ERROR, Set.of("node", "stream"));
    public static final ProductErrorDefinition MEMBERSHIP_CHANGED_BEFORE_START =
            DEFINITIONS.define(14, Severity.ERROR, Set.of("pipeline", "planned", "actual"));
    public static final ProductErrorDefinition CLUSTER_REFUSED_THE_STATE =
            DEFINITIONS.define(15, Severity.ERROR, Set.of("state", "seconds"));

    private EngineErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
