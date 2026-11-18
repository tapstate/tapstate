package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class PositionErrorDefinitions {
    public static final ProductErrorDefinition WRITE_BACK_WHILE_LIVE =
            new ProductErrorDefinition(ProductErrorModule.POSITION, 1, Severity.ERROR, Set.of("chain", "pipelines"));
    public static final ProductErrorDefinition CHAIN_NOT_READ =
            new ProductErrorDefinition(ProductErrorModule.POSITION, 2, Severity.ERROR, Set.of("chain", "pipeline", "known"));
    public static final ProductErrorDefinition FIELD_NOT_EDITABLE =
            new ProductErrorDefinition(ProductErrorModule.POSITION, 3, Severity.ERROR, Set.of("field", "chain"));
    public static final ProductErrorDefinition NOTHING_TO_WRITE =
            new ProductErrorDefinition(ProductErrorModule.POSITION, 4, Severity.ERROR, Set.of("pipeline"));

    private PositionErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(WRITE_BACK_WHILE_LIVE, CHAIN_NOT_READ, FIELD_NOT_EDITABLE, NOTHING_TO_WRITE);
    }
}
