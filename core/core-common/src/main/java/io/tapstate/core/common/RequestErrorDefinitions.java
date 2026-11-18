package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared request-contract definitions used by both service and independent client producers. */
public final class RequestErrorDefinitions {
    public static final ProductErrorDefinition MALFORMED_REQUEST =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 1, Severity.ERROR, Set.of("reason"));

    private RequestErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() { return List.of(MALFORMED_REQUEST); }
}
