package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared request-contract definitions used by both service and independent client producers. */
public final class RequestErrorDefinitions {
    public static final ProductErrorDefinition MALFORMED_REQUEST =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 1, Severity.ERROR, Set.of("reason"));
    public static final ProductErrorDefinition AUTH_FAILED =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 2, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition UNAUTHENTICATED =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 3, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition FORBIDDEN =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 4, Severity.ERROR, Set.of("op", "required"));
    public static final ProductErrorDefinition UNREACHABLE =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 5, Severity.ERROR, Set.of());

    private RequestErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(MALFORMED_REQUEST, AUTH_FAILED, UNAUTHENTICATED, FORBIDDEN, UNREACHABLE);
    }
}
