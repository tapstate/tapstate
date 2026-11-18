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
    public static final ProductErrorDefinition AI_NOT_CONFIGURED =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 6, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition AI_PROVIDER_AUTH_FAILED =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 7, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition AI_PROVIDER_REQUEST_REJECTED =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 8, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition AI_PROVIDER_RATE_LIMITED =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 9, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition AI_PROVIDER_FAILED =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 10, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition AI_PROVIDER_OUTPUT_LIMIT =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 11, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition AI_INVALID_PROPOSAL =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 12, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition AI_OVERLOADED =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 13, Severity.ERROR, Set.of());

    public static final ProductErrorDefinition PREVIEW_OVERLOADED =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 14, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition AUDIT_BLOCKED =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 15, Severity.ERROR, Set.of("op"));
    public static final ProductErrorDefinition SCHEMA_DERIVATION_INCOMPLETE =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 16, Severity.WARNING, Set.of("pipeline", "causeCode", "causeParams"));
    public static final ProductErrorDefinition BOOTSTRAP_FORBIDDEN =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 17, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition BOOTSTRAP_CLOSED =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 18, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition AUTH_MODE_UNAVAILABLE =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 19, Severity.ERROR, Set.of("mode"));
    public static final ProductErrorDefinition BROWSER_AUTH_CONFIG_INVALID =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 20, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CLOUD_AUTH_UNAVAILABLE =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 21, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition STATE_DATABASE_UNAVAILABLE =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 22, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition SOURCE_CONFIG_REVEAL_UNAVAILABLE =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 23, Severity.ERROR, Set.of());

    public static final ProductErrorDefinition STORAGE_DATABASES_OVERLAP =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 24, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition STORAGE_TEST_REQUIRED =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 25, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition STORAGE_TEST_FAILED =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 26, Severity.ERROR, Set.of("layer", "causeCode"));
    public static final ProductErrorDefinition STORAGE_CACHE_DATABASE_OCCUPIED =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 27, Severity.ERROR, Set.of("database"));
    public static final ProductErrorDefinition STORAGE_CACHE_DATABASE_OCCUPIED_SUGGESTED =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 28, Severity.ERROR, Set.of("database", "suggestedDatabase"));
    public static final ProductErrorDefinition STORAGE_SCOPE_CONFLICT =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 29, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition STORAGE_UNAVAILABLE =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 30, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition STORAGE_QUOTA_EXCEEDED =
            new ProductErrorDefinition(ProductErrorModule.CONTROL, 31, Severity.ERROR, Set.of());

    private RequestErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(MALFORMED_REQUEST, AUTH_FAILED, UNAUTHENTICATED, FORBIDDEN, UNREACHABLE,
                AI_NOT_CONFIGURED, AI_PROVIDER_AUTH_FAILED, AI_PROVIDER_REQUEST_REJECTED,
                AI_PROVIDER_RATE_LIMITED, AI_PROVIDER_FAILED, AI_PROVIDER_OUTPUT_LIMIT, AI_INVALID_PROPOSAL, AI_OVERLOADED,
                PREVIEW_OVERLOADED, AUDIT_BLOCKED, SCHEMA_DERIVATION_INCOMPLETE, BOOTSTRAP_FORBIDDEN, BOOTSTRAP_CLOSED, AUTH_MODE_UNAVAILABLE, BROWSER_AUTH_CONFIG_INVALID, CLOUD_AUTH_UNAVAILABLE, STATE_DATABASE_UNAVAILABLE, SOURCE_CONFIG_REVEAL_UNAVAILABLE,
                STORAGE_DATABASES_OVERLAP, STORAGE_TEST_REQUIRED, STORAGE_TEST_FAILED, STORAGE_CACHE_DATABASE_OCCUPIED, STORAGE_CACHE_DATABASE_OCCUPIED_SUGGESTED, STORAGE_SCOPE_CONFLICT, STORAGE_UNAVAILABLE, STORAGE_QUOTA_EXCEEDED);
    }
}
