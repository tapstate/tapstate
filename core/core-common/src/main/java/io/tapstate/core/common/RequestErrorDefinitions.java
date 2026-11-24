package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared request-contract definitions used by both service and independent client producers. */
public final class RequestErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.CONTROL);

    public static final ProductErrorDefinition MALFORMED_REQUEST =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("reason"));
    public static final ProductErrorDefinition AUTH_FAILED =
            DEFINITIONS.define(2, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition UNAUTHENTICATED =
            DEFINITIONS.define(3, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition FORBIDDEN =
            DEFINITIONS.define(4, Severity.ERROR, Set.of("op", "required"));
    public static final ProductErrorDefinition UNREACHABLE =
            DEFINITIONS.define(5, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition AI_NOT_CONFIGURED =
            DEFINITIONS.define(6, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition AI_PROVIDER_AUTH_FAILED =
            DEFINITIONS.define(7, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition AI_PROVIDER_REQUEST_REJECTED =
            DEFINITIONS.define(8, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition AI_PROVIDER_RATE_LIMITED =
            DEFINITIONS.define(9, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition AI_PROVIDER_FAILED =
            DEFINITIONS.define(10, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition AI_PROVIDER_OUTPUT_LIMIT =
            DEFINITIONS.define(11, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition AI_INVALID_PROPOSAL =
            DEFINITIONS.define(12, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition AI_OVERLOADED =
            DEFINITIONS.define(13, Severity.ERROR, Set.of());

    public static final ProductErrorDefinition PREVIEW_OVERLOADED =
            DEFINITIONS.define(14, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition AUDIT_BLOCKED =
            DEFINITIONS.define(15, Severity.ERROR, Set.of("op"));
    public static final ProductErrorDefinition SCHEMA_DERIVATION_INCOMPLETE =
            DEFINITIONS.define(16, Severity.WARNING, Set.of("pipeline", "causeCode", "causeParams"));
    public static final ProductErrorDefinition BOOTSTRAP_FORBIDDEN =
            DEFINITIONS.define(17, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition BOOTSTRAP_CLOSED =
            DEFINITIONS.define(18, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition AUTH_MODE_UNAVAILABLE =
            DEFINITIONS.define(19, Severity.ERROR, Set.of("mode"));
    public static final ProductErrorDefinition BROWSER_AUTH_CONFIG_INVALID =
            DEFINITIONS.define(20, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CLOUD_AUTH_UNAVAILABLE =
            DEFINITIONS.define(21, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition STATE_DATABASE_UNAVAILABLE =
            DEFINITIONS.define(22, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition SOURCE_CONFIG_REVEAL_UNAVAILABLE =
            DEFINITIONS.define(23, Severity.ERROR, Set.of());

    public static final ProductErrorDefinition STORAGE_DATABASES_OVERLAP =
            DEFINITIONS.define(24, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition STORAGE_TEST_REQUIRED =
            DEFINITIONS.define(25, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition STORAGE_TEST_FAILED =
            DEFINITIONS.define(26, Severity.ERROR, Set.of("layer", "causeCode"));
    public static final ProductErrorDefinition STORAGE_CACHE_DATABASE_OCCUPIED =
            DEFINITIONS.define(27, Severity.ERROR, Set.of("database"));
    public static final ProductErrorDefinition STORAGE_CACHE_DATABASE_OCCUPIED_SUGGESTED =
            DEFINITIONS.define(28, Severity.ERROR, Set.of("database", "suggestedDatabase"));
    public static final ProductErrorDefinition STORAGE_SCOPE_CONFLICT =
            DEFINITIONS.define(29, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition STORAGE_UNAVAILABLE =
            DEFINITIONS.define(30, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition STORAGE_QUOTA_EXCEEDED =
            DEFINITIONS.define(31, Severity.ERROR, Set.of());

    public static final ProductErrorDefinition AI_MANAGED_PROVIDER_AUTH_FAILED =
            DEFINITIONS.define(32, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition AI_MANAGED_PROVIDER_INPUT_LIMIT =
            DEFINITIONS.define(33, Severity.ERROR, Set.of());

    public static final ProductErrorDefinition RETRY_SETTINGS_REVISION_CONFLICT =
            DEFINITIONS.define(34, Severity.ERROR, Set.of());

    private RequestErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
