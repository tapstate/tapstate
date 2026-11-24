package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class StoreErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.STORE);

    public static final ProductErrorDefinition UNREACHABLE =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("target"));
    public static final ProductErrorDefinition DATABASE_ACCESS_FAILED =
            DEFINITIONS.define(2, Severity.ERROR, Set.of("database"));
    public static final ProductErrorDefinition CACHE_LIST_COLLECTIONS_DENIED =
            DEFINITIONS.define(3, Severity.ERROR, Set.of("database"));
    public static final ProductErrorDefinition NOT_REPLICA_SET =
            DEFINITIONS.define(4, Severity.ERROR, Set.of("target"));
    public static final ProductErrorDefinition INVALID_OPERATOR_STATE_DATABASE =
            DEFINITIONS.define(5, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition INVALID_URI =
            DEFINITIONS.define(6, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition AI_PROVIDER_SCHEMA_UNSUPPORTED =
            DEFINITIONS.define(7, Severity.ERROR, Set.of("required"));
    public static final ProductErrorDefinition SOURCE_CONFIG_KEYRING_INVALID =
            DEFINITIONS.define(8, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition SOURCE_CONFIG_KEYRING_NOT_READY =
            DEFINITIONS.define(9, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition SOURCE_CONFIG_KEYRING_ROTATION_BLOCKED =
            DEFINITIONS.define(10, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition INITIALIZATION_REFUSED =
            DEFINITIONS.define(11, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition DEFAULT_SOURCE_PROTECTED =
            DEFINITIONS.define(12, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition PIPELINE_RETIREMENT_REQUIRED =
            DEFINITIONS.define(13, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition TLS_CA_UNREADABLE =
            DEFINITIONS.define(14, Severity.ERROR, Set.of("path"));
    public static final ProductErrorDefinition STORAGE_CONNECTION_UNSUPPORTED =
            DEFINITIONS.define(15, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition INVALID_HISTORY_RETENTION =
            DEFINITIONS.define(16, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition HISTORY_RETENTION_MISMATCH =
            DEFINITIONS.define(17, Severity.ERROR, Set.of("configuredSeconds", "storedSeconds"));
    public static final ProductErrorDefinition TLS_TRUST_INVALID =
            DEFINITIONS.define(18, Severity.ERROR, Set.of());

    private StoreErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
