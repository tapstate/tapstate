package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class StoreErrorDefinitions {
    public static final ProductErrorDefinition UNREACHABLE =
            new ProductErrorDefinition(ProductErrorModule.STORE, 1, Severity.ERROR, Set.of("target"));
    public static final ProductErrorDefinition DATABASE_ACCESS_FAILED =
            new ProductErrorDefinition(ProductErrorModule.STORE, 2, Severity.ERROR, Set.of("database"));
    public static final ProductErrorDefinition CACHE_LIST_COLLECTIONS_DENIED =
            new ProductErrorDefinition(ProductErrorModule.STORE, 3, Severity.ERROR, Set.of("database"));
    public static final ProductErrorDefinition NOT_REPLICA_SET =
            new ProductErrorDefinition(ProductErrorModule.STORE, 4, Severity.ERROR, Set.of("target"));
    public static final ProductErrorDefinition INVALID_OPERATOR_STATE_DATABASE =
            new ProductErrorDefinition(ProductErrorModule.STORE, 5, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition INVALID_URI =
            new ProductErrorDefinition(ProductErrorModule.STORE, 6, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition AI_PROVIDER_SCHEMA_UNSUPPORTED =
            new ProductErrorDefinition(ProductErrorModule.STORE, 7, Severity.ERROR, Set.of("required"));
    public static final ProductErrorDefinition SOURCE_CONFIG_KEYRING_INVALID =
            new ProductErrorDefinition(ProductErrorModule.STORE, 8, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition SOURCE_CONFIG_KEYRING_NOT_READY =
            new ProductErrorDefinition(ProductErrorModule.STORE, 9, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition SOURCE_CONFIG_KEYRING_ROTATION_BLOCKED =
            new ProductErrorDefinition(ProductErrorModule.STORE, 10, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition INITIALIZATION_REFUSED =
            new ProductErrorDefinition(ProductErrorModule.STORE, 11, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition DEFAULT_SOURCE_PROTECTED =
            new ProductErrorDefinition(ProductErrorModule.STORE, 12, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition PIPELINE_RETIREMENT_REQUIRED =
            new ProductErrorDefinition(ProductErrorModule.STORE, 13, Severity.ERROR, Set.of("pipeline"));
    public static final ProductErrorDefinition TLS_CA_UNREADABLE =
            new ProductErrorDefinition(ProductErrorModule.STORE, 14, Severity.ERROR, Set.of("path"));
    public static final ProductErrorDefinition STORAGE_CONNECTION_UNSUPPORTED =
            new ProductErrorDefinition(ProductErrorModule.STORE, 15, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition INVALID_HISTORY_RETENTION =
            new ProductErrorDefinition(ProductErrorModule.STORE, 16, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition HISTORY_RETENTION_MISMATCH =
            new ProductErrorDefinition(ProductErrorModule.STORE, 17, Severity.ERROR, Set.of("configuredSeconds", "storedSeconds"));
    public static final ProductErrorDefinition TLS_TRUST_INVALID =
            new ProductErrorDefinition(ProductErrorModule.STORE, 18, Severity.ERROR, Set.of());

    private StoreErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(UNREACHABLE, DATABASE_ACCESS_FAILED, CACHE_LIST_COLLECTIONS_DENIED, NOT_REPLICA_SET, INVALID_OPERATOR_STATE_DATABASE, INVALID_URI, AI_PROVIDER_SCHEMA_UNSUPPORTED, SOURCE_CONFIG_KEYRING_INVALID, SOURCE_CONFIG_KEYRING_NOT_READY, SOURCE_CONFIG_KEYRING_ROTATION_BLOCKED, INITIALIZATION_REFUSED, DEFAULT_SOURCE_PROTECTED, PIPELINE_RETIREMENT_REQUIRED, TLS_CA_UNREADABLE, STORAGE_CONNECTION_UNSUPPORTED, INVALID_HISTORY_RETENTION, HISTORY_RETENTION_MISMATCH, TLS_TRUST_INVALID);
    }
}
