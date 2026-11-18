package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class ConnectorErrorDefinitions {
    public static final ProductErrorDefinition TEST_FAILED =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 1, Severity.ERROR, Set.of("connector", "detail"));
    public static final ProductErrorDefinition TARGET_IDENTITY_CONFLICT =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 2, Severity.ERROR, Set.of("table", "field", "preserved", "reason"));
    public static final ProductErrorDefinition TARGET_IDENTITY_CHECK_FAILED =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 3, Severity.ERROR, Set.of("connector", "table", "detail"));
    public static final ProductErrorDefinition NOT_REGISTERED =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 4, Severity.ERROR, Set.of("connector"));
    public static final ProductErrorDefinition AMBIGUOUS_REGISTRATION =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 5, Severity.ERROR, Set.of("connector", "artifacts"));
    public static final ProductErrorDefinition REGISTRATION_CONFLICT =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 6, Severity.ERROR, Set.of("connector", "existing", "incoming"));
    public static final ProductErrorDefinition CLOUD_UPLOAD_DISABLED =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 7, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition NOT_OFFICIAL =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 8, Severity.ERROR, Set.of("connector", "official"));
    public static final ProductErrorDefinition LOAD_FAILED =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 9, Severity.ERROR, Set.of("connector"));
    public static final ProductErrorDefinition TLS_TRUST_INVALID =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 10, Severity.ERROR, Set.of("connector"));
    public static final ProductErrorDefinition TLS_MATERIAL_CLEANUP_FAILED =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 11, Severity.ERROR, Set.of("connector"));
    public static final ProductErrorDefinition CLASS_NOT_FOUND =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 12, Severity.ERROR, Set.of("connector", "class"));
    public static final ProductErrorDefinition ARTIFACT_UNREADABLE =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 13, Severity.ERROR, Set.of("artifact"));
    public static final ProductErrorDefinition NO_CONNECTOR_CLASS =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 14, Severity.ERROR, Set.of("artifact"));
    public static final ProductErrorDefinition AMBIGUOUS_CONNECTOR_CLASS =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 15, Severity.ERROR, Set.of("artifact", "classes"));
    public static final ProductErrorDefinition SPEC_NOT_FOUND =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 16, Severity.ERROR, Set.of("artifact", "spec"));
    public static final ProductErrorDefinition SPEC_INVALID =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 17, Severity.ERROR, Set.of("artifact", "spec", "detail"));
    public static final ProductErrorDefinition CONFIG_TYPE_MISMATCH =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 18, Severity.ERROR, Set.of("connector", "field", "expected", "value"));
    public static final ProductErrorDefinition LOGMINER_IDENTIFIER_TOO_LONG =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 19, Severity.ERROR, Set.of("connector", "kind", "identifier", "limit"));
    public static final ProductErrorDefinition LOGMINER_PREFLIGHT_TIMEOUT =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 20, Severity.ERROR, Set.of("connector", "timeout"));
    public static final ProductErrorDefinition API_LEVEL_INCOMPATIBLE =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 21, Severity.ERROR, Set.of("connector", "required", "provided"));
    public static final ProductErrorDefinition API_LEVEL_UNKNOWN =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 22, Severity.ERROR, Set.of("connector", "version"));
    public static final ProductErrorDefinition PROJECTION_FAILED =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 23, Severity.ERROR, Set.of("connector", "detail"));
    public static final ProductErrorDefinition CAPTURE_FAILED =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 24, Severity.ERROR, Set.of("connector", "detail"));
    public static final ProductErrorDefinition TEST_TIMEOUT =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 25, Severity.ERROR, Set.of("connector", "timeout"));
    public static final ProductErrorDefinition DISCOVER_FAILED =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 26, Severity.ERROR, Set.of("connector", "detail"));
    public static final ProductErrorDefinition WRITE_FAILED =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 27, Severity.ERROR, Set.of("connector", "detail"));
    public static final ProductErrorDefinition READ_FAILED =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 28, Severity.ERROR, Set.of("connector", "detail"));
    public static final ProductErrorDefinition SNAPSHOT_PREPARATION_UNAVAILABLE =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 29, Severity.ERROR, Set.of("connector", "reason"));
    public static final ProductErrorDefinition READ_ABANDONED =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 30, Severity.ERROR, Set.of("connector"));
    public static final ProductErrorDefinition CAPABILITY_MISSING =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 31, Severity.ERROR, Set.of("connector", "capability"));
    public static final ProductErrorDefinition INSTANCES_BUSY =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 32, Severity.ERROR, Set.of("connector", "timeout"));
    public static final ProductErrorDefinition INSTANCE_LIMIT_REACHED =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 33, Severity.ERROR, Set.of("limit"));
    public static final ProductErrorDefinition READ_TIMEOUT =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 34, Severity.ERROR, Set.of("connector", "timeout"));
    public static final ProductErrorDefinition STATE_VALUE_UNSUPPORTED =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 35, Severity.ERROR, Set.of("type"));
    public static final ProductErrorDefinition STATE_UNREADABLE =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 36, Severity.ERROR, Set.of("detail"));
    public static final ProductErrorDefinition POSITION_UNRENDERABLE =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 37, Severity.ERROR, Set.of("connector", "detail"));
    public static final ProductErrorDefinition POSITION_UNREADABLE =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 38, Severity.ERROR, Set.of("connector", "detail"));
    public static final ProductErrorDefinition ACKNOWLEDGE_FAILED =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 39, Severity.ERROR, Set.of("connector", "detail"));
    public static final ProductErrorDefinition RELEASE_FAILED =
            new ProductErrorDefinition(ProductErrorModule.CONNECTOR, 40, Severity.ERROR, Set.of("connector", "detail", "resources"));

    private ConnectorErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(TEST_FAILED, TARGET_IDENTITY_CONFLICT, TARGET_IDENTITY_CHECK_FAILED, NOT_REGISTERED, AMBIGUOUS_REGISTRATION, REGISTRATION_CONFLICT, CLOUD_UPLOAD_DISABLED, NOT_OFFICIAL, LOAD_FAILED, TLS_TRUST_INVALID, TLS_MATERIAL_CLEANUP_FAILED, CLASS_NOT_FOUND, ARTIFACT_UNREADABLE, NO_CONNECTOR_CLASS, AMBIGUOUS_CONNECTOR_CLASS, SPEC_NOT_FOUND, SPEC_INVALID, CONFIG_TYPE_MISMATCH, LOGMINER_IDENTIFIER_TOO_LONG, LOGMINER_PREFLIGHT_TIMEOUT, API_LEVEL_INCOMPATIBLE, API_LEVEL_UNKNOWN, PROJECTION_FAILED, CAPTURE_FAILED, TEST_TIMEOUT, DISCOVER_FAILED, WRITE_FAILED, READ_FAILED, SNAPSHOT_PREPARATION_UNAVAILABLE, READ_ABANDONED, CAPABILITY_MISSING, INSTANCES_BUSY, INSTANCE_LIMIT_REACHED, READ_TIMEOUT, STATE_VALUE_UNSUPPORTED, STATE_UNREADABLE, POSITION_UNRENDERABLE, POSITION_UNREADABLE, ACKNOWLEDGE_FAILED, RELEASE_FAILED);
    }
}
