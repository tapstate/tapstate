package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Shared diagnostic identities retain their owners' existing behavioral contracts. */
public final class ConnectorErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.CONNECTOR);

    public static final ProductErrorDefinition TEST_FAILED =
            DEFINITIONS.define(1, Severity.ERROR, Set.of("connector", "detail"));
    public static final ProductErrorDefinition TARGET_IDENTITY_CONFLICT =
            DEFINITIONS.define(2, Severity.ERROR, Set.of("table", "field", "preserved", "reason"));
    public static final ProductErrorDefinition TARGET_IDENTITY_CHECK_FAILED =
            DEFINITIONS.define(3, Severity.ERROR, Set.of("connector", "table", "detail"));
    public static final ProductErrorDefinition NOT_REGISTERED =
            DEFINITIONS.define(4, Severity.ERROR, Set.of("connector"));
    public static final ProductErrorDefinition AMBIGUOUS_REGISTRATION =
            DEFINITIONS.define(5, Severity.ERROR, Set.of("connector", "artifacts"));
    public static final ProductErrorDefinition REGISTRATION_CONFLICT =
            DEFINITIONS.define(6, Severity.ERROR, Set.of("connector", "existing", "incoming"));
    public static final ProductErrorDefinition CLOUD_UPLOAD_DISABLED =
            DEFINITIONS.define(7, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition NOT_OFFICIAL =
            DEFINITIONS.define(8, Severity.ERROR, Set.of("connector", "official"));
    public static final ProductErrorDefinition LOAD_FAILED =
            DEFINITIONS.define(9, Severity.ERROR, Set.of("connector"));
    public static final ProductErrorDefinition TLS_TRUST_INVALID =
            DEFINITIONS.define(10, Severity.ERROR, Set.of("connector"));
    public static final ProductErrorDefinition TLS_MATERIAL_CLEANUP_FAILED =
            DEFINITIONS.define(11, Severity.ERROR, Set.of("connector"));
    public static final ProductErrorDefinition CLASS_NOT_FOUND =
            DEFINITIONS.define(12, Severity.ERROR, Set.of("connector", "class"));
    public static final ProductErrorDefinition ARTIFACT_UNREADABLE =
            DEFINITIONS.define(13, Severity.ERROR, Set.of("artifact"));
    public static final ProductErrorDefinition NO_CONNECTOR_CLASS =
            DEFINITIONS.define(14, Severity.ERROR, Set.of("artifact"));
    public static final ProductErrorDefinition AMBIGUOUS_CONNECTOR_CLASS =
            DEFINITIONS.define(15, Severity.ERROR, Set.of("artifact", "classes"));
    public static final ProductErrorDefinition SPEC_NOT_FOUND =
            DEFINITIONS.define(16, Severity.ERROR, Set.of("artifact", "spec"));
    public static final ProductErrorDefinition SPEC_INVALID =
            DEFINITIONS.define(17, Severity.ERROR, Set.of("artifact", "spec", "detail"));
    public static final ProductErrorDefinition CONFIG_TYPE_MISMATCH =
            DEFINITIONS.define(18, Severity.ERROR, Set.of("connector", "field", "expected", "value"));
    public static final ProductErrorDefinition LOGMINER_IDENTIFIER_TOO_LONG =
            DEFINITIONS.define(19, Severity.ERROR, Set.of("connector", "kind", "identifier", "limit"));
    public static final ProductErrorDefinition LOGMINER_PREFLIGHT_TIMEOUT =
            DEFINITIONS.define(20, Severity.ERROR, Set.of("connector", "timeout"));
    public static final ProductErrorDefinition API_LEVEL_INCOMPATIBLE =
            DEFINITIONS.define(21, Severity.ERROR, Set.of("connector", "required", "provided"));
    public static final ProductErrorDefinition API_LEVEL_UNKNOWN =
            DEFINITIONS.define(22, Severity.ERROR, Set.of("connector", "version"));
    public static final ProductErrorDefinition PROJECTION_FAILED =
            DEFINITIONS.define(23, Severity.ERROR, Set.of("connector", "detail"));
    public static final ProductErrorDefinition CAPTURE_FAILED =
            DEFINITIONS.define(24, Severity.ERROR, Set.of("connector", "detail"));
    public static final ProductErrorDefinition TEST_TIMEOUT =
            DEFINITIONS.define(25, Severity.ERROR, Set.of("connector", "timeout"));
    public static final ProductErrorDefinition DISCOVER_FAILED =
            DEFINITIONS.define(26, Severity.ERROR, Set.of("connector", "detail"));
    public static final ProductErrorDefinition WRITE_FAILED =
            DEFINITIONS.define(27, Severity.ERROR, Set.of("connector", "detail"));
    public static final ProductErrorDefinition READ_FAILED =
            DEFINITIONS.define(28, Severity.ERROR, Set.of("connector", "detail"));
    public static final ProductErrorDefinition SNAPSHOT_PREPARATION_UNAVAILABLE =
            DEFINITIONS.define(29, Severity.ERROR, Set.of("connector", "reason"));
    public static final ProductErrorDefinition READ_ABANDONED =
            DEFINITIONS.define(30, Severity.ERROR, Set.of("connector"));
    public static final ProductErrorDefinition CAPABILITY_MISSING =
            DEFINITIONS.define(31, Severity.ERROR, Set.of("connector", "capability"));
    public static final ProductErrorDefinition INSTANCES_BUSY =
            DEFINITIONS.define(32, Severity.ERROR, Set.of("connector", "timeout"));
    public static final ProductErrorDefinition INSTANCE_LIMIT_REACHED =
            DEFINITIONS.define(33, Severity.ERROR, Set.of("limit"));
    public static final ProductErrorDefinition READ_TIMEOUT =
            DEFINITIONS.define(34, Severity.ERROR, Set.of("connector", "timeout"));
    public static final ProductErrorDefinition STATE_VALUE_UNSUPPORTED =
            DEFINITIONS.define(35, Severity.ERROR, Set.of("type"));
    public static final ProductErrorDefinition STATE_UNREADABLE =
            DEFINITIONS.define(36, Severity.ERROR, Set.of("detail"));
    public static final ProductErrorDefinition POSITION_UNRENDERABLE =
            DEFINITIONS.define(37, Severity.ERROR, Set.of("connector", "detail"));
    public static final ProductErrorDefinition POSITION_UNREADABLE =
            DEFINITIONS.define(38, Severity.ERROR, Set.of("connector", "detail"));
    public static final ProductErrorDefinition ACKNOWLEDGE_FAILED =
            DEFINITIONS.define(39, Severity.ERROR, Set.of("connector", "detail"));
    public static final ProductErrorDefinition RELEASE_FAILED =
            DEFINITIONS.define(40, Severity.ERROR, Set.of("connector", "detail", "resources"));

    public static final ProductErrorDefinition DISCOVERY_CONNECTION_TIMEOUT =
            DEFINITIONS.define(41, Severity.ERROR, Set.of("connector"));

    public static final ProductErrorDefinition DISCOVERY_AUTHENTICATION_REJECTED =
            DEFINITIONS.define(42, Severity.ERROR, Set.of("connector"));

    public static final ProductErrorDefinition LOCAL_ARTIFACT_UNAVAILABLE =
            DEFINITIONS.define(43, Severity.ERROR, Set.of("connector", "hash"));

    private ConnectorErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
