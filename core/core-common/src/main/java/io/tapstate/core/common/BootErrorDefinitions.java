package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Startup diagnostics retain the original configuration and process boundaries. */
public final class BootErrorDefinitions {
    private static final ProductErrorDefinitionSet DEFINITIONS = new ProductErrorDefinitionSet(ProductErrorModule.BOOT);

    public static final ProductErrorDefinition HAZELCAST_UNAVAILABLE =
            DEFINITIONS.define(1, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CLOUD_CONFIG_INCOMPLETE =
            DEFINITIONS.define(2, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CLOUD_BASE_URL_INVALID =
            DEFINITIONS.define(3, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CLOUD_ATLAS_URI_INVALID =
            DEFINITIONS.define(4, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition WEB_PROFILE_INVALID =
            DEFINITIONS.define(5, Severity.ERROR, Set.of("reason"));
    public static final ProductErrorDefinition WEB_PROFILE_MODE_MISMATCH =
            DEFINITIONS.define(6, Severity.ERROR, Set.of("profile", "mode"));
    public static final ProductErrorDefinition WEB_CONSOLE_URL_INVALID =
            DEFINITIONS.define(7, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CLOUD_STORE_REQUIRED =
            DEFINITIONS.define(8, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CLOUD_VIEW_STORE_CONFLICT =
            DEFINITIONS.define(9, Severity.ERROR, Set.of("store"));
    public static final ProductErrorDefinition CLOUD_STATUS_CONFIG_INVALID =
            DEFINITIONS.define(10, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CLOUD_STATUS_MODE_REQUIRED =
            DEFINITIONS.define(11, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CLOUD_STATUS_SDK_REQUIRED =
            DEFINITIONS.define(12, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CLOUD_CONNECTORS_INVALID =
            DEFINITIONS.define(13, Severity.ERROR, Set.of("connector", "reason"));
    public static final ProductErrorDefinition DISCOVERY_CONFIG_INVALID =
            DEFINITIONS.define(14, Severity.ERROR, Set.of("detail"));
    public static final ProductErrorDefinition CLUSTER_ID_REQUIRED =
            DEFINITIONS.define(15, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CLUSTER_ID_INVALID =
            DEFINITIONS.define(16, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CLUSTER_ID_MISMATCH =
            DEFINITIONS.define(17, Severity.ERROR, Set.of("configured", "stored"));
    public static final ProductErrorDefinition CLUSTER_PROFILE_INVALID =
            DEFINITIONS.define(18, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition HEARTBEAT_CONFIG_INVALID =
            DEFINITIONS.define(19, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition NODE_ID_REQUIRED =
            DEFINITIONS.define(20, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CONTROL_ADVERTISE_URL_INVALID =
            DEFINITIONS.define(21, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition MEMBER_BIND_ADDRESS_INVALID =
            DEFINITIONS.define(22, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition NODE_SESSION_TTL_INVALID =
            DEFINITIONS.define(23, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition NODE_SESSION_RENEW_INTERVAL_INVALID =
            DEFINITIONS.define(24, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition WORKLOAD_CLAIM_TTL_INVALID =
            DEFINITIONS.define(25, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition WORKLOAD_CLAIM_RENEW_INTERVAL_INVALID =
            DEFINITIONS.define(26, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition COORDINATION_STORE_REQUIRED =
            DEFINITIONS.define(27, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition NODE_ID_IN_USE =
            DEFINITIONS.define(28, Severity.ERROR, Set.of("nodeId"));
    public static final ProductErrorDefinition NODE_SESSION_LOST =
            DEFINITIONS.define(29, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition WEB_DEVELOPMENT_CONFIRMATION_REQUIRED =
            DEFINITIONS.define(30, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition WEB_DEVELOPMENT_ARTIFACT_INVALID =
            DEFINITIONS.define(31, Severity.ERROR, Set.of("reason"));

    public static final ProductErrorDefinition MIGRATION_MODE_CONFLICT =
            DEFINITIONS.define(32, Severity.ERROR, Set.of());

    private BootErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return DEFINITIONS.registered();
    }
}
