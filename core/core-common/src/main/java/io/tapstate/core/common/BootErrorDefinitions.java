package io.tapstate.core.common;

import java.util.List;
import java.util.Set;

/** Startup diagnostics retain the original configuration and process boundaries. */
public final class BootErrorDefinitions {
    public static final ProductErrorDefinition HAZELCAST_UNAVAILABLE =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 1, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CLOUD_CONFIG_INCOMPLETE =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 2, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CLOUD_BASE_URL_INVALID =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 3, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CLOUD_ATLAS_URI_INVALID =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 4, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition WEB_PROFILE_INVALID =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 5, Severity.ERROR, Set.of("reason"));
    public static final ProductErrorDefinition WEB_PROFILE_MODE_MISMATCH =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 6, Severity.ERROR, Set.of("profile", "mode"));
    public static final ProductErrorDefinition WEB_CONSOLE_URL_INVALID =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 7, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CLOUD_STORE_REQUIRED =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 8, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CLOUD_VIEW_STORE_CONFLICT =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 9, Severity.ERROR, Set.of("store"));
    public static final ProductErrorDefinition CLOUD_STATUS_CONFIG_INVALID =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 10, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CLOUD_STATUS_MODE_REQUIRED =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 11, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CLOUD_STATUS_SDK_REQUIRED =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 12, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CLOUD_CONNECTORS_INVALID =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 13, Severity.ERROR, Set.of("connector", "reason"));
    public static final ProductErrorDefinition DISCOVERY_CONFIG_INVALID =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 14, Severity.ERROR, Set.of("detail"));
    public static final ProductErrorDefinition CLUSTER_ID_REQUIRED =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 15, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CLUSTER_ID_INVALID =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 16, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CLUSTER_ID_MISMATCH =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 17, Severity.ERROR, Set.of("configured", "stored"));
    public static final ProductErrorDefinition CLUSTER_PROFILE_INVALID =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 18, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition HEARTBEAT_CONFIG_INVALID =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 19, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition NODE_ID_REQUIRED =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 20, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition CONTROL_ADVERTISE_URL_INVALID =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 21, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition MEMBER_BIND_ADDRESS_INVALID =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 22, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition NODE_SESSION_TTL_INVALID =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 23, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition NODE_SESSION_RENEW_INTERVAL_INVALID =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 24, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition WORKLOAD_CLAIM_TTL_INVALID =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 25, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition WORKLOAD_CLAIM_RENEW_INTERVAL_INVALID =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 26, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition COORDINATION_STORE_REQUIRED =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 27, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition NODE_ID_IN_USE =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 28, Severity.ERROR, Set.of("nodeId"));
    public static final ProductErrorDefinition NODE_SESSION_LOST =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 29, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition WEB_DEVELOPMENT_CONFIRMATION_REQUIRED =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 30, Severity.ERROR, Set.of());
    public static final ProductErrorDefinition WEB_DEVELOPMENT_ARTIFACT_INVALID =
            new ProductErrorDefinition(ProductErrorModule.BOOT, 31, Severity.ERROR, Set.of("reason"));

    private BootErrorDefinitions() {}

    public static List<ProductErrorDefinition> registered() {
        return List.of(HAZELCAST_UNAVAILABLE, CLOUD_CONFIG_INCOMPLETE, CLOUD_BASE_URL_INVALID, CLOUD_ATLAS_URI_INVALID, WEB_PROFILE_INVALID, WEB_PROFILE_MODE_MISMATCH, WEB_CONSOLE_URL_INVALID, CLOUD_STORE_REQUIRED, CLOUD_VIEW_STORE_CONFLICT, CLOUD_STATUS_CONFIG_INVALID, CLOUD_STATUS_MODE_REQUIRED, CLOUD_STATUS_SDK_REQUIRED, CLOUD_CONNECTORS_INVALID, DISCOVERY_CONFIG_INVALID, CLUSTER_ID_REQUIRED, CLUSTER_ID_INVALID, CLUSTER_ID_MISMATCH, CLUSTER_PROFILE_INVALID, HEARTBEAT_CONFIG_INVALID, NODE_ID_REQUIRED, CONTROL_ADVERTISE_URL_INVALID, MEMBER_BIND_ADDRESS_INVALID, NODE_SESSION_TTL_INVALID, NODE_SESSION_RENEW_INTERVAL_INVALID, WORKLOAD_CLAIM_TTL_INVALID, WORKLOAD_CLAIM_RENEW_INTERVAL_INVALID, COORDINATION_STORE_REQUIRED, NODE_ID_IN_USE, NODE_SESSION_LOST, WEB_DEVELOPMENT_CONFIRMATION_REQUIRED, WEB_DEVELOPMENT_ARTIFACT_INVALID);
    }
}
