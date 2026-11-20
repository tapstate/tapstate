package io.tapstate.app;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;
import io.tapstate.core.common.ProductErrorDefinition;
import io.tapstate.core.common.BootErrorDefinitions;

import java.util.Set;

/**
 * The {@code boot} domain's error codes: startup-fatal failures of the assembly root that are the
 * operator's to fix. Bringing the embedded Hazelcast member up can fail because its loopback port is
 * already held by another server on the same host — a user-facing, diagnosable failure carried
 * through the error-code system and rendered through the shared message catalog, not a bare stack.
 *
 * <p>{@code placeholders()} is the named-argument contract: every throw site supplies a value for
 * each name, and the build-time placeholder gate checks the catalog templates against it. This code
 * carries none on purpose — the underlying cause is attached to the exception (and logged), so the
 * operator-facing message stays a stable, detail-free diagnostic.
 */
enum BootError implements TapstateErrorCode {

    /** The embedded Hazelcast member could not be started (e.g. its loopback port is in use). */
    HAZELCAST_UNAVAILABLE(BootErrorDefinitions.HAZELCAST_UNAVAILABLE),

    /** A managed Cloud runtime must receive its four external settings as one complete unit. */
    CLOUD_CONFIG_INCOMPLETE(BootErrorDefinitions.CLOUD_CONFIG_INCOMPLETE),

    /** The configured Global Control Plane address is not an absolute HTTP(S) base URL. */
    CLOUD_BASE_URL_INVALID(BootErrorDefinitions.CLOUD_BASE_URL_INVALID),

    /** The managed metadata connection is not a MongoDB or MongoDB SRV connection string. */
    CLOUD_ATLAS_URI_INVALID(BootErrorDefinitions.CLOUD_ATLAS_URI_INVALID),

    /** A Web-bearing distribution has missing, unsupported, malformed, or conflicting metadata. */
    WEB_PROFILE_INVALID(BootErrorDefinitions.WEB_PROFILE_INVALID),

    /** The packaged Web profile does not match the mode selected by the external Cloud settings. */
    WEB_PROFILE_MODE_MISMATCH(BootErrorDefinitions.WEB_PROFILE_MODE_MISMATCH),

    /** Cloud Web metadata requires a safe Console URL for its artifact purpose; on-prem carries none. */
    WEB_CONSOLE_URL_INVALID(BootErrorDefinitions.WEB_CONSOLE_URL_INVALID),

    /** The optional on-prem store switch cannot disable managed Cloud metadata persistence. */
    CLOUD_STORE_REQUIRED(BootErrorDefinitions.CLOUD_STORE_REQUIRED),

    /** An existing view-store resource conflicts with the managed deployment or a concurrent refresh. */
    CLOUD_VIEW_STORE_CONFLICT(BootErrorDefinitions.CLOUD_VIEW_STORE_CONFLICT),

    /** The outbound status switch is malformed; its value is deliberately not echoed. */
    CLOUD_STATUS_CONFIG_INVALID(BootErrorDefinitions.CLOUD_STATUS_CONFIG_INVALID),

    /** An on-prem process attempted to enable managed Cloud status reporting. */
    CLOUD_STATUS_MODE_REQUIRED(BootErrorDefinitions.CLOUD_STATUS_MODE_REQUIRED),

    /** Status reporting was explicitly enabled without the real SDK-backed reporter. */
    CLOUD_STATUS_SDK_REQUIRED(BootErrorDefinitions.CLOUD_STATUS_SDK_REQUIRED),

    /** A Cloud connector release is missing, mismatched, or cannot be fully registered and loaded. */
    CLOUD_CONNECTORS_INVALID(BootErrorDefinitions.CLOUD_CONNECTORS_INVALID),

    /** The selected member-discovery mode is missing a required, deterministic input. */
    DISCOVERY_CONFIG_INVALID(BootErrorDefinitions.DISCOVERY_CONFIG_INVALID),

    /** Cluster mode was selected without the stable cluster id that names every coordination record. */
    CLUSTER_ID_REQUIRED(BootErrorDefinitions.CLUSTER_ID_REQUIRED),

    /** The configured cluster id cannot be represented by the stable identity contract. */
    CLUSTER_ID_INVALID(BootErrorDefinitions.CLUSTER_ID_INVALID),

    /** The configured cluster id disagrees with the identity already stored for this control store. */
    CLUSTER_ID_MISMATCH(BootErrorDefinitions.CLUSTER_ID_MISMATCH),

    /** The selected cluster profile and bootstrap member count cannot provide its promised safety. */
    CLUSTER_PROFILE_INVALID(BootErrorDefinitions.CLUSTER_PROFILE_INVALID),

    /** Member heartbeats require positive whole-second values and a longer failure-detection window. */
    HEARTBEAT_CONFIG_INVALID(BootErrorDefinitions.HEARTBEAT_CONFIG_INVALID),

    /** Cluster mode was selected without this member's stable node id. */
    NODE_ID_REQUIRED(BootErrorDefinitions.NODE_ID_REQUIRED),

    /** The advertised control endpoint is missing or is not an absolute HTTP(S) URL. */
    CONTROL_ADVERTISE_URL_INVALID(BootErrorDefinitions.CONTROL_ADVERTISE_URL_INVALID),

    /** Member exposure is allowed only through an explicit routable interface in cluster mode. */
    MEMBER_BIND_ADDRESS_INVALID(BootErrorDefinitions.MEMBER_BIND_ADDRESS_INVALID),

    /** A node-session lease with no positive lifetime could never protect a stable node id. */
    NODE_SESSION_TTL_INVALID(BootErrorDefinitions.NODE_SESSION_TTL_INVALID),

    /** Renewal must happen before the node-session lease expires. */
    NODE_SESSION_RENEW_INTERVAL_INVALID(BootErrorDefinitions.NODE_SESSION_RENEW_INTERVAL_INVALID),

    /** A business workload claim needs a positive server-time lease. */
    WORKLOAD_CLAIM_TTL_INVALID(BootErrorDefinitions.WORKLOAD_CLAIM_TTL_INVALID),

    /** Business ownership must renew before its lease expires. */
    WORKLOAD_CLAIM_RENEW_INTERVAL_INVALID(BootErrorDefinitions.WORKLOAD_CLAIM_RENEW_INTERVAL_INVALID),

    /** Cluster mode cannot reserve identities without the Mongo-backed coordination ports. */
    COORDINATION_STORE_REQUIRED(BootErrorDefinitions.COORDINATION_STORE_REQUIRED),

    /** Another live boot already holds this stable node id. */
    NODE_ID_IN_USE(BootErrorDefinitions.NODE_ID_IN_USE),

    /** The reserved identity stopped being provably owned before member startup completed. */
    NODE_SESSION_LOST(BootErrorDefinitions.NODE_SESSION_LOST),

    /** A development-only embedded Web artifact needs explicit runtime confirmation. */
    WEB_DEVELOPMENT_CONFIRMATION_REQUIRED(BootErrorDefinitions.WEB_DEVELOPMENT_CONFIRMATION_REQUIRED),

    /** A development Web marker is malformed or unsupported by this runtime. */
    WEB_DEVELOPMENT_ARTIFACT_INVALID(BootErrorDefinitions.WEB_DEVELOPMENT_ARTIFACT_INVALID);

    private final ProductErrorDefinition definition;

    BootError(ProductErrorDefinition definition) { this.definition = definition; }

    @Override public String code() { return definition.code(); }
    @Override public Severity severity() { return definition.severity(); }
    @Override public Set<String> placeholders() { return definition.parameters(); }
}
