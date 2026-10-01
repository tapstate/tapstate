package io.tapstate.app;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.Severity;

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
    HAZELCAST_UNAVAILABLE("boot.hazelcast-unavailable", Set.of()),

    /** A managed Cloud runtime must receive its four external settings as one complete unit. */
    CLOUD_CONFIG_INCOMPLETE("boot.cloud-config-incomplete", Set.of()),

    /** The configured Global Control Plane address is not an absolute HTTP(S) base URL. */
    CLOUD_BASE_URL_INVALID("boot.cloud-base-url-invalid", Set.of()),

    /** The managed metadata connection is not a MongoDB or MongoDB SRV connection string. */
    CLOUD_ATLAS_URI_INVALID("boot.cloud-atlas-uri-invalid", Set.of()),

    /** A Web-bearing distribution has missing, unsupported, malformed, or conflicting metadata. */
    WEB_PROFILE_INVALID("boot.web-profile-invalid", Set.of("reason")),

    /** The packaged Web profile does not match the mode selected by the external Cloud settings. */
    WEB_PROFILE_MODE_MISMATCH("boot.web-profile-mode-mismatch", Set.of("profile", "mode")),

    /** Cloud Web metadata requires a safe HTTPS Console URL; on-prem must carry no Console URL. */
    WEB_CONSOLE_URL_INVALID("boot.web-console-url-invalid", Set.of()),

    /** The optional on-prem store switch cannot disable managed Cloud metadata persistence. */
    CLOUD_STORE_REQUIRED("boot.cloud-store-required", Set.of()),

    /** The outbound status switch is malformed; its value is deliberately not echoed. */
    CLOUD_STATUS_CONFIG_INVALID("boot.cloud-status-config-invalid", Set.of()),

    /** An on-prem process attempted to enable managed Cloud status reporting. */
    CLOUD_STATUS_MODE_REQUIRED("boot.cloud-status-mode-required", Set.of()),

    /** Status reporting was explicitly enabled without the real SDK-backed reporter. */
    CLOUD_STATUS_SDK_REQUIRED("boot.cloud-status-sdk-required", Set.of()),

    /** A Cloud connector release is missing, mismatched, or cannot be fully registered and loaded. */
    CLOUD_CONNECTORS_INVALID("boot.cloud-connectors-invalid", Set.of("connector", "reason")),

    /** The selected member-discovery mode is missing a required, deterministic input. */
    DISCOVERY_CONFIG_INVALID("boot.discovery-config-invalid", Set.of("detail")),

    /** Cluster mode was selected without the stable cluster id that names every coordination record. */
    CLUSTER_ID_REQUIRED("boot.cluster-id-required", Set.of()),

    /** The configured cluster id cannot be represented by the stable identity contract. */
    CLUSTER_ID_INVALID("boot.cluster-id-invalid", Set.of()),

    /** The configured cluster id disagrees with the identity already stored for this control store. */
    CLUSTER_ID_MISMATCH("boot.cluster-id-mismatch", Set.of("configured", "stored")),

    /** The selected cluster profile and bootstrap member count cannot provide its promised safety. */
    CLUSTER_PROFILE_INVALID("boot.cluster-profile-invalid", Set.of()),

    /** Member heartbeats require positive whole-second values and a longer failure-detection window. */
    HEARTBEAT_CONFIG_INVALID("boot.heartbeat-config-invalid", Set.of()),

    /** Cluster mode was selected without this member's stable node id. */
    NODE_ID_REQUIRED("boot.node-id-required", Set.of()),

    /** The advertised control endpoint is missing or is not an absolute HTTP(S) URL. */
    CONTROL_ADVERTISE_URL_INVALID("boot.control-advertise-url-invalid", Set.of()),

    /** Member exposure is allowed only through an explicit routable interface in cluster mode. */
    MEMBER_BIND_ADDRESS_INVALID("boot.member-bind-address-invalid", Set.of()),

    /** A node-session lease with no positive lifetime could never protect a stable node id. */
    NODE_SESSION_TTL_INVALID("boot.node-session-ttl-invalid", Set.of()),

    /** Renewal must happen before the node-session lease expires. */
    NODE_SESSION_RENEW_INTERVAL_INVALID("boot.node-session-renew-interval-invalid", Set.of()),

    /** A business workload claim needs a positive server-time lease. */
    WORKLOAD_CLAIM_TTL_INVALID("boot.workload-claim-ttl-invalid", Set.of()),

    /** Business ownership must renew before its lease expires. */
    WORKLOAD_CLAIM_RENEW_INTERVAL_INVALID("boot.workload-claim-renew-interval-invalid", Set.of()),

    /** Cluster mode cannot reserve identities without the Mongo-backed coordination ports. */
    COORDINATION_STORE_REQUIRED("boot.coordination-store-required", Set.of()),

    /** Another live boot already holds this stable node id. */
    NODE_ID_IN_USE("boot.node-id-in-use", Set.of("nodeId"));

    private final String code;
    private final Set<String> placeholders;

    BootError(String code, Set<String> placeholders) {
        this.code = code;
        this.placeholders = placeholders;
    }

    @Override
    public String code() {
        return code;
    }

    @Override
    public Severity severity() {
        return Severity.ERROR;
    }

    @Override
    public Set<String> placeholders() {
        return placeholders;
    }
}
