package io.tapstate.app;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/** Stable cluster and node identity plus the pre-join node-session lease budget. */
@ConfigurationProperties(prefix = "tapstate.cluster")
class ClusterProperties {

    enum Profile {
        SINGLE,
        PROCESS_FAILURE_ONLY,
        PRODUCTION_HA
    }

    private String id;
    private String nodeId;
    private Profile profile = Profile.SINGLE;
    private Boolean workloadLeasesEnabled;
    private int bootstrapMinMembers = 3;
    private Duration nodeSessionTtl = Duration.ofSeconds(30);
    private Duration nodeSessionRenewInterval = Duration.ofSeconds(10);
    private Duration workloadClaimTtl = Duration.ofSeconds(30);
    private Duration workloadClaimRenewInterval = Duration.ofSeconds(10);
    private boolean workloadClaimTimingConfigured;
    private Duration membershipReconcileInterval = Duration.ofSeconds(1);

    String getId() {
        return id;
    }

    void setId(String id) {
        this.id = id;
    }

    String getNodeId() {
        return nodeId;
    }

    void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    Profile getProfile() {
        return profile;
    }

    void setProfile(Profile profile) {
        this.profile = profile;
    }

    public Boolean getWorkloadLeasesEnabled() { return workloadLeasesEnabled; }

    public void setWorkloadLeasesEnabled(Boolean enabled) { workloadLeasesEnabled = enabled; }

    enum WorkloadLeases { INDEPENDENT, NODE_BACKED }

    WorkloadLeases workloadLeases(boolean cloud, boolean op, HazelcastProperties.DiscoveryMode discovery) {
        boolean singleOp = !cloud && op && profile == Profile.SINGLE && discovery == HazelcastProperties.DiscoveryMode.NONE;
        if (!singleOp && Boolean.FALSE.equals(workloadLeasesEnabled))
            throw new IllegalArgumentException("tapstate.cluster.workload-leases-enabled=false requires single-node OP with discovery NONE");
        return singleOp && !Boolean.TRUE.equals(workloadLeasesEnabled) ? WorkloadLeases.NODE_BACKED : WorkloadLeases.INDEPENDENT;
    }

    int getBootstrapMinMembers() {
        return bootstrapMinMembers;
    }

    void setBootstrapMinMembers(int bootstrapMinMembers) {
        this.bootstrapMinMembers = bootstrapMinMembers;
    }

    Duration getNodeSessionTtl() {
        return nodeSessionTtl;
    }

    void setNodeSessionTtl(Duration nodeSessionTtl) {
        this.nodeSessionTtl = nodeSessionTtl;
    }

    Duration getNodeSessionRenewInterval() {
        return nodeSessionRenewInterval;
    }

    void setNodeSessionRenewInterval(Duration nodeSessionRenewInterval) {
        this.nodeSessionRenewInterval = nodeSessionRenewInterval;
    }

    Duration getWorkloadClaimTtl() {
        return workloadClaimTtl;
    }

    void setWorkloadClaimTtl(Duration workloadClaimTtl) {
        this.workloadClaimTtl = workloadClaimTtl;
        workloadClaimTimingConfigured = true;
    }

    Duration getWorkloadClaimRenewInterval() {
        return workloadClaimRenewInterval;
    }

    void setWorkloadClaimRenewInterval(Duration workloadClaimRenewInterval) {
        this.workloadClaimRenewInterval = workloadClaimRenewInterval;
        workloadClaimTimingConfigured = true;
    }

    Duration pipelineClaimTtl(boolean cloud) {
        return cloud && !workloadClaimTimingConfigured ? Duration.ofSeconds(60) : workloadClaimTtl;
    }

    Duration pipelineClaimRenewInterval(boolean cloud) {
        return cloud && !workloadClaimTimingConfigured ? Duration.ofSeconds(15) : workloadClaimRenewInterval;
    }

    Duration getMembershipReconcileInterval() {
        return membershipReconcileInterval;
    }

    void setMembershipReconcileInterval(Duration membershipReconcileInterval) {
        this.membershipReconcileInterval = membershipReconcileInterval;
    }
}
