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
    private int bootstrapMinMembers = 3;
    private Duration nodeSessionTtl = Duration.ofSeconds(30);
    private Duration nodeSessionRenewInterval = Duration.ofSeconds(10);
    private Duration workloadClaimTtl = Duration.ofSeconds(30);
    private Duration workloadClaimRenewInterval = Duration.ofSeconds(10);
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
    }

    Duration getWorkloadClaimRenewInterval() {
        return workloadClaimRenewInterval;
    }

    void setWorkloadClaimRenewInterval(Duration workloadClaimRenewInterval) {
        this.workloadClaimRenewInterval = workloadClaimRenewInterval;
    }

    Duration getMembershipReconcileInterval() {
        return membershipReconcileInterval;
    }

    void setMembershipReconcileInterval(Duration membershipReconcileInterval) {
        this.membershipReconcileInterval = membershipReconcileInterval;
    }
}
