package io.tapstate.app;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/** Stable cluster and node identity plus the pre-join node-session lease budget. */
@ConfigurationProperties(prefix = "tapstate.cluster")
class ClusterProperties {

    private String id;
    private String nodeId;
    private Duration nodeSessionTtl = Duration.ofSeconds(30);
    private Duration nodeSessionRenewInterval = Duration.ofSeconds(10);

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
}
