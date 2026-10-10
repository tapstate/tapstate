package io.tapstate.control.restapi;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.tapstate.control.core.CloudClusterContextView;

/** Public projection of the display context bound to the current managed Cloud session. */
public record ClusterContextResponse(
        String organizationId, String clusterId, String organizationName, String clusterName, String region) {

    /** The local session captures display names while the live Cloud runtime state remains unknown. */
    @JsonProperty("clusterState")
    @JsonInclude(JsonInclude.Include.ALWAYS)
    public String clusterState() {
        return null;
    }

    static ClusterContextResponse from(CloudClusterContextView context) {
        return new ClusterContextResponse(context.organizationId(), context.clusterId(), context.organizationName(),
                context.clusterName(), context.region());
    }
}
