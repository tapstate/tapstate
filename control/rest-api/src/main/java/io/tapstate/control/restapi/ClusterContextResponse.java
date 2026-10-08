package io.tapstate.control.restapi;

import io.tapstate.spi.store.CloudSessionContext;

/** Public projection of the display context bound to the current managed Cloud session. */
public record ClusterContextResponse(
        String organizationId, String clusterId, String organizationName, String clusterName, String region) {

    static ClusterContextResponse from(CloudSessionContext context) {
        return new ClusterContextResponse(context.organizationId(), context.clusterId(), context.organizationName(),
                context.clusterName(), context.region());
    }
}
