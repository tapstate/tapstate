package io.tapstate.spi.store;

import java.util.Objects;

/** Delete and recreate cannot inherit the prior incarnation's recovery item. */
public record ClusterRecoveryKey(String clusterId, String pipelineId, String incarnation) {
    public ClusterRecoveryKey {
        clusterId = required(clusterId, "clusterId");
        pipelineId = required(pipelineId, "pipelineId");
        incarnation = required(incarnation, "incarnation");
    }

    static String required(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
