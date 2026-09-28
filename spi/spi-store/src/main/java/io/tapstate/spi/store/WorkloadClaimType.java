package io.tapstate.spi.store;

/** Every owner-controlled cluster resource uses the same durable claim contract. */
public enum WorkloadClaimType {
    NODE_SESSION,
    PIPELINE_ACTUATION,
    CAPTURE,
    CLUSTER_RECOVERY,
    CLUSTER_OPERATIONS
}
