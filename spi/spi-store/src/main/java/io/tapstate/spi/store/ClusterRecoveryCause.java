package io.tapstate.spi.store;

/** The closed set of failures that authorize automatic topology recovery. */
public enum ClusterRecoveryCause {
    MEMBER_LOSS,
    FULL_CLUSTER_RESTART
}
