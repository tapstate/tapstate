package io.tapstate.spi.store;

/** Recovery progress is independent from pipeline state and member membership. */
public enum ClusterRecoveryStatus {
    WAITING_QUORUM,
    WAITING_PERMIT,
    REBUILDING,
    RETRY_BACKOFF,
    REBUILD_FAILED,
    RECOVERED,
    CANCELLED;

    public boolean terminal() {
        return this == REBUILD_FAILED || this == RECOVERED || this == CANCELLED;
    }
}
