package io.tapstate.spi.store;

/** Conditional-write result; none of the refused outcomes allocate an execution generation. */
public enum ClusterRecoveryMutation {
    APPLIED,
    DUPLICATE,
    NOT_FOUND,
    STALE_ITEM,
    STALE_INTENT,
    STALE_PROFILE,
    STALE_EXECUTION,
    STALE_RECOVERY_CLAIM,
    STALE_PIPELINE_CLAIM,
    TERMINAL,
    WAITING_QUORUM,
    WAITING_PERMIT,
    RETRY_BACKOFF,
    CAPACITY_REFUSED,
    SUCCESSOR_STILL_AUTHORIZED,
    MISSING_STARTUP_RECEIPT
}
