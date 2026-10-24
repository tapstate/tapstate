package io.tapstate.spi.store;

/** One original reservation and one CACHE transaction; values alone never grant physical replacement. */
public interface SharedSrsCacheRecoveries {
    SharedSrsRecoveryReceipt replace(SharedSrsRecoveryPreparation original,WorkloadClaimFence currentNode);
    SharedSrsRecoveryReceipt cancel(SharedSrsRecoveryPreparation original,WorkloadClaimFence currentNode);
}
