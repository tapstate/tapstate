package io.tapstate.spi.store;

/** One original reservation and one CACHE transaction; values alone never grant physical replacement. */
public interface SharedSrsCacheRecoveries {
    /** Reads only an exact already-applied result; absence grants no creation, cancellation or redirection authority. */
    default java.util.Optional<SharedSrsRecoveryReceipt> applied(SharedSrsRecoveryPreparation original) {
        throw new io.tapstate.core.common.TapstateException(IoError.CACHE_BUSINESS_FENCED,java.util.Map.of(),null);
    }
    SharedSrsRecoveryReceipt replace(SharedSrsRecoveryPreparation original,WorkloadClaimFence currentNode);
    SharedSrsRecoveryReceipt cancel(SharedSrsRecoveryPreparation original,WorkloadClaimFence currentNode);
}
