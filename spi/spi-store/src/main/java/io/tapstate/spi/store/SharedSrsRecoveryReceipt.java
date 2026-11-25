package io.tapstate.spi.store;

/** Store-issued proof of an exact CACHE result; rechecking CONFIG and CACHE never implies a joint transaction. */
public interface SharedSrsRecoveryReceipt {
    enum Outcome { RETIRED, CANCELLED }
    SharedSrsRecoveryPreparation preparation();
    Outcome outcome();
    void requireCurrent();
    /** A fresh typed observation for the same cancelled namespace; values alone confer no retry authority. */
    default SharedSrsRecoverySnapshot cancelledSnapshot() {
        throw new io.tapstate.core.common.TapstateException(IoError.CACHE_BUSINESS_FENCED,java.util.Map.of(),null);
    }
}
