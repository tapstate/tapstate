package io.tapstate.spi.store;

/** Store-issued proof of an exact CACHE result; rechecking CONFIG and CACHE never implies a joint transaction. */
public interface SharedSrsRecoveryReceipt {
    enum Outcome { RETIRED, CANCELLED }
    SharedSrsRecoveryPreparation preparation();
    Outcome outcome();
    void requireCurrent();
}
