package io.tapstate.spi.store;

/** An implementation-private non-serializable single invocation issued only after CONFIG commits its first CAS. */
public interface FirstCaptureCreation {
    CapturePreparation preparation();
    CapturePreparationRequest request();
    String retention();
    /** This CONFIG commit proved that the new shared capture has no earlier incarnation. */
    default boolean newSharedHistory() { return false; }
    void consume();
    void requireActive();
}
