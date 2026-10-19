package io.tapstate.spi.store;

/** An implementation-private non-serializable single invocation issued only after CONFIG commits its first CAS. */
public interface FirstCaptureCreation {
    CapturePreparation preparation();
    CapturePreparationRequest request();
    String retention();
    void consume();
    void requireActive();
}
