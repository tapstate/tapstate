package io.tapstate.spi.store;

/** Controlled first history insertion and load-only continuation using the original CACHE client. */
public interface CaptureCachePreparations {
    CaptureCacheState createFirst(FirstCaptureCreation original);
    CaptureCacheState load(CapturePreparation original);
}
