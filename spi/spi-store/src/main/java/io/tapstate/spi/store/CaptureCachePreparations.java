package io.tapstate.spi.store;

/** Controlled first history insertion and load-only continuation using the original CACHE client. */
public interface CaptureCachePreparations {
    CaptureCacheState createFirst(FirstCaptureCreation original);
    CaptureCacheState load(CapturePreparation original);

    default CaptureClearCache explicitClear() {
        throw new io.tapstate.core.common.TapstateException(IoError.CAPTURE_EXTERNAL_RETIREMENT_UNSUPPORTED,
                java.util.Map.of("capture", "unproven", "operation", "clear", "reason", "clear-cache-unavailable"), null);
    }
}
