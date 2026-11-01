package io.tapstate.spi.store;

import java.util.Optional;

/** Each command and receipt belongs to one actual original CACHE transaction domain. */
public interface CaptureClearCache {
    default CaptureStorageUpgrade.CacheObservation observeLegacyUpgrade(java.util.List<CapturePreparation> captures) {
        throw new io.tapstate.core.common.TapstateException(IoError.CAPTURE_EXTERNAL_RETIREMENT_UNSUPPORTED,
                java.util.Map.of("capture","legacy","operation","upgrade","reason","unsupported-layout"),null);
    }
    CaptureClearObservation inspect(CapturePreparation original);
    Receipt freeze(CaptureClearPreparation original, WorkloadClaimFence node);
    Optional<Receipt> frozen(CaptureClearPreparation original);
    Receipt apply(CaptureClearPreparation original, WorkloadClaimFence node);
    Optional<Receipt> applied(CaptureClearPreparation original);
    Receipt cancel(CaptureClearPreparation original, WorkloadClaimFence node);
    boolean reclaim(CaptureClearPreparation original,WorkloadClaimFence node);
    interface Receipt {
        CaptureClearPreparation preparation();
        String hash();
        Outcome outcome();
        void requireCurrent();
    }
    enum Outcome { FROZEN, APPLIED, CANCELLED }
}
