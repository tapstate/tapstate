package io.tapstate.spi.store;

import java.util.Objects;

/** Verified retained CONFIG history, with no CACHE insertion or workload authority. */
public record CaptureSharedHistory(CapturePreparation preparation, boolean positionEditsRestricted) {
    public CaptureSharedHistory {
        Objects.requireNonNull(preparation, "preparation");
        if (preparation.mode() != CapturePreparation.Mode.SHARED_SRS)
            throw new IllegalArgumentException("shared history requires a shared capture");
    }
}
