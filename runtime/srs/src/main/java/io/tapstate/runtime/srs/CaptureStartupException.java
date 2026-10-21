package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CaptureStartupFailure;
import java.util.Map;

/** Coded surface carrier retaining the original remote source failure without rebuilding an enum. */
public final class CaptureStartupException extends TapstateException {
    private final CaptureStartupFailure failure;

    public CaptureStartupException(CaptureStartupFailure failure) {
        super(CaptureError.READER_STARTUP_FAILED, Map.of("pipeline", failure.pipelineClaim().key().resourceId(),
                "source", failure.witness().sourceId(), "code", failure.code()), null);
        this.failure = failure;
    }
    public CaptureStartupFailure failure() { return failure; }
}
