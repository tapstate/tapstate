package io.tapstate.runtime.srs;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import java.util.Map;

/** A complete, intact retained baseline differs from this source session's actual table structure. */
public final class SrsBaselineModelMismatch extends TapstateException {
    private final String chainId;

    SrsBaselineModelMismatch(String chainId, String table) {
        super(IoError.SRS_MODEL_UNAVAILABLE, Map.of("chain", chainId, "table", table, "version", "0"), null);
        this.chainId = chainId;
    }

    public String chainId() { return chainId; }
}
