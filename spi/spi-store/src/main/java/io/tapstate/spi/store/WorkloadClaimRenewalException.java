package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateException;
import java.util.Map;

/** Reports whether a failed renewal can be retried or must be read back first. */
public final class WorkloadClaimRenewalException extends TapstateException {
    public enum Reason { WRITE_CONFLICT, UNCONFIRMED }

    private final Reason reason;

    public WorkloadClaimRenewalException(Reason reason, String detail) {
        super(IoError.STORE_UNAVAILABLE, Map.of("detail", detail), null);
        this.reason = reason;
    }

    public Reason reason() { return reason; }
}
