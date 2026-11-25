package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateException;
import java.util.Map;

/** Reports whether a failed renewal can be retried or must be read back first. */
public final class WorkloadClaimRenewalException extends TapstateException {
    public enum Reason { WRITE_CONFLICT, UNCONFIRMED }

    private final Reason reason;
    private final boolean writeConflict;

    public WorkloadClaimRenewalException(Reason reason, String detail) {
        this(reason, detail, reason == Reason.WRITE_CONFLICT);
    }

    private WorkloadClaimRenewalException(Reason reason, String detail, boolean writeConflict) {
        super(IoError.STORE_UNAVAILABLE, Map.of("detail", detail), null);
        this.reason = reason;
        this.writeConflict = writeConflict;
    }

    public static WorkloadClaimRenewalException writeConflict(boolean transactionReleased) {
        return new WorkloadClaimRenewalException(transactionReleased ? Reason.WRITE_CONFLICT : Reason.UNCONFIRMED,
                "Mongo Pipeline renewal WriteConflict code=112", true);
    }

    public Reason reason() { return reason; }

    public boolean isWriteConflict() { return writeConflict; }
}
