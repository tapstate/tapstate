package io.tapstate.control.core;

import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;

/** Only declared storage wrappers participate; untrusted messages and arbitrary cause chains never do. */
final class StorageQuota {
    private StorageQuota() {}

    static boolean confirmed(TapstateException failure) {
        if (quotaCode(failure.code())) return true;
        if (failure.code() == ControlError.STORAGE_TEST_FAILED) {
            Object cause = failure.args().get("causeCode");
            return IoError.STORE_QUOTA_EXCEEDED.code().equals(cause) || ControlError.STORAGE_QUOTA_EXCEEDED.code().equals(cause);
        }
        var cause = auditCause(failure);
        return cause != null && quotaCode(cause.code());
    }

    static boolean obscuresCause(TapstateException failure) {
        if (genericCode(failure.code())) return true;
        if (failure.code() == ControlError.STORAGE_TEST_FAILED && failure.args().get("causeCode") instanceof String cause) {
            return IoError.STORE_UNAVAILABLE.code().equals(cause) || ControlError.STORAGE_UNAVAILABLE.code().equals(cause)
                    || IoError.CACHE_AUTH_FENCED.code().equals(cause) || IoError.CACHE_BUSINESS_FENCED.code().equals(cause);
        }
        var cause = auditCause(failure);
        return cause != null && genericCode(cause.code());
    }

    private static TapstateException auditCause(TapstateException failure) {
        return failure.code() == ControlError.AUDIT_BLOCKED && failure.getCause() instanceof TapstateException coded ? coded : null;
    }
    private static boolean quotaCode(TapstateErrorCode code) {
        return code == IoError.STORE_QUOTA_EXCEEDED || code == ControlError.STORAGE_QUOTA_EXCEEDED;
    }
    private static boolean genericCode(TapstateErrorCode code) {
        return code == IoError.STORE_UNAVAILABLE || code == ControlError.STORAGE_UNAVAILABLE
                || code == IoError.CACHE_AUTH_FENCED || code == IoError.CACHE_BUSINESS_FENCED;
    }
}
