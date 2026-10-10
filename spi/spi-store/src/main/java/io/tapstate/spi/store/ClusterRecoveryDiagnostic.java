package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Persisted coded cause, named arguments, original position and actionable disposition. */
public record ClusterRecoveryDiagnostic(
        Reason reason, String code, Map<String, Object> params,
        Map<String, ClusterRecoveryPosition> positions, String disposition) {
    public enum Reason {
        CAPACITY_REFUSED,
        REBUILD_REFUSED,
        EXECUTION_FAILED,
        SOURCE_POSITION_REJECTED
    }

    public ClusterRecoveryDiagnostic {
        reason = Objects.requireNonNull(reason, "reason");
        code = ClusterRecoveryKey.required(code, "code");
        if (!code.matches("[a-z][a-z0-9-]*(?:\\.[a-z0-9][a-z0-9-]*)+")) {
            throw new IllegalArgumentException("diagnostic code must be canonical");
        }
        params = Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(params, "params")));
        positions = Map.copyOf(Objects.requireNonNull(positions, "positions"));
        disposition = ClusterRecoveryKey.required(disposition, "disposition");
    }

    /** Carry the registered code string across persistence without serializing its enum. */
    public static ClusterRecoveryDiagnostic from(Reason reason, TapstateException error,
            Map<String, ClusterRecoveryPosition> positions, String disposition) {
        Objects.requireNonNull(error, "error");
        if (!error.args().keySet().containsAll(error.code().placeholders())) {
            throw new IllegalArgumentException("diagnostic arguments do not satisfy the error code");
        }
        return new ClusterRecoveryDiagnostic(reason, error.code().code(), error.args(), positions, disposition);
    }
}
