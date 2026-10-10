package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Original registered read diagnostic, retained without rebuilding its enum across a boundary. */
public record ClusterRecoveryReadFailure(String code, Map<String, Object> params) {
    public ClusterRecoveryReadFailure {
        Objects.requireNonNull(code, "code");
        params = Collections.unmodifiableMap(new LinkedHashMap<>(Objects.requireNonNull(params, "params")));
    }

    public static ClusterRecoveryReadFailure from(TapstateException error) {
        return new ClusterRecoveryReadFailure(error.code().code(), error.args());
    }
}
