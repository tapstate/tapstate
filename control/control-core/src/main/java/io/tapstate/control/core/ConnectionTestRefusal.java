package io.tapstate.control.core;

import io.tapstate.core.common.DiagnosticProjection;
import io.tapstate.core.common.TapstateException;

import java.util.Map;

/** A connection test that could not run, carrying the already authorized surface snapshot. */
public final class ConnectionTestRefusal extends TapstateException {
    private static final long serialVersionUID = 1L;
    private final transient Map<String, Object> diagnostic;

    ConnectionTestRefusal(TapstateException safeFailure, Map<String, Object> diagnostic) {
        super(safeFailure.code(), safeFailure.args(), null);
        setStackTrace(safeFailure.getStackTrace());
        this.diagnostic = DiagnosticProjection.read(diagnostic);
    }

    public Map<String, Object> diagnostic() {
        return diagnostic == null ? Map.of() : diagnostic;
    }
}
