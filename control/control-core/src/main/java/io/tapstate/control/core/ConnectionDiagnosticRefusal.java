package io.tapstate.control.core;

import io.tapstate.core.common.DiagnosticProjection;
import io.tapstate.core.common.TapstateException;

import java.util.Map;

/** A failed connection operation, carrying the already authorized surface snapshot. */
public class ConnectionDiagnosticRefusal extends TapstateException {
    private static final long serialVersionUID = 1L;
    private final transient Map<String, Object> diagnostic;

    ConnectionDiagnosticRefusal(TapstateException safeFailure, Map<String, Object> diagnostic) {
        super(safeFailure.code(), safeFailure.args(), null);
        setStackTrace(safeFailure.getStackTrace());
        this.diagnostic = DiagnosticProjection.read(diagnostic);
    }

    public Map<String, Object> diagnostic() {
        return diagnostic == null ? Map.of() : diagnostic;
    }
}
