package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import java.util.Map;

/** A refused connection test with its sanitized diagnostic. */
public final class ConnectionTestRefusal extends ConnectionDiagnosticRefusal {
    private static final long serialVersionUID = 1L;

    ConnectionTestRefusal(TapstateException failure, Map<String, Object> diagnostic) {
        super(failure, diagnostic);
    }
}
