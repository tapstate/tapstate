package io.tapstate.core.logging;

import io.tapstate.core.common.FailureFact;
import io.tapstate.core.common.RuntimeDiagnosticProjection;
import java.util.LinkedHashMap;
import java.util.Map;

/** Keeps safe captured details, or message-free actual stack frames when capture was unavailable. */
public final class FailureLogSnapshot {
    private FailureLogSnapshot() {}

    public static Map<String, Object> of(FailureFact fact) {
        if (fact.diagnostic().containsKey("stackTrace")) return LogDiagnosticProjection.fit(fact.diagnostic());
        var fields = new LinkedHashMap<String, Object>(fact.diagnostic());
        var stack = new StringBuilder();
        Throwable current = fact.cause();
        int lines = 0;
        try {
            while (current != null && lines < RuntimeDiagnosticProjection.MAX_STACK_FRAMES) {
                if (!stack.isEmpty()) stack.append("\nCaused by: ");
                stack.append(current.getClass().getName());
                lines++;
                for (var frame : current.getStackTrace()) {
                    if (lines >= RuntimeDiagnosticProjection.MAX_STACK_FRAMES) break;
                    stack.append("\n\tat ").append(frame);
                    lines++;
                }
                var next = current.getCause();
                current = next == current ? null : next;
            }
        } catch (RuntimeException | LinkageError unavailable) {
            // Optional diagnostics must not replace the original operation failure.
        }
        if (stack.length() > RuntimeDiagnosticProjection.MAX_TEXT) stack.setLength(RuntimeDiagnosticProjection.MAX_TEXT);
        fields.put("stackTrace", stack.toString());
        fields.put("truncated", true);
        return LogDiagnosticProjection.fit(fields);
    }
}
