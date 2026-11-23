package io.tapstate.core.common;

import java.util.Map;
import java.util.Objects;

/** The original in-process failure and safe public snapshots captured by the same owner. */
public record FailureFact(Throwable cause, Map<String, Object> diagnostic, PublicFailure publicFailure) {
    public FailureFact {
        Objects.requireNonNull(cause, "cause");
        diagnostic = RuntimeDiagnosticProjection.fit(diagnostic);
    }

    public FailureFact(Throwable cause, Map<String, Object> diagnostic) {
        this(cause, diagnostic, null);
    }

    public FailureFact(Throwable cause) {
        this(cause, Map.of(), null);
    }

    public static FailureFact unavailable(Throwable cause) {
        return new FailureFact(cause, Map.of("truncated", true), new PublicFailure("", Map.of()));
    }

    /** Already-redacted values for the first coded cause, preserving its identity and named keys. */
    public record PublicFailure(String code, Map<String, String> params) {
        public PublicFailure {
            Objects.requireNonNull(code, "code");
            params = Map.copyOf(params);
        }
    }
}
