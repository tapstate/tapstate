package io.tapstate.core.logging;

import java.util.Locale;

/** Minimum severity retained for one pipeline's node-local log view. */
public enum PipelineLogLevel {
    ERROR(40), WARN(30), INFO(20), DEBUG(10), TRACE(0);

    private final int severity;

    PipelineLogLevel(int severity) {
        this.severity = severity;
    }

    public boolean accepts(String level) {
        try {
            return severityOf(level) >= severity;
        } catch (IllegalArgumentException ignored) {
            return true;
        }
    }

    public static PipelineLogLevel parse(String value) {
        return valueOf(value.toUpperCase(Locale.ROOT));
    }

    private static int severityOf(String value) {
        return switch (value.toUpperCase(Locale.ROOT)) {
            case "FATAL", "ERROR" -> 40;
            case "WARN", "WARNING" -> 30;
            case "INFO" -> 20;
            case "DEBUG" -> 10;
            case "TRACE" -> 0;
            default -> throw new IllegalArgumentException("Unknown log level: " + value);
        };
    }
}
