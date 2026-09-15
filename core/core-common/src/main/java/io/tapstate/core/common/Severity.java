package io.tapstate.core.common;

/**
 * Severity of a {@link TapstateErrorCode}. Two values to start; consumers are
 * the CLI exit code ({@code ERROR} → non-zero), the {@code -o json} field, and the log level.
 * No value is added without a second consumer — {@code recoverable}/{@code skippable} were
 * deliberately cut until a runtime (engine) consumer exists, to avoid dead metadata.
 */
public enum Severity {
    ERROR,
    WARNING
}
