package io.tapstate.cli;

/**
 * One operational log line as read back from the server: when it was logged, its level, and the message.
 * This mirrors the server's per-line shape independently (rule R6: the CLI carries no shared control type).
 *
 * @param timestampMillis when the line was logged, epoch milliseconds
 * @param level           the log level (for example {@code INFO}, {@code WARN}, {@code ERROR})
 * @param message         the formatted log message
 */
record RemoteLogLine(long timestampMillis, String level, String message, java.util.Map<String, Object> diagnostic) {
    RemoteLogLine(long timestampMillis, String level, String message) {
        this(timestampMillis, level, message, java.util.Map.of());
    }
    RemoteLogLine {
        diagnostic = io.tapstate.core.common.RuntimeDiagnosticProjection.read(diagnostic);
    }
}
