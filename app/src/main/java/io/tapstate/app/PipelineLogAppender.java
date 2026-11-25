package io.tapstate.app;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.AppenderBase;
import io.tapstate.core.logging.LogLine;
import io.tapstate.core.logging.LogSink;
import io.tapstate.core.logging.PipelineAttribution;
import io.tapstate.core.logging.SecretRedactor;

import java.util.Objects;

/**
 * A logging appender that feeds the node-local {@link LogSink}. For each log event carrying a pipeline
 * attribution — the reserved {@code pipeline_id} MDC slot — it records the line against that pipeline so
 * the logs read face can tail it. Events with no pipeline attribution (startup lines, background work not
 * scoped to a pipeline) are ignored, so the sink holds only pipeline-attributed lines. The assembly root
 * attaches one of these to the logging backend alongside the console and file appenders; it does not
 * replace them.
 */
final class PipelineLogAppender extends AppenderBase<ILoggingEvent> {

    /** The MDC attribution slot a log line is filtered by; matches the slot reserved in the log format. */
    static final String PIPELINE_ID_MDC_KEY = PipelineAttribution.MDC_KEY;

    private final LogSink sink;
    private final SecretRedactor redactor;

    PipelineLogAppender(LogSink sink, SecretRedactor redactor) {
        this.sink = Objects.requireNonNull(sink, "sink");
        this.redactor = Objects.requireNonNull(redactor, "redactor");
    }

    @Override
    protected void append(ILoggingEvent event) {
        // Optional profiling belongs to process logs, not the operator's bounded Pipeline log tail.
        if (PipelineDiagnostics.LOGGER_NAME.equals(event.getLoggerName())) return;
        String pipelineId = event.getMDCPropertyMap().get(PIPELINE_ID_MDC_KEY);
        if (pipelineId == null || pipelineId.isBlank()) {
            return;
        }
        var line = new LogLine(event.getTimeStamp(), event.getLevel().toString(),
                redactor.redactLog(render(event)), diagnostic(event, redactor));
        String generation = event.getMDCPropertyMap().get(PipelineAttribution.STATE_GENERATION_MDC_KEY);
        if (generation == null) {
            sink.append(pipelineId, line);
            return;
        }
        long original;
        try {
            original = Long.parseLong(generation);
        } catch (NumberFormatException invalid) {
            return;
        }
        if (original > 0) {
            sink.append(pipelineId, original, line);
        }
    }

    static java.util.Map<String, Object> diagnostic(ILoggingEvent event, SecretRedactor redactor) {
        var fields = event.getKeyValuePairs();
        if (fields == null) return java.util.Map.of();
        for (var field : fields) {
            if ("diagnostic".equals(field.key)) {
                var checked = io.tapstate.core.common.RuntimeDiagnosticProjection.read(field.value);
                var bounded = io.tapstate.core.logging.LogDiagnosticProjection.fit(checked);
                return io.tapstate.core.logging.LogDiagnosticProjection.fit(redactFields(bounded, redactor, false));
            }
        }
        return java.util.Map.of();
    }

    private static java.util.Map<String, Object> redactFields(java.util.Map<?, ?> fields, SecretRedactor redactor, boolean parameterNames) {
        var safe = new java.util.LinkedHashMap<String, Object>();
        fields.forEach((key, value) -> {
            String name = (String) key;
            Object redacted = "params".equals(name) && !parameterNames && value instanceof java.util.Map<?, ?> params
                    ? redactFields(params, redactor, true) : redactValue(value, redactor);
            safe.put(parameterNames ? redactor.redactLog(name) : name, redacted);
        });
        return safe;
    }

    private static Object redactValue(Object value, SecretRedactor redactor) {
        if (value instanceof String text) return redactor.redactLog(text);
        if (value instanceof java.util.Map<?, ?> fields) return redactFields(fields, redactor, false);
        if (value instanceof java.util.List<?> values) return values.stream().map(item -> redactValue(item, redactor)).toList();
        return value;
    }

    /** The formatted message, with the exception's stack trace appended when the event carries one. */
    private static String render(ILoggingEvent event) {
        String message = event.getFormattedMessage();
        IThrowableProxy throwable = event.getThrowableProxy();
        return throwable == null
                ? message
                : message + System.lineSeparator() + ThrowableProxyUtil.asString(throwable);
    }
}
