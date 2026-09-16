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
        String pipelineId = event.getMDCPropertyMap().get(PIPELINE_ID_MDC_KEY);
        if (pipelineId == null || pipelineId.isBlank()) {
            return;
        }
        sink.append(pipelineId,
                new LogLine(event.getTimeStamp(), event.getLevel().toString(), redactor.redact(render(event))));
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
