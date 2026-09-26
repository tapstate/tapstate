package io.tapstate.app;

import ch.qos.logback.classic.Logger;
import io.tapstate.core.logging.LogLine;
import io.tapstate.core.logging.LogSink;
import io.tapstate.core.logging.PipelineAttribution;
import io.tapstate.core.logging.RingBufferLogSink;
import io.tapstate.core.logging.SecretRedactor;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The logback appender that feeds the node-local log sink. It captures a log event into the sink,
 * attributed to the pipeline named in the reserved {@code pipeline_id} MDC slot; an event with no
 * pipeline attribution is ignored, so the sink holds only pipeline-attributed lines.
 */
class PipelineLogAppenderTest {

    private static final String MDC_KEY = "pipeline_id";

    /** Runs {@code body} with the appender attached to a private logger, then detaches and cleans up. */
    private void withAppender(RingBufferLogSink sink, String loggerName, java.util.function.Consumer<Logger> body) {
        Logger logger = (Logger) LoggerFactory.getLogger(loggerName);
        PipelineLogAppender appender = new PipelineLogAppender(sink, new SecretRedactor());
        appender.setContext(logger.getLoggerContext());
        appender.start();
        logger.addAppender(appender);
        try {
            body.accept(logger);
        } finally {
            MDC.remove(MDC_KEY);
            MDC.remove(PipelineAttribution.INCARNATION_MDC_KEY);
            MDC.remove(PipelineAttribution.EXECUTION_MDC_KEY);
            logger.detachAppender(appender);
            appender.stop();
        }
    }

    @Test
    void capturesTheEmittingExecutionWithoutExposingItsIdentityInTheLine() {
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);
        withAppender(sink, "test.logs.scoped", logger -> {
            MDC.put(MDC_KEY, "orders");
            MDC.put(PipelineAttribution.INCARNATION_MDC_KEY, "resource-a");
            MDC.put(PipelineAttribution.EXECUTION_MDC_KEY, "7");
            logger.info("execution line");
        });

        assertThat(sink.tail("orders")).isEmpty();
        assertThat(sink.tail("orders", new LogSink.Scope("resource-a", 7)))
                .extracting(LogLine::message).containsExactly("execution line");
    }

    @Test
    void capturesAPipelineAttributedLineIntoTheSink() {
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);

        withAppender(sink, "test.logs.attributed", logger -> {
            MDC.put(MDC_KEY, "orders_sync");
            logger.info("converged to RUNNING");
        });

        assertThat(sink.tail("orders_sync")).extracting(LogLine::message).containsExactly("converged to RUNNING");
        assertThat(sink.tail("orders_sync")).first().satisfies(line -> {
            assertThat(line.level()).isEqualTo("INFO");
            assertThat(line.timestampMillis()).isPositive();
        });
    }

    @Test
    void ignoresALineWithNoPipelineAttribution() {
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);

        withAppender(sink, "test.logs.unattributed", logger -> logger.info("a boot line with no pipeline"));

        // No MDC pipeline id was set, so the line is not captured under any pipeline.
        assertThat(sink.tail("orders_sync")).isEmpty();
    }

    @Test
    void ignoresALineWithABlankPipelineAttribution() {
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);

        withAppender(sink, "test.logs.blank", logger -> {
            MDC.put(MDC_KEY, "   ");
            logger.info("a line whose pipeline slot is blank");
        });

        // A blank (empty / whitespace) pipeline id is treated as no attribution, not a pipeline named "   ".
        assertThat(sink.tail("   ")).isEmpty();
    }

    @Test
    void includesTheStackTraceOfALoggedException() {
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);

        withAppender(sink, "test.logs.throwable", logger -> {
            MDC.put(MDC_KEY, "pl-err");
            logger.error("converge failed", new IllegalStateException("boom"));
        });

        // A logs tail is the operator diagnostic stream: an exception's cause must ride into the line, not be
        // dropped, so the tail shows why a pipeline failed.
        assertThat(sink.tail("pl-err")).singleElement().satisfies(line -> {
            assertThat(line.message()).contains("converge failed");
            assertThat(line.message()).contains("IllegalStateException").contains("boom");
        });
    }

    @Test
    void redactsRegisteredSecretsBeforeWritingToTheSink() {
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);
        SecretRedactor redactor = new SecretRedactor();
        redactor.replace("source-orders", java.util.List.of("sentinel-password"));
        Logger logger = (Logger) LoggerFactory.getLogger("test.logs.redacted");
        PipelineLogAppender appender = new PipelineLogAppender(sink, redactor);
        appender.setContext(logger.getLoggerContext());
        appender.start();
        logger.addAppender(appender);
        try {
            MDC.put(MDC_KEY, "orders");
            logger.warn("connector rejected {}", "sentinel-password");
        } finally {
            MDC.remove(MDC_KEY);
            logger.detachAppender(appender);
            appender.stop();
        }

        assertThat(sink.tail("orders")).singleElement().satisfies(line ->
                assertThat(line.message())
                        .isEqualTo("connector rejected ********")
                        .doesNotContain("sentinel-password"));
    }
}
