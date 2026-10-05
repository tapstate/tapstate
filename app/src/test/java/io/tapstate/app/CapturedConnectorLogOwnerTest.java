package io.tapstate.app;

import ch.qos.logback.classic.Logger;
import io.tapstate.adapters.pdk.SyntheticCaptureLogJars;
import io.tapstate.core.logging.LogLine;
import io.tapstate.core.logging.LogSink;
import io.tapstate.core.logging.PipelineAttribution;
import io.tapstate.core.logging.RingBufferLogSink;
import io.tapstate.core.logging.SecretRedactor;
import io.tapstate.core.model.PipelineNode;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CapturedConnectorLogOwnerTest {

    @Test
    void anOldHandleDrivenUnderANewGenerationOrAnotherPipelineStillFilesItsOwnGeneration(@TempDir Path temporary) {
        String pipeline = "captured_log_owner";
        var ownership = PipelineActuationOwnership.single("log-owner-test", new InMemoryWorkloadClaimStore());
        var first = ownership.beginExecution(pipeline);
        assertThat(first.allowed()).isTrue();
        LogSink.Scope old = new LogSink.Scope("resource-a", first.fence().executionGeneration());
        var reference = SyntheticCaptureLogJars.refusingSource(temporary);
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);
        Logger logger = (Logger) LoggerFactory.getLogger("io.tapstate.connector");
        PipelineLogAppender appender = new PipelineLogAppender(sink, new SecretRedactor());
        appender.setContext(logger.getLoggerContext()); appender.start(); logger.addAppender(appender);
        PipelineLogContext saved = PipelineLogContext.capture();
        try (var handle = SyntheticCaptureLogJars.open(reference, new PipelineNode(pipeline, "source"), old)) {
            var second = ownership.beginExecution(pipeline);
            assertThat(second.allowed()).isTrue();
            assertThat(second.fence().executionGeneration()).isEqualTo(old.executionGeneration() + 1);
            LogSink.Scope current = new LogSink.Scope(old.pipelineIncarnationId(), second.fence().executionGeneration());
            MDC.put(PipelineAttribution.MDC_KEY, pipeline);
            MDC.put(PipelineAttribution.INCARNATION_MDC_KEY, current.pipelineIncarnationId());
            MDC.put(PipelineAttribution.EXECUTION_MDC_KEY, Long.toString(current.executionGeneration()));
            PipelineLogContext newCaller = PipelineLogContext.capture();
            assertThatThrownBy(handle::drive).isInstanceOf(IllegalStateException.class)
                    .hasMessage("cannot open a connection");
            assertThat(PipelineLogContext.capture()).isEqualTo(newCaller);
            MDC.put(PipelineAttribution.MDC_KEY, "other_pipeline");
            MDC.put(PipelineAttribution.INCARNATION_MDC_KEY, "other-resource");
            PipelineLogContext otherCaller = PipelineLogContext.capture();
            assertThatThrownBy(handle::drive).isInstanceOf(IllegalStateException.class)
                    .hasMessage("cannot open a connection");
            assertThat(PipelineLogContext.capture()).isEqualTo(otherCaller);
            assertThat(sink.tail(pipeline, old)).hasSize(4);
            assertThat(sink.tail(pipeline, old).stream().map(LogLine::message)
                    .filter(message -> message.contains("password authentication failed: no password was given")))
                    .hasSize(2);
            assertThat(sink.tail(pipeline, old).stream().map(LogLine::message)
                    .filter(message -> message.contains("the connection was refused before any row was read")))
                    .hasSize(2);
            assertThat(sink.tail(pipeline, current)).isEmpty();
            assertThat(sink.tail("other_pipeline", new LogSink.Scope("other-resource", current.executionGeneration())))
                    .isEmpty();
            assertThat(sink.tail(pipeline)).isEmpty();
        } finally {
            saved.restore(); logger.detachAppender(appender); appender.stop();
        }
    }
}
