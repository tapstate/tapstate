package io.tapstate.app;

import ch.qos.logback.classic.Logger;
import io.tapstate.adapters.pdk.ConnectorError;
import io.tapstate.adapters.pdk.PdkCapturePort;
import io.tapstate.adapters.pdk.SyntheticCaptureLogJars;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.logging.LogLine;
import io.tapstate.core.logging.LogSink;
import io.tapstate.core.logging.RingBufferLogSink;
import io.tapstate.core.logging.SecretRedactor;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.SharedNotes;
import io.tapstate.spi.capture.SnapshotSession;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AdmittedSharedSnapshotLogOwnerTest {

    @Test
    void aSharedSnapshotInitRefusalFilesBothOriginalMessagesUnderItsAdmittedExecution(@TempDir Path temporary) {
        String pipeline = "shared_snapshot_owner";
        PipelineNode node = new PipelineNode(pipeline, "source");
        var ownership = PipelineActuationOwnership.single("log-owner-test", new InMemoryWorkloadClaimStore());
        var admitted = ownership.beginExecution(pipeline);
        assertThat(admitted.allowed()).isTrue();
        LogSink.Scope scope = new LogSink.Scope("resource-a", admitted.fence().executionGeneration());
        var reference = SyntheticCaptureLogJars.refusingSource(temporary);
        var port = new PdkCapturePort(ignored -> reference);
        CaptureConfig config = new CaptureConfig("log_owner_source", Map.of(), List.of("orders", "items"), node)
                .sharing(new SharedNotes("shared-log-owner-chain", List.of(new PipelineNode("earlier_pipeline", "earlier_source"))));
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);
        Logger logger = (Logger) LoggerFactory.getLogger("io.tapstate.connector");
        PipelineLogAppender appender = new PipelineLogAppender(sink, new SecretRedactor());
        appender.setContext(logger.getLoggerContext()); appender.start(); logger.addAppender(appender);
        try {
            try (SnapshotSession session = SnapshotSession.open(port.forLogOwner(node, scope), config)) {
                assertThatThrownBy(() -> session.read("orders"))
                        .isInstanceOf(TapstateException.class)
                        .satisfies(thrown -> {
                            TapstateException failure = (TapstateException) thrown;
                            assertThat(failure.code()).isEqualTo(ConnectorError.CAPTURE_FAILED);
                            assertThat(failure.args()).containsEntry("detail", "cannot open a connection");
                            assertThat(failure.getCause()).isInstanceOf(IllegalStateException.class);
                        });
            }
            assertThat(sink.tail(pipeline, scope)).extracting(LogLine::message)
                    .anySatisfy(message -> assertThat(message)
                            .contains("password authentication failed: no password was given"))
                    .anySatisfy(message -> assertThat(message)
                            .contains("the connection was refused before any row was read"));
            assertThat(sink.tail(pipeline, scope)).hasSize(2);
            assertThat(sink.tail(pipeline)).isEmpty();
            assertThat(sink.tailIncarnation("earlier_pipeline", scope.pipelineIncarnationId())).isEmpty();
            assertThat(sink.tail(pipeline, new LogSink.Scope(scope.pipelineIncarnationId(),
                    scope.executionGeneration() + 1))).isEmpty();
        } finally {
            logger.detachAppender(appender); appender.stop();
        }
    }
}
