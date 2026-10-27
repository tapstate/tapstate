package io.tapstate.adapters.pdk;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.logging.PipelineAttribution;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SnapshotReadLoggingTest {

    @Test
    void emptyAndNonemptyReadsReportTheirBoundariesWithoutClaimingTargetCompletion() throws Exception {
        Logger logger = (Logger) LoggerFactory.getLogger(PdkCapturePort.class);
        ListAppender<ILoggingEvent> written = capture();
        written.start();
        logger.addAppender(written);
        String counter = SnapshotReadLoggingTest.class.getName() + ".rows";
        System.getProperties().put(counter, new AtomicLong());
        try {
            for (int rows : new int[] {0, 2}) {
                written.list.clear();
                Path dir = scratch();
                PdkCapturePort port = port(Synthetic.largeSource(dir, rows, counter), "synthetic.LargeSource");
                try (CaptureBatch batch = port.snapshotOnly(config())) {
                    while (batch.hasNext()) batch.next();
                }
                assertThat(written.list).extracting(ILoggingEvent::getFormattedMessage).containsExactly(
                        "Snapshot read started for table t1",
                        "Snapshot read finished for table t1 (" + rows + " events read; awaiting target confirmation)");
                assertThat(written.list).allSatisfy(event -> assertThat(event.getMDCPropertyMap())
                        .containsEntry(PipelineAttribution.MDC_KEY, "orders"));
            }
        } finally {
            System.getProperties().remove(counter);
            logger.detachAppender(written);
            written.stop();
        }
    }

    @Test
    void aFailedReadHasNoSuccessfulFinishEvent() throws Exception {
        Logger logger = (Logger) LoggerFactory.getLogger(PdkCapturePort.class);
        ListAppender<ILoggingEvent> written = capture();
        written.start();
        logger.addAppender(written);
        try {
            PdkCapturePort port = port(Synthetic.throwingReadSource(scratch()), "synthetic.ThrowingRead");
            assertThatThrownBy(() -> {
                try (CaptureBatch batch = port.snapshotOnly(config())) {
                    while (batch.hasNext()) batch.next();
                }
            }).isInstanceOf(TapstateException.class);
            assertThat(written.list).extracting(ILoggingEvent::getFormattedMessage)
                    .containsExactly("Snapshot read started for table t1");
        } finally {
            logger.detachAppender(written);
            written.stop();
        }
    }

    private static PdkCapturePort port(Path jar, String className) {
        return new PdkCapturePort(id -> new ConnectorRef(List.of(jar), className, "2.0.8", null));
    }

    private static ListAppender<ILoggingEvent> capture() {
        return new ListAppender<>() {
            @Override
            protected void append(ILoggingEvent event) {
                event.prepareForDeferredProcessing();
                super.append(event);
            }
        };
    }

    private static CaptureConfig config() {
        return new CaptureConfig("demo", Map.of(), List.of("t1"), new PipelineNode("orders", "src"));
    }

    private static Path scratch() throws Exception {
        return Files.createTempDirectory(Path.of(System.getProperty("java.io.tmpdir")), "snapshot-logs-");
    }
}
