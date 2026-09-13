package io.tapstate.app;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.tapstate.core.logging.LogLine;
import io.tapstate.core.logging.RingBufferLogSink;
import io.tapstate.core.logging.SecretRedactor;
import io.tapstate.core.sql.JoinKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The one account anyone gets of a join dropping a dimension row to a key another row already held.
 * Nothing else about the pipeline moves when it happens - the target is merely short a row, and every
 * row it does hold looks entirely ordinary - so what this writes is the whole of the signal, which is
 * why the assertions here are on the text.
 */
class LoggingDimensionRowDisplacedAlertTest {

    private final ListAppender<ILoggingEvent> written = new ListAppender<>();
    private Logger logger;

    @BeforeEach
    void captureWhatIsWritten() {
        logger = (Logger) LoggerFactory.getLogger(LoggingDimensionRowDisplacedAlert.class);
        written.start();
        logger.addAppender(written);
    }

    @AfterEach
    void stopCapturing() {
        MDC.remove(PipelineLogAppender.PIPELINE_ID_MDC_KEY);
        logger.detachAppender(written);
        written.stop();
    }

    @Test
    void aDisplacedRowIsWarnedAboutUnderItsCodeNamingTheSourceAndTheKeyBothRowsShare() {
        bound().displaced("customers", JoinKey.of(List.of(1L)).name());

        assertThat(written.list).hasSize(1);
        ILoggingEvent event = written.list.get(0);
        assertThat(event.getLevel()).isEqualTo(Level.WARN);
        assertThat(event.getFormattedMessage())
                .contains("engine.join-dimension-row-displaced")
                .contains("customers")
                // The key as the reader's own table spells it. Filed under, it is an encoding - and
                // somebody told that "AAAAATE" lost a row cannot say which of their rows that is.
                .contains("1")
                .contains("orders_sync")
                .contains("join_customers");
    }

    @Test
    void aJoinLosingRowByRowSaysSoAtWideningIntervalsRatherThanOncePerRow() {
        // A join matched on a column that identifies nothing loses a row per duplicate, which on a first
        // load is a line per row of the source. That buries every other line in the log, so the first of
        // each decade is written and the count on it says how far past the first this has gone.
        LoggingDimensionRowDisplacedAlert alert = bound();
        for (int row = 0; row < 100; row++) {
            alert.displaced("customers", JoinKey.of(List.of((long) row)).name());
        }

        assertThat(alert.count()).as("every loss is counted, whether or not it is written").isEqualTo(100);
        assertThat(written.list.stream().map(ILoggingEvent::getFormattedMessage).toList())
                .as("the first, the tenth and the hundredth")
                .hasSize(3);
        assertThat(written.list.get(2).getFormattedMessage()).contains("100");
    }

    @Test
    void theWarningReachesTheAffectedPipelinesLogStreamAndRestoresExistingAttribution() {
        RingBufferLogSink sink = new RingBufferLogSink(8, 8);
        PipelineLogAppender appender = new PipelineLogAppender(sink, new SecretRedactor());
        appender.setContext(logger.getLoggerContext());
        appender.start();
        logger.addAppender(appender);
        MDC.put(PipelineLogAppender.PIPELINE_ID_MDC_KEY, "outer_pipeline");
        try {
            bound().displaced("customers", JoinKey.of(List.of(1L)).name());

            assertThat(MDC.get(PipelineLogAppender.PIPELINE_ID_MDC_KEY))
                    .as("the caller's attribution is restored after the scoped warning")
                    .isEqualTo("outer_pipeline");
        } finally {
            logger.detachAppender(appender);
            appender.stop();
        }

        assertThat(sink.tail("orders_sync"))
                .extracting(LogLine::message)
                .as("the warning is visible through the affected pipeline's logs read face")
                .singleElement()
                .asString()
                .contains("engine.join-dimension-row-displaced")
                .contains("customers")
                .contains("join_customers");
        assertThat(sink.tail("outer_pipeline")).isEmpty();
    }

    private static LoggingDimensionRowDisplacedAlert bound() {
        return (LoggingDimensionRowDisplacedAlert) new LoggingDimensionRowDisplacedAlert()
                .bind("orders_sync", "join_customers");
    }
}
