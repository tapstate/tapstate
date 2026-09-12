package io.tapstate.app;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.tapstate.core.sql.JoinKey;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

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
        logger.detachAppender(written);
        written.stop();
    }

    @Test
    void aDisplacedRowIsWarnedAboutUnderItsCodeNamingTheSourceAndTheKeyBothRowsShare() {
        new LoggingDimensionRowDisplacedAlert().displaced("customers", JoinKey.of(List.of(1L)).name());

        assertThat(written.list).hasSize(1);
        ILoggingEvent event = written.list.get(0);
        assertThat(event.getLevel()).isEqualTo(Level.WARN);
        assertThat(event.getFormattedMessage())
                .contains("engine.join-dimension-row-displaced")
                .contains("customers")
                // The key as the reader's own table spells it. Filed under, it is an encoding - and
                // somebody told that "AAAAATE" lost a row cannot say which of their rows that is.
                .contains("1");
    }

    @Test
    void aJoinLosingRowByRowSaysSoAtWideningIntervalsRatherThanOncePerRow() {
        // A join matched on a column that identifies nothing loses a row per duplicate, which on a first
        // load is a line per row of the source. That buries every other line in the log, so the first of
        // each decade is written and the count on it says how far past the first this has gone.
        LoggingDimensionRowDisplacedAlert alert = new LoggingDimensionRowDisplacedAlert();
        for (int row = 0; row < 100; row++) {
            alert.displaced("customers", JoinKey.of(List.of((long) row)).name());
        }

        assertThat(alert.count()).as("every loss is counted, whether or not it is written").isEqualTo(100);
        assertThat(written.list.stream().map(ILoggingEvent::getFormattedMessage).toList())
                .as("the first, the tenth and the hundredth")
                .hasSize(3);
        assertThat(written.list.get(2).getFormattedMessage()).contains("100");
    }
}
