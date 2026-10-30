package io.tapstate.app;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.tapstate.core.lifecycle.NestStateReading;
import io.tapstate.core.lifecycle.NestStateWindow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;
import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The one account anyone gets of a nest namespace that has stopped being served from memory. Nothing else
 * about the pipeline moves when it happens, so what this writes is the whole of the signal — which is why
 * the assertions here are on the text rather than on a counter.
 */
class LoggingNestColdLayerAlertTest {

    private final ListAppender<ILoggingEvent> written = new ListAppender<>();
    private Logger logger;
    private Level previousLevel;

    @BeforeEach
    void captureWhatIsWritten() {
        logger = (Logger) LoggerFactory.getLogger(LoggingNestColdLayerAlert.class);
        previousLevel = logger.getLevel();
        logger.setLevel(Level.INFO);
        written.start();
        logger.addAppender(written);
    }

    @AfterEach
    void stopCapturing() {
        logger.detachAppender(written);
        written.stop();
        logger.setLevel(previousLevel);
    }

    @Test
    void aNamespaceFallingToTheColdLayerIsWarnedAboutUnderItsCodeWithTheNumbersBehindIt() {
        new LoggingNestColdLayerAlert().crossed("p1", "nest.p1.items", window(1_000, 950, 4_750));

        ILoggingEvent event = only();
        assertThat(event.getLevel()).isEqualTo(Level.WARN);
        assertThat(event.getFormattedMessage())
                .contains("nest.state-served-from-cold-layer")
                .contains("nest.p1.items")
                .contains("950")
                .contains("1000")
                .contains("95%")
                .contains("100")
                .contains("10000")
                // How far from resident the namespace is. The share says the reaching went to storage; this
                // is the number that suggests what to do about it, and leaving the reader to divide 100 by
                // 10000 in their head is how a line stops being read.
                .contains("1.0%");
    }

    @Test
    void aNamespaceServedFromMemoryAgainIsSaidUnderTheSameCodeSoOneSearchFindsBothEnds() {
        // The all-clear is not an error and has no severity that would make it one, but whoever went
        // looking for the warning went looking by its code. Written without the code, the line that
        // withdraws it is the one line the search that found the problem will not return.
        new LoggingNestColdLayerAlert().cleared("p1", "nest.p1.items", window(1_000, 10, 50));

        ILoggingEvent event = only();
        assertThat(event.getLevel()).isEqualTo(Level.INFO);
        assertThat(event.getFormattedMessage())
                .contains("nest.state-served-from-cold-layer")
                .contains("nest.p1.items");
    }

    @Test
    void theNumbersReadTheSameWhateverLocaleTheProcessHappensToRunUnder() {
        // A decimal comma in a log line is not a cosmetic difference: it is what an operator greps and what
        // a downstream parser splits on, and the locale a server boots under is nobody's deliberate choice.
        Locale was = Locale.getDefault();
        try {
            Locale.setDefault(Locale.GERMANY);
            new LoggingNestColdLayerAlert().crossed("p1", "nest.p1.items", window(1_000, 950, 4_750));
        } finally {
            Locale.setDefault(was);
        }

        assertThat(only().getFormattedMessage()).contains("95%").contains("5.0ms").doesNotContain("5,0");
    }

    @Test
    void aNamespaceWithNothingBehindItsMemoryStillReadsAsAWholeSentence() {
        // It cannot arrive here -- no layer behind the memory means no trip to it to count -- so this is
        // about what the line says if it ever does, rather than a case with a route to it. A missing number
        // must not render as an empty gap that reads like a truncated message.
        NestStateWindow window = NestStateWindow.between(
                new NestStateReading(100, 0, 0, 0),
                new NestStateReading(100, 1_000, 950, 4_750));

        new LoggingNestColdLayerAlert().crossed("p1", "nest.p1.items", window);

        assertThat(only().getFormattedMessage()).contains("stored=unknown");
    }

    private ILoggingEvent only() {
        List<ILoggingEvent> events = written.list;
        assertThat(events).hasSize(1);
        return events.get(0);
    }

    /** A window over a namespace holding 100 of its 10000 keys in memory. */
    private static NestStateWindow window(long accesses, long backfills, long backfillMillis) {
        return NestStateWindow.between(
                new NestStateReading(100, 0, 0, 0, OptionalLong.of(10_000)),
                new NestStateReading(100, accesses, backfills, backfillMillis, OptionalLong.of(10_000)));
    }
}
