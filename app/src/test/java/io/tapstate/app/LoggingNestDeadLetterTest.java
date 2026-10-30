package io.tapstate.app;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.runtime.engine.nest.ElementRef;
import io.tapstate.runtime.engine.nest.NestDeadLetter;
import io.tapstate.runtime.engine.nest.NestElement;
import io.tapstate.runtime.engine.nest.NestInbound;
import io.tapstate.runtime.engine.nest.NestVertex;
import io.tapstate.runtime.engine.nest.ReleasedChild;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What is said out loud while a nest is discarding changes. The count on the run's statistics says how many
 * and the store says which, but neither puts anything in front of somebody at the time - so what this writes
 * is the whole of the signal that it is happening, which is why the assertions are on the text.
 */
class LoggingNestDeadLetterTest {

    private static final NestVertex ITEMS = new NestVertex(List.of("items"), "items", "nest.p.items",
            List.of("order_id"), List.of("customer_id"),
            List.of(new NestInbound(0, "item", List.of("items"), List.of("order_id"), List.of("id"))));

    private final ListAppender<ILoggingEvent> written = new ListAppender<>();
    private final HandedOver next = new HandedOver();
    private Logger logger;
    private Level previousLevel;

    @BeforeEach
    void captureWhatIsWritten() {
        logger = (Logger) LoggerFactory.getLogger(LoggingNestDeadLetter.class);
        previousLevel = logger.getLevel();
        logger.setLevel(Level.WARN);
        written.start();
        logger.addAppender(written);
    }

    @AfterEach
    void stopCapturing() {
        logger.detachAppender(written);
        written.stop();
        logger.setLevel(previousLevel);
    }

    /**
     * How long the change waited is the one thing in this line that says which of two situations it is - a
     * reference that has dangled all along, or a parent deleted while its children were still arriving - and
     * it is the field most easily lost, since it is put into the arguments and read back out by the catalog
     * rather than written into the sentence here.
     */
    @Test
    void saysHowLongTheChangeHadBeenWaiting() {
        new LoggingNestDeadLetter(next).unassemblable(ITEMS, released(7, Duration.ofMinutes(3)));

        ILoggingEvent event = only();
        assertThat(event.getLevel()).isEqualTo(Level.WARN);
        assertThat(event.getFormattedMessage())
                .contains("nest.parent-absent")
                .contains("item")
                .contains("items")
                // The wait itself. Without this the line reads the same for a reference that has dangled for
                // a month and for a parent deleted a second ago, which are the two things it exists to tell
                // apart -- and it would go on reading the same if the argument stopped being passed at all.
                .contains(Duration.ofMinutes(3).toString());
    }

    /**
     * A parent deleted with many children below it produces one of these per child. Logging every one buries
     * the rest of the log, so the line is sampled - and the sampling is what makes the count elsewhere
     * load-bearing rather than a convenience.
     */
    @Test
    void doesNotWriteALineForEveryOneOfThem() {
        NestDeadLetter channel = new LoggingNestDeadLetter(next);

        for (int id = 0; id < 25; id++) {
            channel.unassemblable(ITEMS, released(id, Duration.ZERO));
        }

        assertThat(next.count).isEqualTo(25);
        // The first of each decade reached, and no other: the 1st and the 10th of 25. Asserting the count
        // alone would pass on a channel that wrote the first two and stopped, which is a different rule.
        assertThat(written.list.stream().map(ILoggingEvent::getFormattedMessage))
                .hasSize(2)
                .anySatisfy(line -> assertThat(line).contains("(1 handed over"))
                .anySatisfy(line -> assertThat(line).contains("(10 handed over"));
    }

    /** Every one reaches what keeps it, whether or not a line was written about it. */
    @Test
    void handsEveryChangeToWhatKeepsItEvenTheOnesItDoesNotMention() {
        NestDeadLetter channel = new LoggingNestDeadLetter(next);

        for (int id = 0; id < 25; id++) {
            channel.unassemblable(ITEMS, released(id, Duration.ZERO));
        }

        assertThat(next.orders).hasSize(25);
    }

    /**
     * The discriminating case for the order of the two: if keeping the row fails, the failure must reach the
     * caller rather than being preceded by a line announcing the row was dealt with. A log saying a row was
     * handed over, from a run where it was not, is worse than no log at all.
     */
    @Test
    void doesNotAnnounceAChangeThatCouldNotBeKept() {
        NestDeadLetter refusing = new LoggingNestDeadLetter((from, released) -> {
            throw new IllegalStateException("the store is down");
        });

        assertThatThrownBy(() -> refusing.unassemblable(ITEMS, released(7, Duration.ZERO)))
                .isInstanceOf(IllegalStateException.class);
        assertThat(written.list).isEmpty();
    }

    /**
     * The binding is carried onto the DAG, so a non-serializable field here would not fail a build or a unit
     * test -- it would fail the job, at submit time, on a pipeline that had passed everything else.
     */
    @Test
    void travelsToTheMemberRunningTheVertex() throws Exception {
        LoggingNestDeadLetter channel = new LoggingNestDeadLetter(new SerializableSink());
        channel.unassemblable(ITEMS, released(1, Duration.ZERO));

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(channel);
        }
        Object read;
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            read = in.readObject();
        }

        assertThat(read).isInstanceOf(LoggingNestDeadLetter.class);
        assertThat(((LoggingNestDeadLetter) read).count()).isEqualTo(1);
    }

    private ILoggingEvent only() {
        assertThat(written.list).hasSize(1);
        return written.list.get(0);
    }

    private static ReleasedChild released(int id, Duration heldFor) {
        return new ReleasedChild(
                new NestElement(new ElementRef(List.of("items"), 1, List.of(id), id), Map.of("id", id),
                        new SourceOrder(1L, id),
                        Map.of("item", new ChainPosition(new SourceOrder(1L, id), "t" + id))),
                heldFor);
    }

    /** Stands in for whatever keeps the rows, so what reached it can be asserted on. */
    private static final class HandedOver implements NestDeadLetter {

        private static final long serialVersionUID = 1L;

        private final List<SourceOrder> orders = new ArrayList<>();
        private int count;

        @Override
        public void unassemblable(NestVertex from, ReleasedChild released) {
            orders.add(released.child().order());
            count++;
        }
    }

    /** The same, with nothing in it, for the case that writes the whole channel down. */
    private static final class SerializableSink implements NestDeadLetter {

        private static final long serialVersionUID = 1L;

        @Override
        public void unassemblable(NestVertex from, ReleasedChild released) {
        }
    }
}
