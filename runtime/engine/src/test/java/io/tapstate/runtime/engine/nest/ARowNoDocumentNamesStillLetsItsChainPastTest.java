package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.SettledPositions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * That a pointed-at row no document names still says where it sat, so its chain is not stopped by it.
 *
 * <p><b>Nothing else downstream can say it.</b> These rows reach a sink only inside the documents naming
 * them, and a sink learns a position is durable by writing what carries it - so a row nobody names produces
 * no record, and the chain it arrived on is never advanced past it. Left there, the chain's durable position
 * stops at whichever row some document happened to name and stays there for the life of the job: every
 * document correct, every count healthy, and a restart replaying that table from further back every day
 * while the position races the source's retention window.
 *
 * <p><b>The second case is the one that keeps the first from being satisfied by saying it about everything.</b>
 * A row that <em>does</em> wake documents owes exactly those documents, and its position travels inside them
 * precisely so a frontier cannot pass it before they have been written. Saying it owes nothing as well would
 * ack above changes still to be written - and after a crash there they are neither delivered nor replayable,
 * because a word about a pointed-at row changes nothing in a document's state and nothing would draw it
 * again. So the two cases are the same fixture with one difference, and an implementation that speaks for
 * every row it files fails the second while passing the first.
 */
class ARowNoDocumentNamesStillLetsItsChainPastTest {

    /** Orders carrying the customer they point at, which is the direction with no path back to a document. */
    private static final TransformBody.Nest TREE = nest("order", List.of("order_id"),
            embed("customer", "customer_id", "cust_ref", EmbedAs.OBJECT, "customer", null));

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TREE, tables());
    private static final NestLookup CUSTOMERS = TOPOLOGY.lookups().get(0);

    private static final long PLENTY = 100L;

    private final HeapNestStore<Map<String, Object>> rows = new HeapNestStore<>();
    private final HeapNestStore<Set<Object>> references = new HeapNestStore<>();

    @Test
    void aRowNothingPointsAtSaysWhereItSat() throws Exception {
        TestOutbox outbox = new TestOutbox(1024);
        LookupProcessor processor = lookup(outbox);

        feed(processor, LookupProcessor.ROWS, customer(100, "Ada"));

        assertThat(settled(drain(outbox)))
                .describedAs("no document names this row, so no record will ever carry where it sat and "
                        + "this vertex is the only thing that can speak for it")
                .singleElement()
                .extracting(word -> word.positions().get("customer"))
                .isEqualTo(new ChainPosition(at(100), null));
    }

    @Test
    void aRowThatWakesADocumentSaysNothing() throws Exception {
        TestOutbox outbox = new TestOutbox(1024);
        LookupProcessor processor = lookup(outbox);

        feed(processor, LookupProcessor.REGISTRATIONS, order(1, 100));
        drain(outbox);

        feed(processor, LookupProcessor.ROWS, customer(100, "Ada"));
        List<Object> sent = drain(outbox);

        assertThat(woken(sent))
                .describedAs("the control: this row does wake a document, so the fixture reached the "
                        + "state being asked about rather than producing nothing for want of a wiring")
                .hasSize(1);
        assertThat(settled(sent))
                .describedAs("and its position is owed to that document, which has not been written yet. "
                        + "It travels inside the document for exactly that reason; saying here that the "
                        + "chain owes nothing would let a frontier past a change that a crash then leaves "
                        + "neither delivered nor replayable")
                .isEmpty();
    }

    @Test
    void aDrainOfRowsNothingPointsAtSaysTheHighestOfThemOnce() throws Exception {
        TestOutbox outbox = new TestOutbox(1024);
        LookupProcessor processor = lookup(outbox);

        feed(processor, LookupProcessor.ROWS,
                customer(100, "Ada"), customer(101, "Grace"), customer(102, "Nobody"));

        assertThat(settled(drain(outbox)))
                .describedAs("one word for the drain, not one per row. On a table nothing points at every "
                        + "row takes this path, so a word each would put as many records on the edge as "
                        + "the stream has rows")
                .singleElement()
                .extracting(word -> word.positions().get("customer"))
                .describedAs("carrying the highest of them, which is the only one a frontier can use: it "
                        + "advances to the highest position it is given at or below its bound")
                .isEqualTo(new ChainPosition(at(102), null));
    }

    private LookupProcessor lookup(TestOutbox outbox) throws Exception {
        LookupProcessor processor = new LookupProcessor(CUSTOMERS, rows, references, PLENTY, null);
        processor.init(outbox, new TestProcessorContext());
        return processor;
    }

    /** Word that a chain got past rows no document names, out of everything the vertex sent. */
    private static List<SettledPositions> settled(List<Object> sent) {
        return sent.stream()
                .filter(SettledPositions.class::isInstance)
                .map(SettledPositions.class::cast)
                .toList();
    }

    /** Word asking a document to be drawn again, out of everything the vertex sent. */
    private static List<Object> woken(List<Object> sent) {
        return sent.stream().filter(NestTouch.class::isInstance).toList();
    }

    /**
     * Everything the vertex has sent since it was last read. Taken once per feed and then read by kind: a
     * second drain answers empty, and empty is what half the assertions here are looking for - so draining
     * per kind would have each of them pass on the order they happened to be written in.
     */
    private static List<Object> drain(TestOutbox outbox) {
        List<Object> sent = new ArrayList<>();
        outbox.drainQueueAndReset(0, sent, false);
        return sent;
    }

    private static void feed(LookupProcessor processor, int ordinal, Object... items) {
        TestInbox inbox = new TestInbox();
        inbox.queue().addAll(Arrays.asList(items));
        processor.process(ordinal, inbox);
    }

    private static Envelope order(long orderId, int customerId) {
        return Envelope.insert(orderId, "order", row("order_id", orderId, "cust_ref", customerId), null)
                .withOrder(at(orderId));
    }

    private static Envelope customer(int customerId, String name) {
        return Envelope.insert(customerId, "customer", row("customer_id", customerId, "name", name), null)
                .withOrder(at(customerId));
    }
}
