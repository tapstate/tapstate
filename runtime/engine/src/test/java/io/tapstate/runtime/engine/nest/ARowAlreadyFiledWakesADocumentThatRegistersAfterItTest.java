package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
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
 * That a row filed before anything pointed at it still reaches the document that points at it afterwards.
 *
 * <p><b>The two deliveries race, and only one order of them was answered.</b> A row arrives on one edge
 * and the identities pointing at it on another, from two different streams, so the engine is free to hand
 * them over either way round. Waking on the row's arrival answers one of those orders: whoever had
 * registered by then is told. The other order says nothing at all - the row is filed while no bucket names
 * anybody, and the registration that follows records an identity and goes quiet.
 *
 * <p><b>What that costs has no symptom.</b> The document is parked waiting for a row that is already in the
 * state layer, and nothing will ever come to say so: the job stays RUNNING, nothing is thrown, no count
 * moves, no dead letter is written, and every document that did assemble is correct. Measured over twenty
 * runs of the job-level witness, three of them hung this way and stayed hung.
 *
 * <p>The control is the other order, in the second case. Both cases must produce exactly one word: a
 * fixture that cannot produce one at all would leave the first case passing for the wrong reason once it
 * was made to fail differently, and zero is what the defect and a broken fixture both read as.
 */
class ARowAlreadyFiledWakesADocumentThatRegistersAfterItTest {

    /** Orders carrying the customer they point at, which is the direction with no path back to a document. */
    private static final TransformBody.Nest TREE = nest("order", List.of("order_id"),
            embed("customer", "customer_id", "cust_ref", EmbedAs.OBJECT, "customer", null));

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TREE, tables());
    private static final NestLookup CUSTOMERS = TOPOLOGY.lookups().get(0);

    private static final long PLENTY = 100L;

    private final HeapNestStore<Map<String, Object>> rows = new HeapNestStore<>();
    private final HeapNestStore<Set<Object>> references = new HeapNestStore<>();

    @Test
    void aRegistrationArrivingAfterTheRowIsToldTheRowIsAlreadyHere() throws Exception {
        TestOutbox outbox = new TestOutbox(1024);
        LookupProcessor processor = lookup(outbox);

        feed(processor, LookupProcessor.ROWS, customer(100, "Ada"));
        assertThat(woken(outbox))
                .describedAs("nothing points at it yet, so its own arrival wakes nobody - which is "
                        + "correct, and is exactly what leaves the next delivery as the only chance")
                .isEmpty();

        feed(processor, LookupProcessor.REGISTRATIONS, order(1, 100));

        assertThat(woken(outbox))
                .describedAs("the row this order points at is already filed, so the order is owed the "
                        + "word its own arrival was too late for. Without it the document waits for an "
                        + "arrival that has happened, with the job running and nothing to see")
                .singleElement()
                .asInstanceOf(type(NestTouch.class))
                .describedAs("and it goes as the word that answers a wait rather than as an edit. This "
                        + "is sent on every row of the pointing stream whose row is already filed, which "
                        + "on the ordinary path is all of them and none of them waiting: unmarked, each "
                        + "draws and sends a document that had already resolved the row on its own")
                .returns(true, NestTouch::onlyIfWaiting);
    }

    /**
     * The order that was already answered, kept as the control: it says the fixture can produce a word at
     * all, so the case above failing is the defect rather than a vertex wired to emit nothing.
     */
    @Test
    void aRegistrationArrivingBeforeTheRowIsWokenByTheRowAsItAlwaysWas() throws Exception {
        TestOutbox outbox = new TestOutbox(1024);
        LookupProcessor processor = lookup(outbox);

        feed(processor, LookupProcessor.REGISTRATIONS, order(1, 100));
        assertThat(woken(outbox))
                .describedAs("the row is not here yet, so there is nothing to be told about")
                .isEmpty();

        feed(processor, LookupProcessor.ROWS, customer(100, "Ada"));

        assertThat(woken(outbox))
                .describedAs("and the row's arrival wakes the one identity registered by then")
                .singleElement()
                .asInstanceOf(type(NestTouch.class))
                .describedAs("as an edit, which every document naming the row has to be drawn again "
                        + "for whatever state it is in - marking this one droppable would lose the "
                        + "propagation this whole direction exists to provide")
                .returns(false, NestTouch::onlyIfWaiting);
    }

    /**
     * A row deleted before the document naming it turned up is an answer too, and the one that ends the
     * wait rather than prolonging it: the document renders without the field and goes. Filed as an empty
     * row rather than taken out precisely so this can be answered, which only holds if the answer is sent.
     */
    @Test
    void aRegistrationArrivingAfterTheRowWasDeletedIsToldTheRowIsGone() throws Exception {
        TestOutbox outbox = new TestOutbox(1024);
        LookupProcessor processor = lookup(outbox);

        feed(processor, LookupProcessor.ROWS, deletedCustomer(100));
        woken(outbox);

        feed(processor, LookupProcessor.REGISTRATIONS, order(1, 100));

        assertThat(woken(outbox))
                .describedAs("a deletion already filed is knowledge the document needs as much as an "
                        + "arrival: one it can never be given again, since the source sends it once")
                .hasSize(1);
    }

    private LookupProcessor lookup(TestOutbox outbox) throws Exception {
        LookupProcessor processor = new LookupProcessor(CUSTOMERS, rows, references, PLENTY, null);
        processor.init(outbox, new TestProcessorContext());
        return processor;
    }

    /**
     * The words this vertex sent asking a document to be drawn again. The other kind it sends - word that
     * a chain got past rows no document names - is left out on purpose: this case is about which documents
     * are told what, and counting a word addressed to no document among them would have every case here
     * read one higher. What that word does and when it is sent has its own case.
     */
    private static List<Object> woken(TestOutbox outbox) {
        List<Object> sent = new ArrayList<>();
        outbox.drainQueueAndReset(0, sent, false);
        sent.removeIf(item -> item instanceof SettledPositions);
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

    private static Envelope deletedCustomer(int customerId) {
        return Envelope.delete(customerId, "customer", row("customer_id", customerId), null)
                .withOrder(at(customerId));
    }
}
