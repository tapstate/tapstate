package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * A row may change the column identifying it while going on pointing at the same row. The record of who
 * points at what is filed under the row's own identity, so such a change has to move the entry - and the
 * departure edge is the only place that can, because the entry to take out is the one keyed by where the
 * row used to point.
 *
 * <p><b>Why the obvious test on that edge does not cover this.</b> Every row of the stream travels the
 * departure edge, not only the ones that moved, so the edge has to tell a row that moved from one that
 * stayed. Comparing what the row points at answers that for a row that was re-pointed, and answers it
 * wrongly for a row that kept its reference and changed its own identity: the two references are equal, the
 * edge concludes nothing moved, and the entry filed under the old identity is left standing while the
 * arrival edge files a second one under the new. Both are then in the same bucket for the life of the job.
 *
 * <p><b>The two costs are asserted separately because they are paid in different places.</b> One is a word
 * sent to a document that does not exist, on every edit to the row pointed at - drawn, written out, and
 * addressed to nobody. The other is a permanent unit of the ceiling on how many rows may point at one,
 * which is spent by a table that merely churns through identities rather than by data that got wide.
 */
class ARowThatChangesItsIdentityLeavesNothingWhereItPointedTest {

    /** Orders carrying the customer they point at: the row's identity and its reference are two columns. */
    private static final TransformBody.Nest TREE = nest("order", List.of("order_id"),
            embed("customer", "customer_id", "cust_ref", EmbedAs.OBJECT, "customer", null));

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TREE, tables());
    private static final NestLookup CUSTOMERS = TOPOLOGY.lookups().get(0);

    private static final long LIMIT = 3L;

    private final HeapNestStore<Map<String, Object>> rows = new HeapNestStore<>();
    private final HeapNestStore<Set<Object>> references = new HeapNestStore<>();

    /**
     * The identity it had is not woken when the row it points at is edited. Asserted as the whole of what
     * was sent rather than as the absence of one word: the new identity has to still be there, or a test
     * that files nothing at all would read exactly like a test that files the right thing.
     */
    @Test
    void theIdentityItLeftBehindIsNotWokenWhenTheRowItPointsAtIsEdited() throws Exception {
        TestOutbox outbox = new TestOutbox(1024);
        LookupProcessor processor = lookup(outbox);
        feed(processor, LookupProcessor.REGISTRATIONS, order(1, 100));

        renamed(processor, 1, 7, 100);

        feed(processor, LookupProcessor.ROWS, customer(100, "Grace"));
        assertThat(woken(outbox))
                .describedAs("the row is one row under a new identity, so one document is drawn again - "
                        + "and it is the one that exists")
                .containsExactly(List.of(7L));
    }

    /**
     * And the room it took comes back with it. A table churning through identities without widening at all
     * would otherwise walk into the ceiling and stop the job.
     */
    @Test
    void theIdentityItLeftBehindStopsCountingAgainstWhatMayPointAtThatRow() throws Exception {
        LookupProcessor processor = lookup(new TestOutbox(1024));
        feed(processor, LookupProcessor.REGISTRATIONS, order(1, 100), order(2, 100), order(3, 100));

        renamed(processor, 1, 7, 100);

        assertThatCode(() -> feed(processor, LookupProcessor.ROWS, customer(100, "Grace")))
                .describedAs("three rows point at the customer before the change and three after it, and "
                        + "three is what it allows")
                .doesNotThrowAnyException();
    }

    private LookupProcessor lookup(TestOutbox outbox) throws Exception {
        LookupProcessor processor = new LookupProcessor(CUSTOMERS, rows, references, LIMIT, null);
        processor.init(outbox, new TestProcessorContext());
        return processor;
    }

    /**
     * One row changing the column that identifies it and keeping the one it points at, delivered the way
     * the engine delivers it: the arrival edge files it under what it is now, the departure edge is handed
     * the same event and is the only one that can take out what it was.
     */
    private static void renamed(LookupProcessor processor, long was, long now, int customerId) {
        Envelope event = Envelope.update(now, "order",
                row("order_id", was, "cust_ref", customerId),
                row("order_id", now, "cust_ref", customerId), null).withOrder(at(now));
        feed(processor, LookupProcessor.REGISTRATIONS, event);
        feed(processor, LookupProcessor.DEPARTED_REGISTRATIONS, event);
    }

    /** The identities this vertex asked to be drawn again. */
    private static List<Object> woken(TestOutbox outbox) {
        List<Object> sent = new ArrayList<>();
        outbox.drainQueueAndReset(0, sent, false);
        List<Object> keys = new ArrayList<>();
        sent.forEach(item -> keys.add(((NestTouch) item).key()));
        return keys;
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
