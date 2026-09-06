package io.tapstate.runtime.engine.nest;

import static io.tapstate.runtime.engine.nest.NestFixtures.at;
import static io.tapstate.runtime.engine.nest.NestFixtures.row;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.embed;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.nest;
import static io.tapstate.runtime.engine.nest.NestTreeFixtures.tables;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.jet.core.test.TestInbox;
import com.hazelcast.jet.core.test.TestOutbox;
import com.hazelcast.jet.core.test.TestProcessorContext;
import io.tapstate.core.common.TapstateException;
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
 * The fifth quantity that fails a run rather than being absorbed by the layer behind the memory: how many
 * rows point at one row.
 *
 * <p><b>What it bounds is the rewrite, not the storage, and every other limit here bounds storage.</b> The
 * identities pointing at one row are spread over a fixed number of buckets, so the entry holding them is
 * divisible and a store behind the map carries it perfectly well however many there are - which is exactly
 * why the first reason written down for this limit was wrong and had to be withdrawn. What no store helps
 * with is the other end: each identity recorded there is a document that has to be drawn again and written
 * out whole the moment the row they all point at is edited. A row a hundred thousand documents name costs a
 * hundred thousand rewrites per edit, and nothing downstream folds them - the throttle's window is opened
 * per document, and these are a different document each.
 *
 * <p>So the count is weighed where the rewrite would be paid: on an edit to the row being pointed at,
 * rather than as each row registers. It costs no extra reach into the state layer there - waking the
 * documents already asks for every bucket in one request and already walks every identity in them, so the
 * number is in hand at the moment it is needed. The cost of the other placement is what makes this one
 * worth naming: weighing it per registration means reading every bucket of a row on each arrival of the
 * stream pointing at it.
 *
 * <p><b>What is deliberately not asserted here: that the refusal happens before the words are queued
 * rather than after.</b> It does, and it is the right way round - a queue of a hundred thousand entries
 * built to be thrown away is an allocation spike at the moment memory is worst - but nothing outside the
 * vertex can see which way it is. Those words go into an internal queue and reach the outbox only in the
 * flush that follows the drain, and an exception leaves before that flush either way, so the outbox is
 * empty under both orderings. Moving the refusal after the loop was tried and every case here still
 * passed. Said out loud rather than left as an assertion that looks like it covers this.
 */
class ARowMayNotBePointedAtByMoreRowsThanItsLimitTest {

    /** Orders carrying the customer they point at, which is the direction with no path back to a document. */
    private static final TransformBody.Nest TREE = nest("order", List.of("order_id"),
            embed("customer", "customer_id", "cust_ref", EmbedAs.OBJECT, "customer", null));

    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TREE, tables());
    private static final NestLookup CUSTOMERS = TOPOLOGY.lookups().get(0);

    /** Three orders may point at one customer; the fourth is what must stop the job. */
    private static final long LIMIT = 3L;

    private final HeapNestStore<Map<String, Object>> rows = new HeapNestStore<>();
    private final HeapNestStore<Set<Object>> references = new HeapNestStore<>();

    @Test
    void aRowPointedAtByMoreRowsThanItsLimitFailsTheJobSayingWhichRowAndHowMany() throws Exception {
        LookupProcessor processor = lookup(new TestOutbox(1024));
        pointAtCustomer(processor, 100, LIMIT + 1);

        assertThatThrownBy(() -> feed(processor, LookupProcessor.ROWS, customer(100, "Grace")))
                .isInstanceOf(TapstateException.class)
                .extracting(thrown -> ((TapstateException) thrown).args())
                .isEqualTo(Map.of("refPath", "customer", "identity", "[100]",
                        "referrers", LIMIT + 1, "limit", LIMIT));
    }

    @Test
    void aRowPointedAtByExactlyItsLimitKeepsRunningAndWakesEveryOneOfThem() throws Exception {
        TestOutbox outbox = new TestOutbox(1024);
        LookupProcessor processor = lookup(outbox);
        pointAtCustomer(processor, 100, LIMIT);

        assertThatCode(() -> feed(processor, LookupProcessor.ROWS, customer(100, "Grace")))
                .describedAs("the limit is what may point at a row, not the first count that is refused")
                .doesNotThrowAnyException();
        assertThat(woken(outbox))
                .describedAs("and the round that shows the limit is not reached is a round that actually "
                        + "woke every document - a limit that passes because nothing propagates bounds "
                        + "nothing, and reads identically here")
                .hasSize((int) LIMIT);
    }

    /** Per row pointed at, so one row carrying all it may leaves another's room untouched. */
    @Test
    void oneRowPointedAtByAllItMayDoesNotSpendAnothersRoom() throws Exception {
        LookupProcessor processor = lookup(new TestOutbox(1024));
        pointAtCustomer(processor, 100, LIMIT);
        pointAtCustomer(processor, 200, LIMIT);

        assertThatCode(() -> feed(processor, LookupProcessor.ROWS, customer(200, "Ada")))
                .describedAs("two customers each pointed at by all they may allow is two allowed customers")
                .doesNotThrowAnyException();
    }

    /**
     * What is weighed is who points at the row now, not how many ever did. A row that stopped pointing here
     * is taken out of the record when it is deleted, and the room it took has to come back with it -
     * otherwise a table churning through references fails a job whose data never got wide at all.
     */
    @Test
    void aRowThatStoppedPointingHereIsNoLongerHeldAgainstTheLimit() throws Exception {
        LookupProcessor processor = lookup(new TestOutbox(1024));
        pointAtCustomer(processor, 100, LIMIT + 1);

        feed(processor, LookupProcessor.REGISTRATIONS, deletedOrder(1, 100));

        assertThatCode(() -> feed(processor, LookupProcessor.ROWS, customer(100, "Grace")))
                .describedAs("one of the four is gone, so three point at it and three is what it allows")
                .doesNotThrowAnyException();
    }

    private LookupProcessor lookup(TestOutbox outbox) throws Exception {
        LookupProcessor processor = new LookupProcessor(CUSTOMERS, rows, references, LIMIT, null);
        processor.init(outbox, new TestProcessorContext());
        return processor;
    }

    /** {@code count} orders, each recorded as pointing at the customer {@code customerId}. */
    private static void pointAtCustomer(LookupProcessor processor, int customerId, long count) {
        List<Object> orders = new ArrayList<>();
        for (long i = 1; i <= count; i++) {
            orders.add(order(customerId * 1000L + i, customerId));
        }
        feed(processor, LookupProcessor.REGISTRATIONS, orders.toArray());
    }

    /** The words this vertex sent, which are the documents it asked to be drawn again. */
    private static List<Object> woken(TestOutbox outbox) {
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

    private static Envelope deletedOrder(long orderId, int customerId) {
        Map<String, Object> was = row("order_id", customerId * 1000L + orderId, "cust_ref", customerId);
        return Envelope.delete(orderId, "order", was, null).withOrder(at(orderId));
    }

    private static Envelope customer(int customerId, String name) {
        return Envelope.insert(customerId, "customer", row("customer_id", customerId, "name", name), null)
                .withOrder(at(customerId));
    }
}
