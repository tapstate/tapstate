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
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.EmbedAs;
import io.tapstate.core.model.TransformBody;
import io.tapstate.runtime.engine.ReplayFloor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** The crossing order behind the duplicate document seen when the whole engine module is loaded. */
class AQueuedWakeDoesNotRedrawAnAlreadyCompleteDocumentTest {

    private static final TransformBody.Nest TREE = nest("order", List.of("order_id"),
            embed("customer", "customer_id", "cust_ref", EmbedAs.OBJECT, "customer", null));
    private static final NestTopology TOPOLOGY = NestTopology.compile("p", "doc", TREE, tables());
    private static final NestLookup CUSTOMERS = TOPOLOGY.lookups().get(0);

    @Test
    void theWakeQueuedByTheCustomerDoesNotSendTheSameCompletedOrderAgain() throws Exception {
        NestStore<Map<String, Object>> customers = new HeapNestStore<>();
        LookupProcessor lookup = new LookupProcessor(
                CUSTOMERS, customers, new HeapNestStore<Set<Object>>(), 100L, null);
        TestOutbox lookupOut = new TestOutbox(128);
        lookup.init(lookupOut, new TestProcessorContext());

        Envelope order = Envelope.insert(2L, "order",
                row("order_id", "O2", "cust_ref", "C-named"), null).withOrder(at(2));
        feed(lookup, LookupProcessor.REGISTRATIONS, order);
        feed(lookup, LookupProcessor.ROWS, Envelope.insert(5L, "customer",
                row("customer_id", "C-named", "name", "Grace"), null).withOrder(at(5)));
        List<Object> queuedWake = drain(lookupOut);
        assertThat(queuedWake)
                .describedAs("the customer arrived after the order registered, so the lookup queued the "
                        + "unconditional wake whose later delivery exposes the duplicate")
                .singleElement()
                .isInstanceOfSatisfying(NestTouch.class,
                        wake -> assertThat(wake.onlyIfWaiting()).isFalse());

        TestOutbox assemblerOut = new TestOutbox(128);
        AssemblerProcessor assembler = new AssemblerProcessor(
                TOPOLOGY.assembler(), TOPOLOGY.slots(), new HeapNestStore<>(), "doc",
                null, null, ReplayFloor.NONE, NestSettings.defaults(), NestClock.SYSTEM,
                NestSendPolicy.within(0L), new HeapNestStore<>(), (from, released) -> { },
                Map.of(CUSTOMERS.mapName(), customers));
        assembler.init(assemblerOut, new TestProcessorContext());

        Map<String, Object> completed = row(
                "order_id", "O2",
                "cust_ref", "C-named",
                "customer", row("customer_id", "C-named", "name", "Grace"));
        feed(assembler, 0, order);
        List<Envelope> documents = envelopes(drain(assemblerOut));
        assertThat(documents)
                .extracting(Envelope::after)
                .describedAs("the root reached the assembler after the customer had been filed, so it "
                        + "already produced the one complete document before the queued wake arrived")
                .containsExactly(completed);

        feed(assembler, CUSTOMERS.touchOrdinal(), queuedWake.toArray());
        documents.addAll(envelopes(drain(assemblerOut)));

        assertThat(documents)
                .extracting(Envelope::after)
                .describedAs("one order and one customer arrival produce one completed document, even "
                        + "when the lookup queued its wake before the root reached the assembler")
                .containsExactly(completed);
    }

    private static void feed(com.hazelcast.jet.core.Processor processor, int ordinal, Object... items) {
        TestInbox inbox = new TestInbox();
        inbox.queue().addAll(Arrays.asList(items));
        processor.process(ordinal, inbox);
    }

    private static List<Object> drain(TestOutbox outbox) {
        List<Object> sent = new ArrayList<>();
        outbox.drainQueueAndReset(0, sent, false);
        return sent;
    }

    private static List<Envelope> envelopes(List<Object> sent) {
        return new ArrayList<>(sent.stream().map(Envelope.class::cast).toList());
    }
}
